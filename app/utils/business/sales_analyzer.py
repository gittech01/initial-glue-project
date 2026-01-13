"""
Analisador de Vendas - Exemplo de Nova Regra de Negócio.

Este é um exemplo completo de como implementar uma nova regra de negócio
seguindo todos os padrões e boas práticas da aplicação.
"""
import logging
from typing import Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig


logger = logging.getLogger(__name__)


class SalesAnalyzer:
    """
    Analisador de vendas - Exemplo de nova regra de negócio.
    
    Esta classe demonstra:
    - Isolamento: Cada análise é independente
    - Idempotência: Reexecuções não causam problemas
    - Integração: Usa todos os handlers corretamente
    - Rastreabilidade: Logs completos
    """
    
    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ):
        """
        Inicializa o analisador de vendas.
        
        IMPORTANTE: Todas as dependências são injetadas.
        Não cria instâncias internamente.
        """
        self.glue_handler = glue_handler
        self.journey_controller = journey_controller
        self.dynamodb_handler = dynamodb_handler
        self.config = config
        logger.info("SalesAnalyzer inicializado")
    
    def analisar_vendas(
        self,
        database: str,
        table_name: str,
        periodo: str,
        output_path: Optional[str] = None
    ) -> Dict:
        """
        Analisa vendas de uma tabela para um período específico.
        
        Esta função pode ser chamada múltiplas vezes sem impacto entre execuções.
        Cada análise é isolada e idempotente.
        
        Args:
            database: Nome do banco de dados
            table_name: Nome da tabela de vendas
            periodo: Período a ser analisado (ex: "2024-01")
            output_path: Caminho de saída no S3 (opcional)
        
        Returns:
            Dicionário com resultado da análise
        
        Exemplo de uso:
            # Análise 1 - Não impacta outras
            result1 = analyzer.analisar_vendas("db", "vendas", "2024-01", "s3://out1")
            
            # Análise 2 - Paralela, isolada
            result2 = analyzer.analisar_vendas("db", "vendas", "2024-02", "s3://out2")
            
            # Análise 3 - Idempotente (mesmos parâmetros)
            result3 = analyzer.analisar_vendas("db", "vendas", "2024-01", "s3://out1")
            # result3 == result1 (sem reprocessar)
        """
        logger.info(f"Iniciando análise de vendas: {database}.{table_name} para período {periodo}")
        
        try:
            # ETAPA 1: Ler dados do catálogo
            logger.info("Etapa 1: Lendo dados do catálogo")
            df = self.glue_handler.read_from_catalog(
                database=database,
                table_name=table_name
            )
            
            record_count = df.count()
            logger.info(f"Dados lidos: {record_count} registros")
            
            # ETAPA 2: Filtrar por período (se necessário)
            if 'data' in df.columns or 'date' in df.columns:
                date_col = 'data' if 'data' in df.columns else 'date'
                df = df.filter(F.col(date_col).startswith(periodo))
                filtered_count = df.count()
                logger.info(f"Registros após filtro de período: {filtered_count}")
            
            # ETAPA 3: Analisar dados (SUA LÓGICA DE NEGÓCIO)
            logger.info("Etapa 2: Analisando dados de vendas")
            analise = self._analisar_dados(df, periodo)
            logger.info(f"Análise concluída: {analise}")
            
            # ETAPA 4: Salvar congregado no DynamoDB (idempotente)
            logger.info("Etapa 3: Salvando análise no DynamoDB")
            congregado_result = self.dynamodb_handler.save_congregado(
                congregado_data=analise,
                primary_key=f"vendas_{database}_{table_name}_{periodo}",
                metadata={
                    'database': database,
                    'table_name': table_name,
                    'periodo': periodo,
                    'record_count': record_count,
                    'tipo': 'analise_vendas'
                }
            )
            logger.info(f"Análise salva: {congregado_result}")
            
            # ETAPA 5: Escrever resultado (se output_path fornecido)
            if output_path:
                logger.info(f"Etapa 4: Escrevendo análise em {output_path}")
                self.glue_handler.write_to_s3(
                    df=df,
                    path=output_path,
                    format=self.config.default_output_format
                )
                logger.info("Análise escrita com sucesso")
            
            # Retornar resultado
            result = {
                'status': 'success',
                'periodo': periodo,
                'record_count': record_count,
                'analise': analise,
                'congregado_id': congregado_result.get('id'),
                'output_path': output_path
            }
            
            logger.info(f"Análise de vendas concluída: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Erro na análise de vendas: {e}", exc_info=True)
            raise  # Re-raise para retry automático
    
    def _analisar_dados(self, df: DataFrame, periodo: str) -> Dict:
        """
        Analisa dados de vendas.
        
        Esta é a função onde você implementa sua lógica específica de negócio.
        Mantenha esta função pura (sem efeitos colaterais).
        
        Args:
            df: DataFrame Spark com dados de vendas
            periodo: Período analisado
        
        Returns:
            Dicionário com resultados da análise
        """
        # Exemplo de análise
        total_registros = df.count()
        
        # Calcular totais se houver coluna de valor
        analise = {
            'periodo': periodo,
            'total_registros': total_registros,
            'timestamp': str(F.current_timestamp().cast("string"))
        }
        
        # Se houver coluna 'valor' ou 'amount', calcular totais
        if 'valor' in df.columns:
            total_vendas = df.agg(F.sum('valor').alias('total')).collect()[0]['total']
            analise['total_vendas'] = float(total_vendas) if total_vendas else 0.0
            analise['media_vendas'] = float(total_vendas / total_registros) if total_registros > 0 else 0.0
        elif 'amount' in df.columns:
            total_vendas = df.agg(F.sum('amount').alias('total')).collect()[0]['total']
            analise['total_vendas'] = float(total_vendas) if total_vendas else 0.0
            analise['media_vendas'] = float(total_vendas / total_registros) if total_registros > 0 else 0.0
        
        # Agrupar por categoria se existir
        if 'categoria' in df.columns:
            por_categoria = df.groupBy('categoria').agg(
                F.count('*').alias('quantidade'),
                F.sum('valor' if 'valor' in df.columns else F.lit(0)).alias('total')
            ).collect()
            analise['por_categoria'] = [
                {'categoria': row['categoria'], 'quantidade': row['quantidade'], 'total': row['total']}
                for row in por_categoria
            ]
        
        return analise
    
    def analisar_multiplos_periodos(
        self,
        database: str,
        table_name: str,
        periodos: list,
        output_base_path: str
    ) -> Dict:
        """
        Analisa múltiplos períodos de forma isolada.
        
        Demonstra como processar vários períodos sem que um impacte o outro.
        Cada análise é isolada e idempotente.
        
        Args:
            database: Nome do banco de dados
            table_name: Nome da tabela
            periodos: Lista de períodos a analisar
            output_base_path: Caminho base para saída
        
        Returns:
            Dicionário com resultados de cada período
        """
        results = {}
        
        for periodo in periodos:
            try:
                output_path = f"{output_base_path}/{periodo}"
                
                # Cada chamada é isolada - não impacta outras
                result = self.analisar_vendas(
                    database=database,
                    table_name=table_name,
                    periodo=periodo,
                    output_path=output_path
                )
                
                results[periodo] = result
                logger.info(f"Período {periodo} analisado com sucesso")
                
            except Exception as e:
                logger.error(f"Erro ao analisar período {periodo}: {e}")
                results[periodo] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results
