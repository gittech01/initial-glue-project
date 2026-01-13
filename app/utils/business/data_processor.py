"""
Processador de dados - Regra de negócio principal.

Esta classe demonstra como implementar uma regra de negócio que pode ser
chamada múltiplas vezes sem impactar outras execuções, garantindo isolamento
e idempotência através do JourneyController.
"""
import logging
from typing import Dict, Optional
from pyspark.sql import DataFrame

from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig


logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Processador de dados que implementa a regra de negócio principal.
    
    Esta classe demonstra como processar dados de forma idempotente,
    onde múltiplas chamadas não impactam outras execuções.
    
    Características:
    - Isolamento: Cada execução é independente
    - Idempotência: Execuções duplicadas não causam efeitos colaterais
    - Rastreabilidade: Todas as operações são rastreadas
    - Resiliência: Recupera de falhas automaticamente
    """
    
    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ):
        """
        Inicializa o processador de dados.
        
        Args:
            glue_handler: Handler para operações Glue
            journey_controller: Controller de jornada para idempotência
            dynamodb_handler: Handler para salvar congregado
            config: Configurações da aplicação
        """
        self.glue_handler = glue_handler
        self.journey_controller = journey_controller
        self.dynamodb_handler = dynamodb_handler
        self.config = config
        logger.info("DataProcessor inicializado")
    
    def process_data(
        self,
        database: str,
        table_name: str,
        output_path: Optional[str] = None
    ) -> Dict:
        """
        Processa dados de uma tabela do Glue Catalog.
        
        Esta função pode ser chamada múltiplas vezes com os mesmos parâmetros
        sem impactar outras execuções. Cada chamada é isolada e idempotente.
        
        Args:
            database: Nome do banco de dados no Glue Catalog
            table_name: Nome da tabela
            output_path: Caminho de saída no S3 (opcional)
        
        Returns:
            Dicionário com resultado do processamento
        
        Exemplo de uso múltiplo (sem impacto entre execuções):
            # Execução 1
            processor.process_data("db", "table1", "s3://bucket/output1")
            
            # Execução 2 (paralela, não impacta execução 1)
            processor.process_data("db", "table2", "s3://bucket/output2")
            
            # Execução 3 (mesmos parâmetros, idempotente)
            processor.process_data("db", "table1", "s3://bucket/output1")
        """
        logger.info(f"Iniciando processamento: database={database}, table={table_name}")
        
        try:
            # Etapa 1: Ler dados do catálogo
            logger.info("Etapa 1: Lendo dados do catálogo")
            df = self.glue_handler.read_from_catalog(
                database=database,
                table_name=table_name
            )
            
            record_count = df.count()
            logger.info(f"Dados lidos: {record_count} registros")
            
            # Etapa 2: Transformar dados (exemplo: agregar)
            logger.info("Etapa 2: Transformando dados")
            aggregated_data = self._aggregate_data(df)
            logger.info(f"Dados agregados: {aggregated_data}")
            
            # Etapa 3: Salvar congregado no DynamoDB (idempotente)
            logger.info("Etapa 3: Salvando congregado no DynamoDB")
            congregado_result = self.dynamodb_handler.save_congregado(
                congregado_data=aggregated_data,
                primary_key=f"{database}_{table_name}",
                metadata={
                    'database': database,
                    'table_name': table_name,
                    'record_count': record_count
                }
            )
            logger.info(f"Congregado salvo: {congregado_result}")
            
            # Etapa 4: Escrever dados processados (se output_path fornecido)
            if output_path:
                logger.info(f"Etapa 4: Escrevendo dados em {output_path}")
                self.glue_handler.write_to_s3(
                    df=df,
                    path=output_path,
                    format=self.config.default_output_format
                )
                logger.info("Dados escritos com sucesso")
            
            result = {
                'status': 'success',
                'record_count': record_count,
                'aggregated_data': aggregated_data,
                'congregado_id': congregado_result.get('id'),
                'output_path': output_path
            }
            
            logger.info(f"Processamento concluído: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Erro no processamento: {e}", exc_info=True)
            raise
    
    def _aggregate_data(self, df: DataFrame) -> Dict:
        """
        Agrega dados do DataFrame.
        
        Esta é uma função de exemplo que demonstra transformação de dados.
        Em produção, esta função conteria a lógica de negócio específica.
        
        Args:
            df: DataFrame Spark
        
        Returns:
            Dicionário com dados agregados
        """
        # Exemplo de agregação simples
        # Em produção, implementar lógica de negócio específica aqui
        total_rows = df.count()
        
        # Se o DataFrame tiver colunas numéricas, calcular totais
        numeric_columns = [field.name for field in df.schema.fields 
                          if str(field.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType']]
        
        aggregated = {
            'total_rows': total_rows,
            'numeric_columns': numeric_columns
        }
        
        # Calcular soma de colunas numéricas se existirem
        if numeric_columns:
            from pyspark.sql import functions as F
            sums = df.select([F.sum(col).alias(f"sum_{col}") for col in numeric_columns[:3]])  # Limitar a 3 colunas
            sums_dict = sums.collect()[0].asDict()
            aggregated.update(sums_dict)
        
        return aggregated
    
    def process_multiple_tables(
        self,
        tables: list,
        output_base_path: str
    ) -> Dict:
        """
        Processa múltiplas tabelas de forma isolada.
        
        Demonstra como processar várias tabelas sem que uma impacte a outra.
        Cada processamento é isolado e idempotente.
        
        Args:
            tables: Lista de tuplas (database, table_name)
            output_base_path: Caminho base para saída
        
        Returns:
            Dicionário com resultados de cada processamento
        """
        results = {}
        
        for database, table_name in tables:
            try:
                output_path = f"{output_base_path}/{database}/{table_name}"
                
                # Cada chamada é isolada - não impacta outras
                result = self.process_data(
                    database=database,
                    table_name=table_name,
                    output_path=output_path
                )
                
                results[f"{database}.{table_name}"] = result
                logger.info(f"Tabela {database}.{table_name} processada com sucesso")
                
            except Exception as e:
                logger.error(f"Erro ao processar {database}.{table_name}: {e}")
                results[f"{database}.{table_name}"] = {
                    'status': 'error',
                    'error': str(e)
                }
        
        return results
