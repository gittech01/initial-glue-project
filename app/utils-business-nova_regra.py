"""
Consolidação de Posição de Operações - Nova Regra de Negócio. 

Regra de comparação ENTRE origens (A vs B):
    1) data_valor_evento_operacao (desc)
    2) prioridade_evento_operacao (desc)
    3) data_recebimento_evento_operacao (desc)
    4) desempate determinístico por origem preferida (ONLINE por padrão)

Características:
- Isolamento:  Cada execução é independente
- Idempotência: Execuções duplicadas não causam efeitos colaterais
"""
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass, field
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.handlers. glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config. settings import AppConfig

logger = logging.getLogger(__name__)


@dataclass
class ConfigConsolidacaoPosicao:
    """Configuração para consolidação de posição de operações."""
    
    coluna_numero_operacao: str = "num_oper"
    coluna_codigo_versao_operacao: str = "cod_idef_vers_oper"
    coluna_codigo_evento_processado: str = "cod_idef_even_prcs"
    
    coluna_data_valor_evento:  str = "dat_vlr_even_oper"
    coluna_prioridade_evento: str = "num_prio_even_oper"
    coluna_data_recebimento_evento: str = "dat_recm_even_oper"
    
    coluna_origem:  str = "origem_registro"
    origem_preferida_desempate: str = "ONLINE"
    
    nulos_como_minimo: bool = True
    timestamp_minimo: str = "1900-01-01 00:00:00"
    prioridade_minima: int = 1
    
    colunas_ordem_ultima_ocorrencia: Optional[List[str]] = None
    reduzir_cada_lado_para_ultima_ocorrencia: bool = True


class NovaRegraConsolidacao:
    """
    Implementa regra de negócio para consolidação de posição de operações.
    
    Integra a lógica de comparação entre múltiplas origens de dados,
    selecionando a versão mais recente e prioritária de cada operação.
    """
    
    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller:  JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ):
        """
        Inicializa a nova regra de consolidação.
        
        Args:
            glue_handler: Handler para operações Glue/S3
            journey_controller: Controller para controle de jornada
            dynamodb_handler: Handler para DynamoDB
            config: Configurações da aplicação
        """
        self.glue_handler = glue_handler
        self.journey_controller = journey_controller
        self.dynamodb_handler = dynamodb_handler
        self.config = config
        self.cfg = ConfigConsolidacaoPosicao()
        
        logger.info("NovaRegraConsolidacao inicializada com sucesso")
    
    def processar_consolidacao(
        self,
        database:  str,
        table_name: str,
        output_path: Optional[str] = None,
        parametros_extras: Optional[Dict] = None
    ) -> Dict:
        """
        Processa consolidação de posição de operações. 
        
        Args:
            database:  Banco de dados Glue
            table_name:  Tabela de entrada
            output_path: Caminho S3 para saída (opcional)
            parametros_extras: Parâmetros adicionais (opcional)
        
        Returns:
            Dicionário com resultado do processamento
        """
        try: 
            logger.info(f"Iniciando consolidação:  {database}. {table_name}")
            
            # Leitura dos dados
            df_entrada = self._ler_dados(database, table_name)
            logger.info(f"Dados lidos: {df_entrada.count()} registros")
            
            # Normalização de tipos e nulos
            df_normalizado = self._normalizar_tipos_e_nulos(df_entrada)
            
            # Aplicar regra de consolidação
            df_consolidado = self._aplicar_regra_consolidacao(df_normalizado)
            logger.info(f"Consolidação aplicada: {df_consolidado.count()} registros")
            
            # Selecionar colunas finais
            colunas_retorno = self._obter_colunas_retorno(df_consolidado)
            df_final = self._selecionar_colunas_retorno(df_consolidado, colunas_retorno)
            
            # Salvar resultado
            resultado = self._salvar_resultado(df_final, output_path)
            
            logger.info(f"Consolidação finalizada com sucesso")
            return {
                'status': 'success',
                'registry_count': df_consolidado.count(),
                'output_path': resultado. get('output_path'),
                'duration_seconds': resultado.get('duration_seconds', 0)
            }
            
        except Exception as e: 
            logger.error(f"Erro na consolidação: {str(e)}")
            raise
    
    def _ler_dados(self, database: str, table_name: str) -> DataFrame:
        """Lê dados do catálogo Glue."""
        try:
            df = self.glue_handler. read_from_catalog(
                database=database,
                table_name=table_name
            )
            logger.debug(f"Dados lidos de {database}.{table_name}")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler dados: {str(e)}")
            raise
    
    def _normalizar_tipos_e_nulos(self, df: DataFrame) -> DataFrame:
        """
        Normaliza tipos de dados e trata valores nulos.
        
        Args:
            df: DataFrame de entrada
        
        Returns: 
            DataFrame com tipos normalizados
        """
        try:
            c = self.cfg
            
            # Converter para timestamp
            df_norm = (
                df.withColumn(
                    c.coluna_data_valor_evento,
                    F.to_timestamp(F.col(c.coluna_data_valor_evento))
                )
                .withColumn(
                    c. coluna_data_recebimento_evento,
                    F. to_timestamp(F.col(c.coluna_data_recebimento_evento))
                )
                .withColumn(
                    c.coluna_prioridade_evento,
                    F.col(c.coluna_prioridade_evento).cast("long")
                )
            )
            
            # Tratar nulos como mínimo
            if c.nulos_como_minimo: 
                ts_min = F.to_timestamp(F.lit(c.timestamp_minimo))
                pr_min = F.lit(c.prioridade_minima).cast("long")
                
                df_norm = (
                    df_norm.withColumn(
                        c.coluna_data_valor_evento,
                        F.coalesce(F.col(c.coluna_data_valor_evento), ts_min)
                    )
                    .withColumn(
                        c.coluna_data_recebimento_evento,
                        F.coalesce(F. col(c.coluna_data_recebimento_evento), ts_min)
                    )
                    .withColumn(
                        c.coluna_prioridade_evento,
                        F.coalesce(F.col(c.coluna_prioridade_evento), pr_min)
                    )
                )
            
            logger.debug("Normalização de tipos concluída")
            return df_norm
            
        except Exception as e:
            logger. error(f"Erro na normalização: {str(e)}")
            raise
    
    def _aplicar_regra_consolidacao(self, df: DataFrame) -> DataFrame:
        """
        Aplica regra de consolidação com ordenação por prioridade.
        
        Ordem de comparação:
        1) data_valor_evento (desc)
        2) prioridade_evento (desc)
        3) data_recebimento_evento (desc)
        4) origem preferida (ONLINE por padrão)
        
        Args:
            df: DataFrame com dados normalizados
        
        Returns: 
            DataFrame consolidado
        """
        try: 
            c = self.cfg
            
            # Window function para ordenar por prioridade
            from pyspark.sql.window import Window
            
            window_spec = Window.partitionBy(c.coluna_numero_operacao).orderBy(
                F.col(c.coluna_data_valor_evento).desc(),
                F.col(c.coluna_prioridade_evento).desc(),
                F.col(c. coluna_data_recebimento_evento).desc(),
                F.when(F.col(c.coluna_origem) == c.origem_preferida_desempate, 0).otherwise(1)
            )
            
            # Aplicar row_number para selecionar primeira ocorrência
            df_com_rank = df.withColumn("rank", F.row_number().over(window_spec))
            
            # Manter apenas o melhor registro por operação
            df_consolidado = df_com_rank. filter(F.col("rank") == 1).drop("rank")
            
            logger.debug("Regra de consolidação aplicada")
            return df_consolidado
            
        except Exception as e: 
            logger.error(f"Erro ao aplicar regra:  {str(e)}")
            raise
    
    def _obter_colunas_retorno(self, df: DataFrame) -> List[str]:
        """Obtém lista de colunas para retorno."""
        colunas = df.columns
        logger.debug(f"Colunas de retorno: {colunas}")
        return colunas
    
    def _selecionar_colunas_retorno(
        self,
        df: DataFrame,
        colunas_retorno: List[str]
    ) -> DataFrame:
        """Seleciona colunas finais garantindo presença da coluna origem."""
        c = self.cfg
        
        # Garantir que coluna origem está presente
        if c.coluna_origem not in colunas_retorno:
            colunas_retorno = colunas_retorno + [c.coluna_origem]
        
        df_selecionado = df. select(*colunas_retorno)
        logger.debug(f"Colunas selecionadas: {colunas_retorno}")
        return df_selecionado
    
    def _salvar_resultado(
        self,
        df: DataFrame,
        output_path: Optional[str] = None
    ) -> Dict: 
        """
        Salva resultado do processamento. 
        
        Args:
            df: DataFrame com resultado
            output_path: Caminho S3 para salvar (opcional)
        
        Returns:
            Dicionário com informações da saída
        """
        try:
            if output_path is None:
                output_path = self.config.output_path or "s3://default-output/consolidacao"
            
            self.glue_handler.write_to_s3(
                df=df,
                path=output_path,
                format=self.config.default_output_format or "parquet"
            )
            
            logger.info(f"Resultado salvo em: {output_path}")
            return {
                'output_path': output_path,
                'duration_seconds': 0
            }
            
        except Exception as e:
            logger.error(f"Erro ao salvar resultado:  {str(e)}")
            raise