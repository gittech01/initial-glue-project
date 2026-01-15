# """
# Processador de dados - Regra de negócio principal.
#
# Esta classe demonstra como implementar uma regra de negócio que pode ser
# chamada múltiplas vezes sem impactar outras execuções, garantindo isolamento
# e idempotência através do JourneyController.
#
# Implementa Template Method Pattern através de BaseBusinessProcessor.
# """
# import logging
# from typing import Dict, Optional
# from pyspark.sql import DataFrame
#
# from utils.business.base_processor import BaseBusinessProcessor
#
#
# logger = logging.getLogger(__name__)
#
#
# class DataProcessor(BaseBusinessProcessor):
#     """
#     Processador de dados que implementa a regra de negócio principal.
#
#     Esta classe demonstra como processar dados de forma idempotente,
#     onde múltiplas chamadas não impactam outras execuções.
#
#     Características:
#     - Isolamento: Cada execução é independente
#     - Idempotência: Execuções duplicadas não causam efeitos colaterais
#     - Rastreabilidade: Todas as operações são rastreadas
#     - Resiliência: Recupera de falhas automaticamente
#     """
#
#     def _read_data(self, **kwargs) -> DataFrame:
#         """Lê dados do catálogo Glue."""
#         database = kwargs.get('database')
#         table_name = kwargs.get('table_name')
#
#         if not database or not table_name:
#             raise ValueError("database e table_name são obrigatórios")
#
#         return self.glue_handler.read_from_catalog(
#             database=database,
#             table_name=table_name
#         )
#
#     def _transform_data(self, df: DataFrame, **kwargs) -> Dict:
#         """Transforma dados agregando informações."""
#         return self._aggregate_data(df)
#
#     def _get_congregado_key(self, **kwargs) -> str:
#         """Gera chave primária para congregado."""
#         database = kwargs.get('database')
#         table_name = kwargs.get('table_name')
#         return f"{database}_{table_name}"
#
#     def _get_congregado_metadata(self, **kwargs) -> Dict:
#         """Gera metadados para congregado."""
#         metadata = super()._get_congregado_metadata(**kwargs)
#         metadata.update({
#             'database': kwargs.get('database'),
#             'table_name': kwargs.get('table_name')
#         })
#         return metadata
#
#     def process_data(
#         self,
#         database: str,
#         table_name: str,
#         output_path: Optional[str] = None
#     ) -> Dict:
#         """
#         Processa dados de uma tabela do Glue Catalog.
#
#         Esta função usa o template method do BaseBusinessProcessor.
#         Pode ser chamada múltiplas vezes sem impactar outras execuções.
#
#         Args:
#             database: Nome do banco de dados no Glue Catalog
#             table_name: Nome da tabela
#             output_path: Caminho de saída no S3 (opcional)
#
#         Returns:
#             Dicionário com resultado do processamento
#         """
#         return self.process(
#             database=database,
#             table_name=table_name,
#             output_path=output_path
#         )
#
#     def _aggregate_data(self, df: DataFrame) -> Dict:
#         """
#         Agrega dados do DataFrame.
#
#         Esta é uma função de exemplo que demonstra transformação de dados.
#         Em produção, esta função conteria a lógica de negócio específica.
#
#         Args:
#             df: DataFrame Spark
#
#         Returns:
#             Dicionário com dados agregados
#         """
#         # Exemplo de agregação simples
#         # Em produção, implementar lógica de negócio específica aqui
#         total_rows = df.count()
#
#         # Se o DataFrame tiver colunas numéricas, calcular totais
#         numeric_columns = [field.name for field in df.schema.fields
#                           if str(field.dataType) in ['IntegerType', 'LongType', 'DoubleType', 'FloatType']]
#
#         aggregated = {
#             'total_rows': total_rows,
#             'numeric_columns': numeric_columns
#         }
#
#         # Calcular soma de colunas numéricas se existirem
#         if numeric_columns:
#             from pyspark.sql import functions as F
#             sums = df.select([F.sum(col).alias(f"sum_{col}") for col in numeric_columns[:3]])  # Limitar a 3 colunas
#             sums_dict = sums.collect()[0].asDict()
#             aggregated.update(sums_dict)
#
#         return aggregated
#
#     def process_multiple_tables(
#         self,
#         tables: list,
#         output_base_path: str
#     ) -> Dict:
#         """
#         Processa múltiplas tabelas de forma isolada.
#
#         Demonstra como processar várias tabelas sem que uma impacte a outra.
#         Cada processamento é isolado e idempotente.
#
#         Args:
#             tables: Lista de tuplas (database, table_name)
#             output_base_path: Caminho base para saída
#
#         Returns:
#             Dicionário com resultados de cada processamento
#         """
#         results = {}
#
#         for database, table_name in tables:
#             try:
#                 output_path = f"{output_base_path}/{database}/{table_name}"
#
#                 # Cada chamada é isolada - não impacta outras
#                 result = self.process_data(
#                     database=database,
#                     table_name=table_name,
#                     output_path=output_path
#                 )
#
#                 results[f"{database}.{table_name}"] = result
#                 logger.info(f"Tabela {database}.{table_name} processada com sucesso")
#
#             except Exception as e:
#                 logger.error(f"Erro ao processar {database}.{table_name}: {e}")
#                 results[f"{database}.{table_name}"] = {
#                     'status': 'error',
#                     'error': str(e)
#                 }
#
#         return results
