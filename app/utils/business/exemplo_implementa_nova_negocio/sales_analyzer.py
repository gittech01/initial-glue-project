# """
# Analisador de Vendas - Regra de Negócio Implementada.
#
# Implementa análise de vendas usando Template Method Pattern.
# """
# import logging
# from typing import Dict, Optional
# from pyspark.sql import DataFrame
# from pyspark.sql import functions as F
#
# from utils.business.base_processor import BaseBusinessProcessor
#
#
# logger = logging.getLogger(__name__)
#
#
# class SalesAnalyzer(BaseBusinessProcessor):
#     """
#     Analisador de vendas - Exemplo de nova regra de negócio.
#
#     Esta classe demonstra:
#     - Isolamento: Cada análise é independente
#     - Idempotência: Reexecuções não causam problemas
#     - Integração: Usa todos os handlers corretamente
#     - Rastreabilidade: Logs completos
#     """
#
#     def _read_data(self, **kwargs) -> DataFrame:
#         """Lê dados do catálogo e filtra por período."""
#         database = kwargs.get('database')
#         table_name = kwargs.get('table_name')
#         periodo = kwargs.get('periodo')
#
#         if not database or not table_name or not periodo:
#             raise ValueError("database, table_name e periodo são obrigatórios")
#
#         df = self.glue_handler.read_from_catalog(
#             database=database,
#             table_name=table_name
#         )
#
#         # Filtrar por período se coluna de data existir
#         if 'data' in df.columns or 'date' in df.columns:
#             date_col = 'data' if 'data' in df.columns else 'date'
#             df = df.filter(F.col(date_col).startswith(periodo))
#
#         return df
#
#     def _transform_data(self, df: DataFrame, **kwargs) -> Dict:
#         """Analisa dados de vendas."""
#         periodo = kwargs.get('periodo')
#         return self._analisar_dados(df, periodo)
#
#     def _get_congregado_key(self, **kwargs) -> str:
#         """Gera chave primária para análise de vendas."""
#         database = kwargs.get('database')
#         table_name = kwargs.get('table_name')
#         periodo = kwargs.get('periodo')
#         return f"vendas_{database}_{table_name}_{periodo}"
#
#     def _get_congregado_metadata(self, **kwargs) -> Dict:
#         """Gera metadados específicos para análise de vendas."""
#         metadata = super()._get_congregado_metadata(**kwargs)
#         metadata.update({
#             'database': kwargs.get('database'),
#             'table_name': kwargs.get('table_name'),
#             'periodo': kwargs.get('periodo'),
#             'tipo': 'analise_vendas'
#         })
#         return metadata
#
#     def analisar_vendas(
#         self,
#         database: str,
#         table_name: str,
#         periodo: str,
#         output_path: Optional[str] = None
#     ) -> Dict:
#         """
#         Analisa vendas de uma tabela para um período específico.
#
#         Usa o template method do BaseBusinessProcessor.
#         Pode ser chamada múltiplas vezes sem impacto entre execuções.
#
#         Args:
#             database: Nome do banco de dados
#             table_name: Nome da tabela de vendas
#             periodo: Período a ser analisado (ex: "2024-01")
#             output_path: Caminho de saída no S3 (opcional)
#
#         Returns:
#             Dicionário com resultado da análise
#         """
#         result = self.process(
#             database=database,
#             table_name=table_name,
#             periodo=periodo,
#             output_path=output_path
#         )
#
#         # Adicionar período ao resultado
#         result['periodo'] = periodo
#         return result
#
#     def _analisar_dados(self, df: DataFrame, periodo: str) -> Dict:
#         """
#         Analisa dados de vendas.
#
#         Esta é a função onde você implementa sua lógica específica de negócio.
#         Mantenha esta função pura (sem efeitos colaterais).
#
#         Args:
#             df: DataFrame Spark com dados de vendas
#             periodo: Período analisado
#
#         Returns:
#             Dicionário com resultados da análise
#         """
#         # Exemplo de análise
#         total_registros = df.count()
#
#         # Calcular totais se houver coluna de valor
#         analise = {
#             'periodo': periodo,
#             'total_registros': total_registros,
#             'timestamp': str(F.current_timestamp().cast("string"))
#         }
#
#         # Se houver coluna 'valor' ou 'amount', calcular totais
#         if 'valor' in df.columns:
#             total_vendas = df.agg(F.sum('valor').alias('total')).collect()[0]['total']
#             analise['total_vendas'] = float(total_vendas) if total_vendas else 0.0
#             analise['media_vendas'] = float(total_vendas / total_registros) if total_registros > 0 else 0.0
#         elif 'amount' in df.columns:
#             total_vendas = df.agg(F.sum('amount').alias('total')).collect()[0]['total']
#             analise['total_vendas'] = float(total_vendas) if total_vendas else 0.0
#             analise['media_vendas'] = float(total_vendas / total_registros) if total_registros > 0 else 0.0
#
#         # Agrupar por categoria se existir
#         if 'categoria' in df.columns:
#             por_categoria = df.groupBy('categoria').agg(
#                 F.count('*').alias('quantidade'),
#                 F.sum('valor' if 'valor' in df.columns else F.lit(0)).alias('total')
#             ).collect()
#             analise['por_categoria'] = [
#                 {'categoria': row['categoria'], 'quantidade': row['quantidade'], 'total': row['total']}
#                 for row in por_categoria
#             ]
#
#         return analise
#
#     def analisar_multiplos_periodos(
#         self,
#         database: str,
#         table_name: str,
#         periodos: list,
#         output_base_path: str
#     ) -> Dict:
#         """
#         Analisa múltiplos períodos de forma isolada.
#
#         Demonstra como processar vários períodos sem que um impacte o outro.
#         Cada análise é isolada e idempotente.
#
#         Args:
#             database: Nome do banco de dados
#             table_name: Nome da tabela
#             periodos: Lista de períodos a analisar
#             output_base_path: Caminho base para saída
#
#         Returns:
#             Dicionário com resultados de cada período
#         """
#         results = {}
#
#         for periodo in periodos:
#             try:
#                 output_path = f"{output_base_path}/{periodo}"
#
#                 # Cada chamada é isolada - não impacta outras
#                 result = self.analisar_vendas(
#                     database=database,
#                     table_name=table_name,
#                     periodo=periodo,
#                     output_path=output_path
#                 )
#
#                 results[periodo] = result
#                 logger.info(f"Período {periodo} analisado com sucesso")
#
#             except Exception as e:
#                 logger.error(f"Erro ao analisar período {periodo}: {e}")
#                 results[periodo] = {
#                     'status': 'error',
#                     'error': str(e)
#                 }
#
#         return results
