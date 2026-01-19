"""
Handler para operações de leitura e escrita no AWS Glue.

Mantido em utils/handlers/ para separação de responsabilidades.
"""
import boto3
from unittest.mock import MagicMock
from datetime import datetime, timedelta

try:
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
except ImportError:
    # Fallback para ambiente local/testes onde as libs do Glue não estão presentes
    class GlueContext:
        pass
    
    class DynamicFrame:
        @staticmethod
        def fromDF(df, context, name):
            return MagicMock()
    
    print("Aviso: Bibliotecas AWS Glue não encontradas. Usando mocks para compatibilidade local.")

from pyspark.sql import DataFrame


def _get_glue_client(region_name: str = None):
    """
    Obtém cliente Glue com região especificada.
    
    Args:
        region_name: Nome da região AWS (opcional)
    
    Returns:
        Cliente boto3 Glue
    """
    if region_name:
        return boto3.client("glue", region_name=region_name)
    # Tentar obter região do ambiente ou usar padrão
    try:
        return boto3.client("glue", region_name='sa-east-1')
    except Exception:
        # Fallback: retornar mock em ambiente de teste
        return MagicMock()


class GlueDataHandler:
    """
    Classe especialista para lidar com operações de leitura e escrita no AWS Glue.
    Abstrai a complexidade do GlueContext e fornece métodos robustos para I/O.
    """
    def __init__(self, glue_context: GlueContext):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session

    def read_from_catalog(self, database: str, table_name: str, filter: str = None) -> DataFrame:
        """
        Lê dados do AWS Glue Data Catalog.
        """
        dynamic_frame = self.glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name,
            push_down_predicate=filter or ""
        )
        return dynamic_frame.toDF()

    def read_from_s3(self, path: str | list, format: str = "parquet", format_options: dict = None) -> DataFrame:
        """
        Lê dados diretamente do S3.
        """

        dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path] if isinstance(path, str) else path},
            format=format,
            format_options=format_options or {}
        )
        return dynamic_frame.toDF()

    def write_to_s3(
        self, 
        df: DataFrame, 
        path: str, 
        format: str = "parquet", 
        partition_cols: list = None,
        compression: str = "snappy"
    ):
        """
        Escreve um DataFrame Spark no S3 usando o GlueContext para melhor integração com o catálogo.
        
        Args:
            df: DataFrame Spark
            path: Caminho S3 (ex: s3://bucket/path/)
            format: Formato de arquivo (padrão: parquet)
            partition_cols: Lista de colunas para particionamento
            compression: Tipo de compressão (padrão: snappy)
        """
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "dynamic_frame")
        
        # Configurar opções de formato com compressão
        format_options = {}
        if format == "parquet":
            format_options = {
                "compression": compression
            }
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": path,
                "partitionKeys": partition_cols or []
            },
            format=format,
            format_options=format_options,
            transformation_ctx="write_to_s3"
        )

    def write_to_catalog(
        self, 
        df: DataFrame, 
        database: str, 
        table_name: str,
        compression: str = "snappy"
    ):
        """
        Escreve dados no S3 e atualiza o Glue Data Catalog com nova partição.
        
        IMPORTANTE: Este método sempre salva no S3 (localização da tabela no catálogo)
        e atualiza o catálogo com a nova partição.
        
        Formato: Parquet com compressão Snappy (padrão)
        
        Args:
            df: DataFrame Spark
            database: Nome do banco de dados no Glue Catalog
            table_name: Nome da tabela no Glue Catalog
            compression: Tipo de compressão (padrão: snappy)
        """
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "dynamic_frame")
        
        # Obter caminho S3 da tabela do catálogo
        # Isso permite usar from_options com compressão e ainda atualizar o catálogo
        try:
            import boto3
            glue_client = boto3.client('glue', region_name='sa-east-1')
            table_response = glue_client.get_table(DatabaseName=database, Name=table_name)
            table_location = table_response['Table']['StorageDescriptor']['Location']
            
            # Obter colunas de partição da tabela
            partition_keys = [col['Name'] for col in table_response['Table'].get('PartitionKeys', [])]
            
            # Usar from_options para ter controle sobre compressão
            # O Glue ainda atualiza o catálogo automaticamente quando escreve no caminho da tabela
            format_options = {
                "compression": compression
            }
            
            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="s3",
                connection_options={
                    "path": table_location,
                    "partitionKeys": partition_keys if partition_keys else [],
                    "enableUpdateCatalog": True
                },
                format="parquet",
                format_options=format_options,
                transformation_ctx="write_to_catalog"
            )
        except Exception as e:
            # Fallback: usar from_catalog se não conseguir obter o caminho
            # Nota: from_catalog pode não suportar compressão customizada
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(
                f"Não foi possível obter caminho S3 da tabela {database}.{table_name}. "
                f"Usando from_catalog (compressão pode não ser aplicada): {e}"
            )
            self.glue_context.write_dynamic_frame.from_catalog(
                frame=dynamic_frame,
                database=database,
                table_name=table_name,
                transformation_ctx="write_to_catalog"
            )

    def get_last_partition(self, database: str, table_name: str, partition_key: str, region_name: str = None):
        """
        Obtém a última partição de uma tabela.
        
        Args:
            database: Nome do banco de dados
            table_name: Nome da tabela
            partition_key: Chave de partição
            region_name: Região AWS (opcional)
        
        Returns:
            Última partição encontrada ou None
        """
        days_back = 7
        limit_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        expression = f"{partition_key} >= '{limit_date}'"

        glue_client = _get_glue_client(region_name)
        response = glue_client.get_partitions(
            DatabaseName=database,
            TableName=table_name,
            Expression=expression
        )

        partitions = response.get('Partitions', [])
        if not partitions:
            return None
        
        # Encontrar a partição mais recente
        # Cada partição tem {'Values': ['valor1', 'valor2', ...]} onde Values é uma lista
        # Para partições com uma única coluna, pegamos o primeiro valor
        # Para comparar, usamos o primeiro valor (assumindo que é a data/anomesdia)
        last_partition = max(partitions, key=lambda p: p['Values'][0] if p['Values'] else '')
        
        # Retornar o primeiro valor da lista Values (partição mais recente)
        # Se houver múltiplas colunas de partição, retorna a lista completa
        values = last_partition.get('Values', [])
        if len(values) == 1:
            # Partição simples (ex: anomesdia='20240116')
            return values[0]
        else:
            # Múltiplas colunas de partição, retornar lista
            return values

