"""
Handler para operações de leitura e escrita no AWS Glue.

Mantido em utils/handlers/ para separação de responsabilidades.
"""
import sys
import boto3
from unittest.mock import MagicMock
from datetime import datetime, timedelta
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
except ImportError:
    # Fallback para ambiente local/testes onde as libs do Glue não estão presentes
    class GlueContext: pass
    class DynamicFrame:
        @staticmethod
        def fromDF(df, context, name): return MagicMock()
    print("Aviso: Bibliotecas AWS Glue não encontradas. Usando mocks para compatibilidade local.")

from pyspark.context import SparkContext
from pyspark.sql import DataFrame


glue_client = boto3.client("glue")


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

    def write_to_s3(self, df: DataFrame, path: str, format: str = "parquet", partition_cols: list = None):
        """
        Escreve um DataFrame Spark no S3 usando o GlueContext para melhor integração com o catálogo.
        """
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "dynamic_frame")
        
        self.glue_context.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": path,
                "partitionKeys": partition_cols or []
            },
            format=format,
            transformation_ctx="write_to_s3"
        )

    def write_to_catalog(self, df: DataFrame, database: str, table_name: str):
        """
        Escreve dados e atualiza o Glue Data Catalog.
        """
        dynamic_frame = DynamicFrame.fromDF(df, self.glue_context, "dynamic_frame")
        
        self.glue_context.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database,
            table_name=table_name,
            transformation_ctx="write_to_catalog"
        )

    def get_last_partition(self, database: str, table_name: str, partition_key: str):
        days_back = 7
        limit_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        expression = f"{partition_key} >= '{limit_date}'"

        response = glue_client.get_partitions(
            DatabaseName=database,
            TableName=table_name,
            Expression=expression
        )

        partitions = [p['Values'] for p in response.get('Partitions', [])]
        return max(partitions) if partitions else None

