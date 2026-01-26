import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from sparksnake.manager import SparkETLManager
from sparksnake.tester.dataframes import (
    generate_dataframe_schema,
    generate_fake_data_from_schema
)

from datetime import datetime


class GlueHandler:
    
    partition_cols = None

    def __init__(self, glue_context: GlueContext, glue_client):
        self.glue_context = glue_context
        self.spark = self.glue_context.spark_session
        self.glue_client = glue_client
    
    def _get_cols_schema(self, database_name, table_name):
        response = self.glue_client.get_table(DatabaseName=database_name, Name=table_name)
        # Coleta colunas normais e colunas de partição
        cols_normals = response['Table']['StorageDescriptor']['Columns']
        __class__.partition_cols = response['Table'].get('PartitionKeys', [])
        all_cols = cols_normals + __class__.partition_cols
        
        mapping = []
        cols = []
        for col in all_cols:
            col_name = col['Name']
            col_type = col['Type']
            cols.append(col_name)
            mapping.append((col_name, f'cast:{col_type}'))

        return cols, mapping

    def write_and_catalog_data(self, dyf, database_name, table_name, s3_path):
        dynamic_frame = dyf
        if not isinstance(dynamic_frame, DynamicFrame):
            dynamic_frame = DynamicFrame.fromDF(dyf, self.glue_context, "dynamic_frame")
        
        _cols, mapping = self._get_cols_schema(database_name, table_name)
        
        # seleciona os campos de forma dinamica
        dynamic_frame = dynamic_frame.select_fields(paths=_cols)
        
        # convert os campos de forma dinamica
        dynamic_frame = dynamic_frame.resolveChoice(specs=mapping)

        # Salva e atualiza o catálogo
        sink = self.glue_context.getSink(
            connection_type="s3",
            path=s3_path,
            enableUpdateCatalog=True,
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=__class__.partition_cols
        )
        sink.setFormat("glueparquet", useGlueParquetWriter=True)
        sink.setCatalogInfo(catalogDatabase=database_name, catalogTableName=table_name)
        sink.writeFrame(dynamic_frame)
        
        print('Dados salvos e catalogo atulizado!')


ARGV_LIST = ["JOB_NAME", "S3_OUTPUT_PATH"]

# Defining dictionary of data sources to be used on job
DATA_DICT = {
    "orders": {
        "database": "db_name",
        "table_name": "tb_name",
        "transformation_ctx": "dyf_orders"
    }
}

# Creating a class object on initializing a glue job
spark_manager = SparkETLManager(
    mode="glue",
    argv_list=ARGV_LIST,
    data_dict=DATA_DICT
)

spark_manager.init_job()
spark = spark_manager.spark
glue_context = spark_manager.glueContext
glue_client = boto3.client('glue')

glue_handler = GlueHandler(glue_context, glue_client)

# Defining job arguments


# Showing an example of a input schema list
schema_info = [
    {
        "Name": "id",
        "Type": "int",
        "nullable": False
    },
    {
        "Name": "first_name",
        "Type": "string",
        "nullable": False
    },
    {
        "Name": "last_name",
        "Type": "string",
        "nullable": False
    },
    {
        "Name": "full_name",
        "Type": "string",
        "nullable": False
    }
]

# Returning a valid Spark schema object based on a dictionary
schema = generate_dataframe_schema(schema_info)

# Generating fake data based on a Spark DataFrame schema
fake_data = generate_fake_data_from_schema(schema=schema, n_rows=10)

df_fake = spark.createDataFrame(
    fake_data,
    schema=schema
)

print('Schema antes de aplicar o dinamismo de troca tipos')
df_fake.printSchema()

# Writing data on S3 and cataloging it on Data Catalog
# dyf, database_name, table_name, s3_path)
glue_handler.write_and_catalog_data(
    dyf=df_fake,
    s3_path=s3_path,
    database_name='db_name',
    table_name='tb_name'
)

# Job commit
spark_manager.job.commit()
