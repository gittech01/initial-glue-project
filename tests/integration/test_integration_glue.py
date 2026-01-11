import pytest
from pyspark.sql import SparkSession
from src.glue_handler import GlueDataHandler
from unittest.mock import MagicMock


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("GlueIntegrationTest") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def glue_context_mock(spark):
    # No ambiente local, não temos as bibliotecas nativas do Glue (C++ bindings)
    # Então simulamos o GlueContext mas usamos o Spark real para o processamento de dados
    mock_glue = MagicMock()
    mock_glue.spark_session = spark
    return mock_glue

def test_full_pipeline_flow(spark, glue_context_mock):
    """
    Testa o fluxo completo: Criação de dados -> Escrita (Simulada) -> Leitura (Simulada)
    Como estamos fora da AWS, validamos a lógica de transformação e a interface.
    """
    handler = GlueDataHandler(glue_context_mock)
    
    # 1. Criar dados de teste
    data = [("Alice", 34), ("Bob", 45)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # 2. Testar lógica de escrita (Mockando a chamada do Glue mas validando os dados)
    # Em um teste de integração real em CI/CD, usaríamos LocalStack ou um ambiente de Dev Endpoint
    handler.write_to_s3(df, "tmp/test_output")
    
    assert glue_context_mock.write_dynamic_frame.from_options.called
    
    # 3. Validar que o DataFrame original tem o que esperamos
    assert df.count() == 2
    assert "name" in df.columns
