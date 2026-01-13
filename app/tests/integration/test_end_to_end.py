"""
Teste de integração end-to-end da aplicação.

Valida o fluxo completo desde o entry point até a execução da regra de negócio.
"""
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
import sys


@pytest.fixture(scope="module")
def spark():
    """Fixture para SparkSession."""
    return SparkSession.builder \
        .appName("EndToEndTest") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def mock_glue_context(spark):
    """Fixture para mock do GlueContext."""
    mock_glue = MagicMock()
    mock_glue.spark_session = spark
    return mock_glue


def test_end_to_end_flow(spark, mock_glue_context):
    """
    Testa o fluxo completo da aplicação de ponta a ponta.
    
    Valida:
    1. Inicialização do contexto
    2. Carregamento de configurações
    3. Criação de handlers
    4. Execução da regra de negócio
    5. Controle de jornada
    6. Persistência no DynamoDB
    """
    # Mock dos componentes AWS Glue
    with patch('src.main.SparkContext') as mock_spark_context, \
         patch('src.main.GlueContext') as mock_glue_class, \
         patch('src.main.Job') as mock_job_class, \
         patch('src.main.getResolvedOptions') as mock_get_options:
        
        # Configurar mocks
        mock_spark_context.return_value = MagicMock()
        mock_glue_class.return_value = mock_glue_context
        mock_job = MagicMock()
        mock_job_class.return_value = mock_job
        
        mock_get_options.return_value = {
            'JOB_NAME': 'test_job',
            'database': 'test_db',
            'table_name': 'test_table',
            'output_path': 's3://bucket/output'
        }
        
        # Mock do DataFrame
        data = [("Alice", 34, 1000), ("Bob", 45, 2000)]
        df = spark.createDataFrame(data, ["name", "age", "value"])
        
        # Mock do GlueDataHandler
        with patch('src.main.GlueDataHandler') as mock_handler_class:
            mock_handler = MagicMock()
            mock_handler.read_from_catalog.return_value = df
            mock_handler_class.return_value = mock_handler
            
            # Mock do JourneyController
            with patch('src.main.JourneyController') as mock_journey_class:
                mock_journey = MagicMock()
                mock_journey.execute_with_journey.return_value = {
                    'status': 'success',
                    'record_count': 2,
                    'congregado_id': 'test_db_test_table'
                }
                mock_journey_class.return_value = mock_journey
                
                # Mock do DynamoDBHandler
                with patch('src.main.DynamoDBHandler') as mock_dynamodb_class:
                    mock_dynamodb = MagicMock()
                    mock_dynamodb.save_congregado.return_value = {
                        'id': 'test_db_test_table',
                        'status': 'created'
                    }
                    mock_dynamodb_class.return_value = mock_dynamodb
                    
                    # Executar main
                    from src.main import main
                    
                    result = main()
                    
                    # Validações
                    assert result is not None
                    assert result['status'] == 'success'
                    assert result['record_count'] == 2
                    
                    # Verificar que todos os componentes foram chamados
                    mock_spark_context.assert_called_once()
                    mock_glue_class.assert_called_once()
                    mock_job_class.assert_called_once()
                    mock_get_options.assert_called_once()
                    mock_handler_class.assert_called_once()
                    mock_journey_class.assert_called_once()
                    mock_dynamodb_class.assert_called_once()
                    mock_journey.execute_with_journey.assert_called_once()
                    mock_job.commit.assert_called_once()


def test_data_processor_isolation(spark, mock_glue_context):
    """
    Testa que múltiplas execuções do DataProcessor são isoladas.
    """
    from utils.business.data_processor import DataProcessor
    from utils.handlers.glue_handler import GlueDataHandler
    from utils.journey_controller import JourneyController
    from utils.dynamodb_handler import DynamoDBHandler
    from utils.config.settings import AppConfig
    
    # Criar componentes reais (modo em memória)
    glue_handler = GlueDataHandler(mock_glue_context)
    journey_controller = JourneyController(
        table_name="test_journey",
        dynamodb_client=None  # Modo em memória
    )
    dynamodb_handler = DynamoDBHandler(
        table_name="test_congregado",
        dynamodb_client=None  # Modo em memória
    )
    config = AppConfig()
    
    processor = DataProcessor(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )
    
    # Mock do read_from_catalog para retornar dados diferentes
    data1 = [("Table1", 100)]
    df1 = spark.createDataFrame(data1, ["name", "value"])
    
    data2 = [("Table2", 200)]
    df2 = spark.createDataFrame(data2, ["name", "value"])
    
    # Simular múltiplas chamadas isoladas
    with patch.object(glue_handler, 'read_from_catalog') as mock_read:
        mock_read.side_effect = [df1, df2]
        
        # Primeira execução
        result1 = processor.process_data("db1", "table1", "s3://out1")
        
        # Segunda execução (deve ser isolada)
        result2 = processor.process_data("db2", "table2", "s3://out2")
        
        # Verificar que são independentes
        assert result1['status'] == 'success'
        assert result2['status'] == 'success'
        assert result1['record_count'] == 1
        assert result2['record_count'] == 1
        assert result1 != result2  # Resultados diferentes
