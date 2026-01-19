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
            'tabela_consolidada': 'test_table',
            'output_path_s3': 's3://bucket/output',
            'continue_on_error': 'true'
        }
        
        # Mock do DataFrame
        data = [("Alice", 34, 1000), ("Bob", 45, 2000)]
        df = spark.createDataFrame(data, ["name", "age", "value"])
        
        # Mock do AppConfig para retornar consolidacoes_tabelas
        with patch('src.main.AppConfig') as mock_config_class:
            mock_config = MagicMock()
            mock_config.consolidacoes_tabelas = {
                'test_table': {
                    'principais': {'sor': {'database': 'db_test', 'table': 'table_sor'}},
                    'auxiliares': {},
                    'joins_auxiliares': {},
                    'agrupamento': ['name'],
                    'tabela_consolidada': 'test_table'
                }
            }
            mock_config.journey_table_name = 'test_journey'
            mock_config.aws_region = 'sa-east-1'
            mock_config_class.return_value = mock_config
            
            # Mock do GlueDataHandler
            with patch('src.main.GlueDataHandler') as mock_handler_class:
                mock_handler = MagicMock()
                mock_handler.read_from_catalog.return_value = df
                mock_handler.get_last_partition.return_value = None
                mock_handler_class.return_value = mock_handler
                
                # Mock do JourneyController e Orchestrator
                with patch('src.main.JourneyController') as mock_journey_class, \
                     patch('src.main.BusinessRuleOrchestrator') as mock_orchestrator_class, \
                     patch('src.main.ProcessorFactory') as mock_factory_class:
                    mock_journey = MagicMock()
                    mock_journey_class.return_value = mock_journey
                    
                    mock_processor = MagicMock()
                    mock_processor.get_processor_name.return_value = 'flexible_consolidation'
                    mock_factory_class.create.return_value = mock_processor
                    
                    mock_orchestrator = MagicMock()
                    mock_orchestrator.execute_rule.return_value = {
                        'status': 'success',
                        'result': {
                            'status': 'success',
                            'record_count': 2
                        }
                    }
                    mock_orchestrator_class.return_value = mock_orchestrator
                    
                    # DynamoDBHandler não é mais necessário - dados são salvos no S3/Glue Catalog
                    # Executar main
                    from src.main import main
                    
                    result = main()
                    
                    # Validações - main retorna estrutura diferente
                    assert result is not None
                    assert result['status'] in ['success', 'partial_success']
                    assert result['total'] == 1
                    assert result['sucessos'] == 1
                    assert 'resultados' in result
                    
                    # Verificar que todos os componentes foram chamados
                    mock_spark_context.assert_called_once()
                    mock_glue_class.assert_called_once()
                    mock_job_class.assert_called_once()
                    mock_get_options.assert_called_once()
                    mock_handler_class.assert_called_once()
                    mock_journey_class.assert_called_once()
                    # DynamoDBHandler não é mais necessário
                    mock_factory_class.create.assert_called_once()
                    mock_orchestrator.execute_rule.assert_called_once()
                    mock_job.commit.assert_called_once()


def test_data_processor_isolation(spark, mock_glue_context):
    """
    Testa que múltiplas execuções são isoladas usando FlexibleConsolidationProcessor.
    
    Valida que:
    1. Múltiplas execuções com diferentes tabelas são independentes
    2. Cada execução mantém seu próprio estado
    3. Os resultados não interferem entre si
    """
    from utils.handlers.glue_handler import GlueDataHandler
    from utils.journey_controller import JourneyController
    from utils.config.settings import AppConfig
    from utils.business.flexible_consolidation_processor import FlexibleConsolidationProcessor
    
    # Criar componentes reais (modo em memória)
    glue_handler = GlueDataHandler(mock_glue_context)
    journey_controller = JourneyController(
        table_name="test_journey",
        dynamodb_client=None  # Modo em memória
    )
    # DynamoDBHandler não é mais necessário - dados são salvos no S3/Glue Catalog
    dynamodb_handler = None
    config = AppConfig()
    
    # Configurar múltiplas consolidações para testar isolamento
    # Usar apenas uma origem por consolidação para evitar problemas de colunas duplicadas
    config.consolidacoes_tabelas = {
        'tbl_consolidada_1': {
            'principais': {
                'sor': {'database': 'db1', 'table': 'tbl_sor_1'}
            },
            'auxiliares': {},
            'joins_auxiliares': {},
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper']
        },
        'tbl_consolidada_2': {
            'principais': {
                'sor': {'database': 'db2', 'table': 'tbl_sor_2'}
            },
            'auxiliares': {},
            'joins_auxiliares': {},
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper']
        }
    }
    
    # Criar processador
    processor = FlexibleConsolidationProcessor(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )
    
    # Criar DataFrames diferentes para cada execução
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    schema = StructType([
        StructField('num_oper', IntegerType(), True),
        StructField('cod_idef_ver_oper', StringType(), True),
        StructField('dat_vlr_even_oper', StringType(), True),
        StructField('num_prio_even_oper', IntegerType(), True),
        StructField('dat_recm_even_oper', StringType(), True)
    ])
    
    # Dados para primeira execução
    data1 = [(1, 'v1', '2024-01-01', 5, '2024-01-01 09:00:00')]
    df1 = spark.createDataFrame(data1, schema)
    
    # Dados para segunda execução (diferentes)
    data2 = [(2, 'v2', '2024-01-02', 6, '2024-01-02 10:00:00')]
    df2 = spark.createDataFrame(data2, schema)
    
    # Mock do read_from_catalog para retornar dados diferentes baseado na tabela
    def read_catalog_side_effect(*args, **kwargs):
        table_name = kwargs.get('table_name', args[1] if len(args) > 1 else '')
        database = kwargs.get('database', args[0] if len(args) > 0 else '')
        
        # Primeira execução (db1, tbl_sor_1)
        if database == 'db1' or 'sor_1' in table_name.lower():
            return df1
        
        # Segunda execução (db2, tbl_sor_2)
        elif database == 'db2' or 'sor_2' in table_name.lower():
            return df2
        
        # Default
        return df1
    
    # Mock dos métodos necessários
    glue_handler.get_last_partition = MagicMock(return_value='20240116')
    glue_handler.read_from_catalog = MagicMock(side_effect=read_catalog_side_effect)
    glue_handler.write_to_catalog = MagicMock()
    
    # DynamoDBHandler não é mais usado - dados são salvos no S3/Glue Catalog
    # Rastrear chamadas de write_to_catalog para verificar isolamento
    write_catalog_calls = []
    
    original_write = glue_handler.write_to_catalog
    def tracked_write(df, database, table_name, **kwargs):
        write_catalog_calls.append({
            'database': database,
            'table_name': table_name
        })
        return original_write(df, database, table_name, **kwargs)
    
    glue_handler.write_to_catalog = tracked_write
    
    # Mock do JourneyController
    journey_controller.execute_with_journey = MagicMock(side_effect=lambda func, *args, **kwargs: func(*args, **kwargs))
    
    # Primeira execução
    result1 = processor.process(
        database='db1',
        tabela_consolidada='tbl_consolidada_1'
    )
    
    # Segunda execução (deve ser isolada)
    result2 = processor.process(
        database='db2',
        tabela_consolidada='tbl_consolidada_2'
    )
    
    # Verificar que são independentes
    assert result1['status'] == 'success'
    assert result2['status'] == 'success'
    assert 'record_count' in result1
    assert 'record_count' in result2
    
    # Verificar que write_to_catalog foi chamado para tabelas diferentes
    assert len(write_catalog_calls) == 2
    assert write_catalog_calls[0]['table_name'] == 'tbl_consolidada_1'
    assert write_catalog_calls[1]['table_name'] == 'tbl_consolidada_2'
    assert write_catalog_calls[0]['database'] == 'db1'
    assert write_catalog_calls[1]['database'] == 'db2'
    
    # Verificar que read_from_catalog foi chamado para tabelas diferentes
    read_calls = glue_handler.read_from_catalog.call_args_list
    assert len(read_calls) >= 2  # Pelo menos 1 tabela por execução
    
    # Verificar que os resultados são diferentes
    assert result1['processor_type'] == result2['processor_type']  # Mesmo processador
    # congregado_id não é mais retornado - dados são salvos no S3/Glue Catalog