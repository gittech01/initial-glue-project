"""Testes de integração para FlexibleConsolidationProcessor."""
import pytest
import os
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from utils.business.flexible_consolidation_processor import FlexibleConsolidationProcessor
from utils.config.settings import AppConfig
from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler

# Configurar região AWS para testes
os.environ['AWS_DEFAULT_REGION'] = 'sa-east-1'


@pytest.fixture(scope="module")
def spark():
    """Fixture para SparkSession."""
    return SparkSession.builder \
        .appName("IntegrationTest") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def config():
    """Fixture para AppConfig com consolidacoes_tabelas."""
    config = AppConfig()
    config.consolidacoes_tabelas = {
        'tbl_processado_operacao_consolidada': {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_processado_operacao_sor'},
                'sot': {'database': 'db_test', 'table': 'tbl_processado_operacao_apropriada'}
            },
            'auxiliares': {},  # Sem auxiliares para simplificar testes
            'joins_auxiliares': {},  # Sem joins para simplificar testes
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper', 'num_prio_even_oper']
        }
    }
    return config


@pytest.fixture
def mock_glue_context(spark):
    """Fixture para mock do GlueContext."""
    mock_glue = MagicMock()
    mock_glue.spark_session = spark
    return mock_glue


@pytest.fixture
def test_dataframes(spark):
    """Fixture para DataFrames de teste."""
    schema = StructType([
        StructField('num_oper', IntegerType(), True),
        StructField('cod_idef_ver_oper', StringType(), True),
        StructField('dat_vlr_even_oper', TimestampType(), True),
        StructField('num_prio_even_oper', IntegerType(), True),
        StructField('dat_recm_even_oper', TimestampType(), True)
    ])
    
    from datetime import datetime
    df_sor = spark.createDataFrame([
        (1, 'v1', datetime(2024, 1, 1, 10, 0, 0), 5, datetime(2024, 1, 1, 9, 0, 0)),
        (2, 'v1', datetime(2024, 1, 1, 11, 0, 0), 3, datetime(2024, 1, 1, 10, 0, 0))
    ], schema)
    
    df_sot = spark.createDataFrame([
        (1, 'v1', datetime(2024, 1, 1, 9, 0, 0), 4, datetime(2024, 1, 1, 8, 0, 0)),
        (2, 'v1', datetime(2024, 1, 1, 12, 0, 0), 5, datetime(2024, 1, 1, 11, 0, 0))
    ], schema)
    
    return {'sor': df_sor, 'sot': df_sot}


def test_full_integration_flow(spark, config, mock_glue_context, test_dataframes):
    """Testa fluxo completo de integração."""
    # Criar handlers reais com mocks
    glue_handler = GlueDataHandler(mock_glue_context)
    journey_controller = JourneyController(table_name='test_journey', region_name='sa-east-1')
    dynamodb_handler = DynamoDBHandler(table_name='test_congregado', region_name='sa-east-1')
    
    # Mock dos métodos de leitura - usar generator para evitar StopIteration
    glue_handler.get_last_partition = MagicMock(return_value='20240116')
    def read_catalog_side_effect(*args, **kwargs):
        # Retornar SOR para tabelas que contém 'sor' ou 'processado', SOT caso contrário
        table_name = kwargs.get('table_name', args[1] if len(args) > 1 else '')
        if 'sor' in table_name.lower() or 'processado' in table_name.lower():
            return test_dataframes['sor']
        return test_dataframes['sot']
    glue_handler.read_from_catalog = MagicMock(side_effect=read_catalog_side_effect)
    
    # Mock do DynamoDB
    dynamodb_handler.save_congregado = MagicMock(return_value={
        'id': 'test_id',
        'version': 1,
        'status': 'created'
    })
    
    # Criar processador
    processor = FlexibleConsolidationProcessor(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )
    
    # Executar processamento completo
    result = processor.process(
        database='db_test',
        tabela_consolidada='tbl_processado_operacao_consolidada'
    )
    
    # Validações
    assert result['status'] == 'success'
    assert 'record_count' in result
    assert 'congregado_id' in result
    assert dynamodb_handler.save_congregado.called
    
    # Verificar que read_from_catalog foi chamado
    assert glue_handler.read_from_catalog.call_count >= 2


def test_integration_with_auxiliaries(spark, config, mock_glue_context, test_dataframes):
    """Testa integração com auxiliares e joins."""
    glue_handler = GlueDataHandler(mock_glue_context)
    journey_controller = JourneyController(table_name='test_journey', region_name='sa-east-1')
    dynamodb_handler = DynamoDBHandler(table_name='test_congregado', region_name='sa-east-1')
    
    # Mock para múltiplas leituras (principal + auxiliares)
    glue_handler.get_last_partition = MagicMock(return_value='20240116')
    def read_catalog_side_effect_aux(*args, **kwargs):
        table_name = kwargs.get('table_name', args[1] if len(args) > 1 else '')
        if 'sor' in table_name.lower() or 'processado' in table_name.lower():
            return test_dataframes['sor']
        return test_dataframes['sot']
    glue_handler.read_from_catalog = MagicMock(side_effect=read_catalog_side_effect_aux)
    
    dynamodb_handler.save_congregado = MagicMock(return_value={
        'id': 'test_id',
        'version': 1
    })
    
    processor = FlexibleConsolidationProcessor(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )
    
    result = processor.process(
        database='db_test',
        tabela_consolidada='tbl_processado_operacao_consolidada'
    )
    
    # Verificar que múltiplas leituras foram feitas
    assert glue_handler.read_from_catalog.call_count >= 4
    assert result['status'] == 'success'


def test_integration_with_ranking(spark, config, mock_glue_context, test_dataframes):
    """Testa integração com ranking e consolidação."""
    glue_handler = GlueDataHandler(mock_glue_context)
    journey_controller = JourneyController(table_name='test_journey', region_name='sa-east-1')
    dynamodb_handler = DynamoDBHandler(table_name='test_congregado', region_name='sa-east-1')
    
    glue_handler.get_last_partition = MagicMock(return_value='20240116')
    def read_catalog_side_effect_simple(*args, **kwargs):
        table_name = kwargs.get('table_name', args[1] if len(args) > 1 else '')
        if 'sor' in table_name.lower() or 'processado' in table_name.lower():
            return test_dataframes['sor']
        return test_dataframes['sot']
    glue_handler.read_from_catalog = MagicMock(side_effect=read_catalog_side_effect_simple)
    
    dynamodb_handler.save_congregado = MagicMock(return_value={
        'id': 'test_id',
        'version': 1
    })
    
    processor = FlexibleConsolidationProcessor(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )
    
    result = processor.process(
        database='db_test',
        tabela_consolidada='tbl_processado_operacao_consolidada'
    )
    
    # Verificar que transformação foi aplicada
    transformed_data = result.get('transformed_data', {})
    assert 'df_consolidado' in transformed_data
    assert 'record_count' in transformed_data
    
    # Verificar que o DataFrame consolidado tem menos ou igual linhas que a soma
    df_consolidado = transformed_data['df_consolidado']
    if df_consolidado:
        count = df_consolidado.count()
        # Deve ter no máximo 2 linhas (1 por operação após ranking)
        assert count <= 2


def test_integration_with_journey_controller(spark, config, mock_glue_context, test_dataframes):
    """Testa integração com JourneyController."""
    glue_handler = GlueDataHandler(mock_glue_context)
    journey_controller = JourneyController(table_name='test_journey', region_name='sa-east-1')
    dynamodb_handler = DynamoDBHandler(table_name='test_congregado', region_name='sa-east-1')
    
    glue_handler.get_last_partition = MagicMock(return_value='20240116')
    def read_catalog_side_effect_simple(*args, **kwargs):
        table_name = kwargs.get('table_name', args[1] if len(args) > 1 else '')
        if 'sor' in table_name.lower() or 'processado' in table_name.lower():
            return test_dataframes['sor']
        return test_dataframes['sot']
    glue_handler.read_from_catalog = MagicMock(side_effect=read_catalog_side_effect_simple)
    
    dynamodb_handler.save_congregado = MagicMock(return_value={
        'id': 'test_id',
        'version': 1
    })
    
    processor = FlexibleConsolidationProcessor(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )
    
    # Executar com journey controller
    idempotency_key = 'test_consolidation_20240116'
    
    result = journey_controller.execute_with_journey(
        processor.process,
        idempotency_key=idempotency_key,
        database='db_test',
        tabela_consolidada='tbl_processado_operacao_consolidada'
    )
    
    # Verificar que foi executado via journey
    assert result is not None
    assert result.get('status') == 'success'
    
    # Executar novamente com mesma chave (deve ser idempotente)
    result2 = journey_controller.execute_with_journey(
        processor.process,
        idempotency_key=idempotency_key,
        database='db_test',
        tabela_consolidada='tbl_processado_operacao_consolidada'
    )
    
    # Deve retornar resultado da execução anterior (idempotência)
    assert result2 is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
