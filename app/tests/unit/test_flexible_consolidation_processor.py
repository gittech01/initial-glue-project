"""Testes unitários para utils/business/flexible_consolidation_processor.py"""
import unittest
import os
from unittest.mock import MagicMock, patch, Mock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from utils.business.flexible_consolidation_processor import FlexibleConsolidationProcessor
from utils.config.settings import AppConfig

# Configurar região AWS para testes
os.environ['AWS_DEFAULT_REGION'] = 'sa-east-1'


class TestFlexibleConsolidationProcessor(unittest.TestCase):
    """Testes para FlexibleConsolidationProcessor."""
    
    def setUp(self):
        """Setup para cada teste."""
        # Criar SparkSession para testes
        self.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("test") \
            .getOrCreate()
        
        # Mocks
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        
        # Configuração com consolidacoes_tabelas
        self.config = AppConfig()
        self.config.consolidacoes_tabelas = {
            'tbl_processado_operacao_consolidada': {
                'principais': {
                    'sor': {'database': 'db_test', 'table': 'tbl_processado_operacao_sor'},
                    'sot': {'database': 'db_test', 'table': 'tbl_processado_operacao_apropriada'}
                },
                'auxiliares': {
                    'sor': {
                        'oper': 'tbl_operecao_sor',
                        'event': 'tbl_evento_processado_sor',
                        'posi': 'tbl_posicao_operacao_sor'
                    },
                    'sot': {
                        'oper': 'tbl_operecao_apropriada',
                        'event': 'tbl_evento_processado_apropriada',
                        'posi': 'tbl_posicao_operacao_apropriada'
                    }
                },
                'joins_auxiliares': {
                    'sor': [
                        {'left': 'principal', 'right': 'oper', 'on': [('num_oper', 'num_oper')], 'how': 'inner'},
                        {'left': 'oper', 'right': 'event', 'on': [('num_oper', 'num_oper')], 'how': 'inner'},
                        {'left': 'oper', 'right': 'posi', 'on': [('num_oper', 'num_oper')], 'how': 'inner'}
                    ],
                    'sot': [
                        {'left': 'principal', 'right': 'oper', 'on': [('num_oper', 'num_oper')], 'how': 'inner'},
                        {'left': 'oper', 'right': 'event', 'on': [('num_oper', 'num_oper')], 'how': 'inner'},
                        {'left': 'oper', 'right': 'posi', 'on': [('num_oper', 'num_oper')], 'how': 'inner'}
                    ]
                },
                'chaves_principais': ['num_oper', 'cod_idef_ver_oper'],
                'campos_decisao': ['dat_vlr_even_oper', 'num_prio_even_oper', 'dat_recm_even_oper']
            }
        }
        
        # Criar DataFrames de teste
        schema_sor = StructType([
            StructField('num_oper', IntegerType(), True),
            StructField('cod_idef_ver_oper', StringType(), True),
            StructField('dat_vlr_even_oper', StringType(), True),
            StructField('num_prio_even_oper', IntegerType(), True),
            StructField('dat_recm_even_oper', StringType(), True)
        ])
        self.df_sor = self.spark.createDataFrame([
            (1, 'v1', '2024-01-01', 5, '2024-01-01 09:00:00'),
            (2, 'v1', '2024-01-01', 3, '2024-01-01 10:00:00')
        ], schema_sor)
        
        schema_sot = StructType([
            StructField('num_oper', IntegerType(), True),
            StructField('cod_idef_ver_oper', StringType(), True),
            StructField('dat_vlr_even_oper', StringType(), True),
            StructField('num_prio_even_oper', IntegerType(), True),
            StructField('dat_recm_even_oper', StringType(), True)
        ])
        self.df_sot = self.spark.createDataFrame([
            (1, 'v1', '2024-01-01', 4, '2024-01-01 08:00:00'),
            (2, 'v1', '2024-01-01', 5, '2024-01-01 11:00:00')
        ], schema_sot)
        
        # Criar processador
        self.processor = FlexibleConsolidationProcessor(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.config
        )
    
    def tearDown(self):
        """Limpeza após cada teste."""
        self.spark.stop()
    
    def test_init(self):
        """Testa inicialização do processador."""
        self.assertEqual(self.processor.glue_handler, self.mock_glue_handler)
        self.assertEqual(self.processor.consolidacoes_config, self.config.consolidacoes_tabelas)
    
    def test_init_without_consolidacoes(self):
        """Testa inicialização sem consolidacoes_tabelas no config."""
        config_empty = AppConfig()
        config_empty.consolidacoes_tabelas = {}
        
        processor = FlexibleConsolidationProcessor(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=config_empty
        )
        
        self.assertEqual(processor.consolidacoes_config, {})
    
    def test_read_data_missing_database(self):
        """Testa erro quando database não é fornecido."""
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(tabela_consolidada='tbl_processado_operacao_consolidada')
        
        self.assertIn("database", str(context.exception))
    
    def test_read_data_missing_tabela_and_config(self):
        """Testa erro quando tabela_consolidada e consolidation_config não são fornecidos."""
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(database='db_test')
        
        self.assertIn("tabela_consolidada", str(context.exception))
    
    def test_read_data_table_not_found(self):
        """Testa erro quando tabela_consolidada não existe em consolidacoes_tabelas."""
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(
                database='db_test',
                tabela_consolidada='tabela_inexistente'
            )
        
        self.assertIn("não encontrada", str(context.exception))
    
    def test_read_data_with_consolidation_config(self):
        """Testa leitura usando consolidation_config direto."""
        consolidation_config = {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_sor'}
            },
            'auxiliares': {},
            'joins_auxiliares': {}
        }
        
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.return_value = self.df_sor
        
        result = self.processor._read_data(
            database='db_test',
            consolidation_config=consolidation_config
        )
        
        self.assertIsNotNone(result)
        self.assertIn('origem', result.columns)
    
    def test_read_data_without_auxiliares(self):
        """Testa leitura sem auxiliares."""
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.return_value = self.df_sor
        
        # Config sem auxiliares
        config_simple = {
            'principais': {'sor': 'tbl_sor'},
            'auxiliares': {},
            'joins_auxiliares': {}
        }
        
        result = self.processor._read_data(
            database='db_test',
            consolidation_config=config_simple
        )
        
        self.assertIsNotNone(result)
        self.mock_glue_handler.read_from_catalog.assert_called()
    
    def test_read_origem_com_auxiliares(self):
        """Testa leitura com auxiliares e joins."""
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.side_effect = [
            self.df_sor,  # tabela principal
            self.df_sor,  # auxiliar 1
            self.df_sor   # auxiliar 2
        ]
        
        auxiliares = {
            'oper': 'tbl_oper',
            'event': 'tbl_event'
        }
        joins_auxiliares = [
            {'left': 'principal', 'right': 'oper', 'on': [('num_oper', 'num_oper')], 'how': 'inner'},
            {'left': 'oper', 'right': 'event', 'on': [('num_oper', 'num_oper')], 'how': 'inner'}
        ]
        
        result, particao = self.processor._read_origem_com_auxiliares(
            database='db_test',
            tabela_principal='tbl_principal',
            auxiliares=auxiliares,
            joins_auxiliares=joins_auxiliares
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(particao, '20240116')  # Verificar que partição foi retornada
        self.assertGreater(self.mock_glue_handler.read_from_catalog.call_count, 1)
    
    def test_read_origem_com_auxiliares_no_partition(self):
        """Testa leitura quando não há partição."""
        self.mock_glue_handler.get_last_partition.return_value = None
        self.mock_glue_handler.read_from_catalog.return_value = self.df_sor
        
        result, particao = self.processor._read_origem_com_auxiliares(
            database='db_test',
            tabela_principal='tbl_principal',
            auxiliares={},
            joins_auxiliares=[]
        )
        
        self.assertIsNotNone(result)
        self.assertIsNone(particao)  # Verificar que partição é None quando não há partição
        # Deve ser chamado sem filtro de partição
        call_args = self.mock_glue_handler.read_from_catalog.call_args
        self.assertIsNone(call_args[1].get('filter'))
    
    def test_read_origem_com_auxiliares_join_error(self):
        """Testa erro em join com coluna inexistente."""
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        
        # DataFrame sem coluna 'num_oper'
        df_empty = self.spark.createDataFrame([], StructType([StructField('id', IntegerType())]))
        
        self.mock_glue_handler.read_from_catalog.side_effect = [
            df_empty,  # tabela principal
            self.df_sor  # auxiliar
        ]
        
        auxiliares = {'oper': 'tbl_oper'}
        joins_auxiliares = [
            {'left': 'principal', 'right': 'oper', 'on': [('num_oper', 'num_oper')], 'how': 'inner'}
        ]
        
        with self.assertRaises(ValueError):
            self.processor._read_origem_com_auxiliares(
                database='db_test',
                tabela_principal='tbl_principal',
                auxiliares=auxiliares,
                joins_auxiliares=joins_auxiliares
            )
    
    def test_transform_data_simple(self):
        """Testa transformação sem campos de decisão."""
        # Preparar dados
        df_unificado = self.df_sor.withColumn('origem', F.lit('online'))
        
        self.processor._current_config = {
            'chaves_principais': [],
            'campos_decisao': []
        }
        self.processor._current_tabela_consolidada = 'tbl_test'
        self.processor._dataframes_originais = {}
        
        result = self.processor._transform_data(df_unificado)
        
        self.assertIsInstance(result, dict)
        self.assertIn('df_consolidado', result)
    
    def test_transform_data_with_ranking_preferencia_online_empate(self):
        """
        Testa que em caso de empate, a origem 'online' é preferida sobre 'batch'.
        
        Cenário: Dois registros com mesmas chaves principais e mesmos valores
        em todos os campos de decisão, mas origens diferentes.
        Resultado esperado: Apenas o registro 'online' deve ter rank=1.
        """
        from datetime import datetime
        
        # Criar dados com EMPATE TOTAL (mesmos valores em todos os campos de decisão)
        schema = StructType([
            StructField('num_oper', IntegerType(), True),
            StructField('cod_idef_ver_oper', StringType(), True),
            StructField('dat_vlr_even_oper', StringType(), True),
            StructField('num_prio_even_oper', IntegerType(), True),
            StructField('dat_recm_even_oper', StringType(), True),
            StructField('origem', StringType(), True)
        ])
        
        # Dois registros com EMPATE TOTAL (mesmos valores em todos os campos de decisão)
        data = [
            (12345, 'v1', '2024-01-15', 5, '2024-01-15 10:00:00', 'online'),
            (12345, 'v1', '2024-01-15', 5, '2024-01-15 10:00:00', 'batch')  # EMPATE TOTAL
        ]
        
        df_unificado = self.spark.createDataFrame(data, schema)
        
        # Configurar regra com chaves principais e campos de decisão
        self.processor._current_config = {
            'chaves_principais': ['num_oper', 'cod_idef_ver_oper'],
            'campos_decisao': ['dat_vlr_even_oper', 'num_prio_even_oper', 'dat_recm_even_oper']
        }
        self.processor._current_tabela_consolidada = 'tbl_test'
        
        # Executar transformação
        result = self.processor._transform_data(df_unificado)
        
        # Verificar resultado
        self.assertIsInstance(result, dict)
        self.assertIn('df_consolidado', result)
        
        df_resultado = result['df_consolidado']
        
        # Verificar que apenas o registro 'online' está no resultado
        # (em caso de empate, 'online' deve ser preferido)
        registros_resultado = df_resultado.collect()
        
        # Deve haver apenas 1 registro (o 'online')
        self.assertEqual(len(registros_resultado), 1, 
                        "Em caso de empate, apenas 'online' deve ser escolhido")
        
        # O registro escolhido deve ser 'online'
        registro_escolhido = registros_resultado[0]
        self.assertEqual(registro_escolhido['origem'], 'online',
                        "Em caso de empate, 'online' deve ser preferido sobre 'batch'")
        
        # Verificar que o registro escolhido tem os valores corretos
        self.assertEqual(registro_escolhido['num_oper'], 12345)
        self.assertEqual(registro_escolhido['cod_idef_ver_oper'], 'v1')
    
    def test_transform_data_with_ranking(self):
        """Testa transformação com ranking."""
        # Criar DataFrame unificado
        df_sor_marked = self.df_sor.withColumn('origem', F.lit('online'))
        df_sot_marked = self.df_sot.withColumn('origem', F.lit('batch'))
        df_unificado = df_sor_marked.unionByName(df_sot_marked)
        
        self.processor._current_config = {
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper', 'num_prio_even_oper']
        }
        self.processor._current_tabela_consolidada = 'tbl_test'
        self.processor._dataframes_originais = {'sor': self.df_sor, 'sot': self.df_sot}
        
        result = self.processor._transform_data(df_unificado)
        
        self.assertIsInstance(result, dict)
        self.assertIn('df_consolidado', result)
        df_consolidado = result['df_consolidado']
        self.assertIsInstance(df_consolidado, DataFrame)
    
    def test_transform_data_config_from_kwargs(self):
        """Testa transformação obtendo config de kwargs."""
        df_unificado = self.df_sor.withColumn('origem', F.lit('online'))
        
        # Limpar atributos internos
        if hasattr(self.processor, '_current_config'):
            delattr(self.processor, '_current_config')
        
        consolidation_config = {
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper']
        }
        
        result = self.processor._transform_data(
            df_unificado,
            consolidation_config=consolidation_config
        )
        
        self.assertIsInstance(result, dict)
    
    def test_transform_data_no_config_error(self):
        """Testa erro quando config não está disponível."""
        df_unificado = self.df_sor.withColumn('origem', F.lit('online'))
        
        # Limpar atributos internos
        if hasattr(self.processor, '_current_config'):
            delattr(self.processor, '_current_config')
        
        with self.assertRaises(ValueError):
            self.processor._transform_data(df_unificado)
    
    def test_join_com_registros_completos(self):
        """Testa join com registros completos."""
        # Criar DataFrame com vencedores
        df_ranked = self.spark.createDataFrame([
            (1, 'v1', 'online'),
            (2, 'v1', 'batch')
        ], StructType([
            StructField('num_oper', IntegerType()),
            StructField('cod_idef_ver_oper', StringType()),
            StructField('origem', StringType())
        ]))
        
        data_originais = {
            'sor': self.df_sor,
            'sot': self.df_sot
        }
        
        regra_cfg = {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_sor'},
                'sot': {'database': 'db_test', 'table': 'tbl_sot'}
            }
        }
        
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.side_effect = [self.df_sor, self.df_sot]
        
        result = self.processor._join_com_registros_completos(
            df_ranked=df_ranked,
            data_originais=data_originais,
            regra_cfg=regra_cfg,
            database='db_test',
            chaves_principais=['num_oper', 'cod_idef_ver_oper']
        )
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, DataFrame)
    
    def test_get_congregado_key(self):
        """Testa geração de chave de congregado."""
        key = self.processor._get_congregado_key(
            database='db_test',
            tabela_consolidada='tbl_consolidada'
        )
        
        self.assertEqual(key, 'db_test_tbl_consolidada')
    
    def test_get_congregado_metadata(self):
        """Testa geração de metadados de congregado."""
        metadata = self.processor._get_congregado_metadata(
            database='db_test',
            tabela_consolidada='tbl_consolidada',
            data={'sor': self.df_sor, 'sot': self.df_sot}
        )
        
        self.assertIsInstance(metadata, dict)
        self.assertEqual(metadata['processor_type'], 'FlexibleConsolidationProcessor')
        self.assertEqual(metadata['tabela_consolidada'], 'tbl_consolidada')
        self.assertIn('origens', metadata)
    
    def test_should_write_output(self):
        """Testa determinação de escrita de output."""
        self.assertTrue(self.processor._should_write_output(output_path='s3://bucket/path'))
        self.assertFalse(self.processor._should_write_output())
    
    def test_write_output_to_catalog(self):
        """Testa escrita no catálogo Glue."""
        df_result = MagicMock()
        transformed_data = {
            'df_consolidado': self.df_sor,
            'record_count': 10
        }
        
        self.processor._write_output(
            df=df_result,
            transformed_data=transformed_data,
            output_path='s3://bucket/path',
            database='db_test',
            tabela_consolidada='tbl_output'
        )
        
        self.mock_glue_handler.write_to_catalog.assert_called_once()
    
    def test_write_output_to_s3(self):
        """Testa escrita no S3."""
        df_result = MagicMock()
        transformed_data = {
            'df_consolidado': self.df_sor,
            'record_count': 10
        }
        
        self.processor._write_output(
            df=df_result,
            transformed_data=transformed_data,
            output_path='s3://bucket/path'
        )
        
        self.mock_glue_handler.write_to_s3.assert_called_once()
    
    def test_get_processor_name(self):
        """Testa nome do processador."""
        self.assertEqual(
            self.processor.get_processor_name(),
            'FlexibleConsolidationProcessor'
        )
    
    def test_process_full_flow(self):
        """Testa fluxo completo do process."""
        # Mock dos métodos internos - usar return_value para evitar StopIteration
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        # Usar uma função que retorna DataFrame correto baseado no nome da tabela
        def read_mock(*args, **kwargs):
            table_name = kwargs.get('table_name', args[1] if len(args) > 1 else '')
            # Retornar df_sor para tabelas sor, df_sot para tabelas sot
            if 'sor' in table_name.lower():
                return self.df_sor
            elif 'sot' in table_name.lower() or 'apropriada' in table_name.lower():
                return self.df_sot
            return self.df_sor  # Default
        self.mock_glue_handler.read_from_catalog = read_mock
        # DynamoDBHandler não é mais usado para salvar congregados
        # Dados são salvos diretamente no S3/Glue Catalog via _write_output
        self.mock_glue_handler.write_to_catalog = MagicMock()
        
        result = self.processor.process(
            database='db_test',
            tabela_consolidada='tbl_processado_operacao_consolidada'
        )
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['status'], 'success')
        # congregado_id não é mais retornado - dados são salvos no S3/Glue Catalog
    
    def test_process_with_output_path(self):
        """Testa process com output_path."""
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        # Usar uma função que sempre retorna o DataFrame para evitar esgotar side_effect
        def read_mock(*args, **kwargs):
            return self.df_sor
        self.mock_glue_handler.read_from_catalog = read_mock
        self.mock_glue_handler.write_to_catalog = MagicMock()
        # DynamoDBHandler não é mais usado para salvar congregados
        
        result = self.processor.process(
            database='db_test',
            tabela_consolidada='tbl_processado_operacao_consolidada'
        )
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['status'], 'success')
        self.mock_glue_handler.write_to_catalog.assert_called()


if __name__ == '__main__':
    unittest.main()
