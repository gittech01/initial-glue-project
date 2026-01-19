"""Testes adicionais para flexible_consolidation_processor.py para aumentar cobertura"""
import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from utils.business.flexible_consolidation_processor import FlexibleConsolidationProcessor
from utils.config.settings import AppConfig
import os

# Configurar região AWS para testes
os.environ['AWS_DEFAULT_REGION'] = 'sa-east-1'


class TestFlexibleConsolidationProcessorAdditional(unittest.TestCase):
    """Testes adicionais para aumentar cobertura."""
    
    @classmethod
    def setUpClass(cls):
        """Setup para toda a classe."""
        cls.spark = SparkSession.builder \
            .appName("TestAdditional") \
            .master("local[1]") \
            .getOrCreate()
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.config = AppConfig()
        
        self.processor = FlexibleConsolidationProcessor(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=None,
            config=self.config
        )
        
        # Criar DataFrames de teste
        schema = StructType([
            StructField('num_oper', IntegerType(), True),
            StructField('cod_idef_ver_oper', StringType(), True),
            StructField('dat_vlr_even_oper', TimestampType(), True),
            StructField('num_prio_even_oper', IntegerType(), True),
            StructField('dat_recm_even_oper', TimestampType(), True)
        ])
        
        from datetime import datetime
        data = [
            (1, 'v1', datetime(2024, 1, 1, 10, 0, 0), 5, datetime(2024, 1, 1, 9, 0, 0)),
            (2, 'v1', datetime(2024, 1, 1, 11, 0, 0), 3, datetime(2024, 1, 1, 10, 0, 0))
        ]
        self.df_test = self.spark.createDataFrame(data, schema)
    
    def test_read_data_missing_database(self):
        """Testa que _read_data levanta erro se database não for fornecido."""
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(tabela_consolidada='tbl_test')
        self.assertIn('database', str(context.exception))
    
    def test_read_data_missing_tabela_and_config(self):
        """Testa que _read_data levanta erro se nem tabela_consolidada nem consolidation_config forem fornecidos."""
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(database='db_test')
        self.assertIn('tabela_consolidada ou consolidation_config', str(context.exception))
    
    def test_read_data_table_not_found(self):
        """Testa que _read_data levanta erro se tabela_consolidada não estiver em consolidacoes_config."""
        self.processor.consolidacoes_config = {}
        
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(database='db_test', tabela_consolidada='tbl_inexistente')
        self.assertIn('não encontrada', str(context.exception))
    
    def test_read_data_no_principais(self):
        """Testa que _read_data levanta erro se configuração não tiver 'principais'."""
        config_invalida = {
            'auxiliares': {},
            'joins_auxiliares': {}
        }
        
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(
                database='db_test',
                consolidation_config=config_invalida
            )
        self.assertIn('principais', str(context.exception))
    
    def test_read_data_origem_sem_table(self):
        """Testa que _read_data levanta erro se origem não tiver 'table'."""
        config_invalida = {
            'principais': {
                'sor': {'database': 'db_test'}  # Sem 'table'
            }
        }
        
        with self.assertRaises(ValueError) as context:
            self.processor._read_data(
                database='db_test',
                consolidation_config=config_invalida
            )
        self.assertIn('table', str(context.exception))
    
    def test_read_data_single_origin(self):
        """Testa _read_data com apenas uma origem."""
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.return_value = self.df_test
        
        config = {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_sor'}
            },
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper']
        }
        
        result = self.processor._read_data(
            database='db_test',
            consolidation_config=config
        )
        
        self.assertIsNotNone(result)
        self.mock_glue_handler.read_from_catalog.assert_called()
    
    def test_read_origem_com_auxiliares_exception(self):
        """Testa _read_origem_com_auxiliares quando get_last_partition levanta exceção."""
        self.mock_glue_handler.get_last_partition.side_effect = Exception("Erro ao obter partição")
        self.mock_glue_handler.read_from_catalog.return_value = self.df_test
        
        result, particao = self.processor._read_origem_com_auxiliares(
            database='db_test',
            tabela_principal='tbl_principal',
            auxiliares={},
            joins_auxiliares=[]
        )
        
        # Deve continuar mesmo com erro na partição
        self.assertIsNotNone(result)
        self.assertIsNone(particao)
    
    def test_read_origem_com_auxiliares_join_error(self):
        """Testa _read_origem_com_auxiliares quando join falha."""
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.return_value = self.df_test
        
        auxiliares = {'oper': 'tbl_oper'}
        joins_auxiliares = [
            {'left': 'principal', 'right': 'oper', 'on': [('num_oper', 'num_oper')], 'how': 'inner'}
        ]
        
        # Simular erro no join
        df_with_error = MagicMock()
        df_with_error.join.side_effect = Exception("Join error")
        self.mock_glue_handler.read_from_catalog.return_value = df_with_error
        
        with self.assertRaises(Exception):
            self.processor._read_origem_com_auxiliares(
                database='db_test',
                tabela_principal='tbl_principal',
                auxiliares=auxiliares,
                joins_auxiliares=joins_auxiliares
            )
    
    def test_transform_data_no_config_error(self):
        """Testa _transform_data quando não há configuração."""
        self.processor._current_config = None
        
        with self.assertRaises(ValueError) as context:
            self.processor._transform_data(self.df_test)
        self.assertIn('Configuração', str(context.exception))
    
    def test_transform_data_config_from_kwargs(self):
        """Testa _transform_data quando configuração vem de kwargs."""
        config = {
            'chaves_principais': ['num_oper'],
            'campos_decisao': ['dat_vlr_even_oper']
        }
        
        result = self.processor._transform_data(
            self.df_test,
            consolidation_config=config
        )
        
        self.assertIsNotNone(result)
        self.assertIn('df_consolidado', result)
    
    def test_join_com_registros_completos_no_metadata(self):
        """Testa _join_com_registros_completos quando data_originais não tem metadata."""
        from pyspark.sql import functions as F
        
        # Criar DataFrame ranked
        df_ranked = self.df_test.withColumn('origem', F.lit('online')).withColumn('rank', F.lit(1))
        
        # data_originais sem metadata estruturado
        data_originais = {
            'sor': 'string_invalida'  # Não é dict com metadata
        }
        
        regra_cfg = {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_sor'}
            }
        }
        
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.return_value = self.df_test
        
        result = self.processor._join_com_registros_completos(
            df_ranked=df_ranked,
            data_originais=data_originais,
            regra_cfg=regra_cfg,
            database='db_test',
            chaves_principais=['num_oper']
        )
        
        # Deve usar fallback da configuração
        self.assertIsNotNone(result)
    
    def test_join_com_registros_completos_no_table(self):
        """Testa _join_com_registros_completos quando origem não tem table."""
        from pyspark.sql import functions as F
        
        df_ranked = self.df_test.withColumn('origem', F.lit('online')).withColumn('rank', F.lit(1))
        
        data_originais = {
            'sor': {
                'database': 'db_test',
                'table': None  # Sem table
            }
        }
        
        regra_cfg = {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_sor'}
            }
        }
        
        result = self.processor._join_com_registros_completos(
            df_ranked=df_ranked,
            data_originais=data_originais,
            regra_cfg=regra_cfg,
            database='db_test',
            chaves_principais=['num_oper']
        )
        
        # Deve pular origem sem table
        self.assertIsNotNone(result)
    
    def test_join_com_registros_completos_partition_fallback(self):
        """Testa _join_com_registros_completos quando partição não está armazenada."""
        from pyspark.sql import functions as F
        
        df_ranked = self.df_test.withColumn('origem', F.lit('online')).withColumn('rank', F.lit(1))
        
        data_originais = {
            'sor': {
                'database': 'db_test',
                'table': 'tbl_sor',
                'particao': None  # Sem partição armazenada
            }
        }
        
        regra_cfg = {
            'principais': {
                'sor': {'database': 'db_test', 'table': 'tbl_sor'}
            }
        }
        
        self.mock_glue_handler.get_last_partition.return_value = '20240116'
        self.mock_glue_handler.read_from_catalog.return_value = self.df_test
        
        result = self.processor._join_com_registros_completos(
            df_ranked=df_ranked,
            data_originais=data_originais,
            regra_cfg=regra_cfg,
            database='db_test',
            chaves_principais=['num_oper']
        )
        
        # Deve usar fallback para obter partição
        self.assertIsNotNone(result)
        self.mock_glue_handler.get_last_partition.assert_called()
    
    def test_should_write_output_with_output_path_only(self):
        """Testa _should_write_output apenas com output_path (deve retornar False)."""
        result = self.processor._should_write_output(output_path='s3://bucket/path')
        self.assertFalse(result)
    
    def test_should_write_output_with_table_and_database(self):
        """Testa _should_write_output com tabela_consolidada e database."""
        result = self.processor._should_write_output(
            tabela_consolidada='tbl_test',
            database='db_test'
        )
        self.assertTrue(result)
    
    def test_should_write_output_with_both(self):
        """Testa _should_write_output com todos os parâmetros."""
        result = self.processor._should_write_output(
            output_path='s3://bucket/path',
            tabela_consolidada='tbl_test',
            database='db_test'
        )
        self.assertTrue(result)
    
    def test_should_write_output_with_neither(self):
        """Testa _should_write_output sem parâmetros."""
        result = self.processor._should_write_output()
        self.assertFalse(result)
    
    def test_should_write_output_only_table(self):
        """Testa _should_write_output apenas com tabela_consolidada."""
        result = self.processor._should_write_output(tabela_consolidada='tbl_test')
        self.assertFalse(result)
    
    def test_should_write_output_only_database(self):
        """Testa _should_write_output apenas com database."""
        result = self.processor._should_write_output(database='db_test')
        self.assertFalse(result)
    
    @classmethod
    def tearDownClass(cls):
        """Cleanup após todos os testes."""
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
