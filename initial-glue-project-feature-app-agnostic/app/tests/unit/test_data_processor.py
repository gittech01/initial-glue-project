"""Testes unitários para utils/business/data_processor.py"""
import unittest
from unittest.mock import MagicMock, patch
from utils.business.data_processor import DataProcessor
from utils.config.settings import AppConfig

class TestDataProcessor(unittest.TestCase):
    """Testes para DataProcessor."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        self.config = AppConfig()
        
        self.processor = DataProcessor(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.config
        )
    
    def test_init(self):
        """Testa inicialização do processador."""
        self.assertEqual(self.processor.glue_handler, self.mock_glue_handler)
        self.assertEqual(self.processor.journey_controller, self.mock_journey_controller)
        self.assertEqual(self.processor.dynamodb_handler, self.mock_dynamodb_handler)
        self.assertEqual(self.processor.config, self.config)
    
    def test_read_data(self):
        """Testa método _read_data."""
        mock_df = MagicMock()
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        df = self.processor._read_data(database="test_db", table_name="test_table")
        
        self.assertEqual(df, mock_df)
        self.mock_glue_handler.read_from_catalog.assert_called_once_with(
            database="test_db",
            table_name="test_table"
        )
    
    def test_get_congregado_key(self):
        """Testa geração de chave para congregado."""
        key = self.processor._get_congregado_key(
            database="test_db",
            table_name="test_table"
        )
        self.assertEqual(key, "test_db_test_table")
    
    @patch('utils.business.data_processor.logger')
    def test_process_data_success(self, mock_logger):
        """Testa processamento bem-sucedido de dados."""
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.schema.fields = []
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        # Mock DynamoDB
        self.mock_dynamodb_handler.save_congregado.return_value = {
            'id': 'test_id',
            'status': 'created'
        }
        
        result = self.processor.process_data(
            database="test_db",
            table_name="test_table",
            output_path="s3://bucket/output"
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['record_count'], 100)
        self.mock_glue_handler.read_from_catalog.assert_called_once()
        self.mock_dynamodb_handler.save_congregado.assert_called_once()
        self.mock_glue_handler.write_to_s3.assert_called_once()
    
    def test_process_data_without_output_path(self):
        """Testa processamento sem caminho de saída."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        mock_df.schema.fields = []
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        self.mock_dynamodb_handler.save_congregado.return_value = {'id': 'test_id'}
        
        result = self.processor.process_data(
            database="test_db",
            table_name="test_table"
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertIsNone(result['output_path'])
        self.mock_glue_handler.write_to_s3.assert_not_called()
    
    def test_process_data_exception(self):
        """Testa tratamento de exceção no processamento."""
        self.mock_glue_handler.read_from_catalog.side_effect = Exception("Read error")
        
        with self.assertRaises(Exception):
            self.processor.process_data("test_db", "test_table")
    
    def test_aggregate_data(self):
        """Testa agregação de dados."""
        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        from pyspark.sql import SparkSession
        
        # Criar DataFrame de teste
        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        data = [(1, 100, "A"), (2, 200, "B"), (3, 300, "C")]
        df = spark.createDataFrame(data, schema)
        
        aggregated = self.processor._aggregate_data(df)
        
        self.assertEqual(aggregated['total_rows'], 3)
        self.assertIn('numeric_columns', aggregated)
        spark.stop()
    
    def test_process_multiple_tables(self):
        """Testa processamento de múltiplas tabelas."""
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.schema.fields = []
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        self.mock_dynamodb_handler.save_congregado.return_value = {'id': 'test_id'}
        
        tables = [
            ("db1", "table1"),
            ("db2", "table2")
        ]
        
        results = self.processor.process_multiple_tables(
            tables=tables,
            output_base_path="s3://bucket/output"
        )
        
        self.assertEqual(len(results), 2)
        self.assertIn("db1.table1", results)
        self.assertIn("db2.table2", results)
        self.assertEqual(self.mock_glue_handler.read_from_catalog.call_count, 2)
    
    def test_process_multiple_tables_with_error(self):
        """Testa processamento de múltiplas tabelas com erro em uma."""
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.schema.fields = []
        
        # Primeira chamada sucesso, segunda falha
        self.mock_glue_handler.read_from_catalog.side_effect = [
            mock_df,
            Exception("Error on second table")
        ]
        
        self.mock_dynamodb_handler.save_congregado.return_value = {'id': 'test_id'}
        
        tables = [
            ("db1", "table1"),
            ("db2", "table2")
        ]
        
        results = self.processor.process_multiple_tables(
            tables=tables,
            output_base_path="s3://bucket/output"
        )
        
        self.assertEqual(len(results), 2)
        # Primeira tabela deve ter sucesso
        self.assertEqual(results["db1.table1"]['status'], 'success')
        # Segunda tabela deve ter erro (process_multiple_tables captura exceções)
        self.assertEqual(results["db2.table2"]['status'], 'error')
        self.assertIn('error', results["db2.table2"])

if __name__ == '__main__':
    unittest.main()
