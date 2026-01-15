"""Testes unitários para utils/business/sales_analyzer.py"""
import unittest
from unittest.mock import MagicMock, patch
from utils.business.sales_analyzer import SalesAnalyzer
from utils.config.settings import AppConfig

class TestSalesAnalyzer(unittest.TestCase):
    """Testes para SalesAnalyzer."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        self.config = AppConfig()
        
        self.analyzer = SalesAnalyzer(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.config
        )
    
    def test_init(self):
        """Testa inicialização do analisador."""
        self.assertEqual(self.analyzer.glue_handler, self.mock_glue_handler)
        self.assertEqual(self.analyzer.journey_controller, self.mock_journey_controller)
        self.assertEqual(self.analyzer.dynamodb_handler, self.mock_dynamodb_handler)
        self.assertEqual(self.analyzer.config, self.config)
    
    @patch('utils.business.sales_analyzer.SalesAnalyzer._analisar_dados')
    def test_analisar_vendas_success(self, mock_analisar):
        """Testa análise bem-sucedida de vendas."""
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ['id', 'valor', 'categoria']
        # Mock filter retorna o mesmo DataFrame
        mock_df.filter.return_value = mock_df
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        # Mock análise
        mock_analisar.return_value = {
            'periodo': '2024-01',
            'total_registros': 100
        }
        
        # Mock DynamoDB
        self.mock_dynamodb_handler.save_congregado.return_value = {
            'id': 'vendas_test_db_test_table_2024-01',
            'status': 'created'
        }
        
        result = self.analyzer.analisar_vendas(
            database="test_db",
            table_name="test_table",
            periodo="2024-01",
            output_path="s3://bucket/output"
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['periodo'], '2024-01')
        self.mock_glue_handler.read_from_catalog.assert_called_once()
        self.mock_dynamodb_handler.save_congregado.assert_called_once()
        self.mock_glue_handler.write_to_s3.assert_called_once()
    
    @patch('utils.business.sales_analyzer.SalesAnalyzer._analisar_dados')
    def test_analisar_vendas_without_output(self, mock_analisar):
        """Testa análise sem caminho de saída."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        mock_df.columns = ['id', 'valor']
        # Mock filter retorna o mesmo DataFrame
        mock_df.filter.return_value = mock_df
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        # Mock análise
        mock_analisar.return_value = {
            'periodo': '2024-01',
            'total_registros': 50
        }
        
        self.mock_dynamodb_handler.save_congregado.return_value = {'id': 'test_id'}
        
        result = self.analyzer.analisar_vendas(
            database="test_db",
            table_name="test_table",
            periodo="2024-01"
        )
        
        self.assertEqual(result['status'], 'success')
        self.assertIsNone(result['output_path'])
        self.mock_glue_handler.write_to_s3.assert_not_called()
    
    def test_analisar_vendas_exception(self):
        """Testa tratamento de exceção na análise."""
        self.mock_glue_handler.read_from_catalog.side_effect = Exception("Read error")
        
        with self.assertRaises(Exception):
            self.analyzer.analisar_vendas("test_db", "test_table", "2024-01")
    
    def test_analisar_multiplos_periodos(self):
        """Testa análise de múltiplos períodos."""
        mock_df = MagicMock()
        mock_df.count.return_value = 10
        mock_df.columns = ['id', 'valor']
        self.mock_glue_handler.read_from_catalog.return_value = mock_df
        
        self.mock_dynamodb_handler.save_congregado.return_value = {'id': 'test_id'}
        
        periodos = ["2024-01", "2024-02"]
        
        results = self.analyzer.analisar_multiplos_periodos(
            database="test_db",
            table_name="test_table",
            periodos=periodos,
            output_base_path="s3://bucket/output"
        )
        
        self.assertEqual(len(results), 2)
        self.assertIn("2024-01", results)
        self.assertIn("2024-02", results)
        self.assertEqual(self.mock_glue_handler.read_from_catalog.call_count, 2)

if __name__ == '__main__':
    unittest.main()
