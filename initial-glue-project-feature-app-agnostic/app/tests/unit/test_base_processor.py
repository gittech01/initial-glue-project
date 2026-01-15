"""Testes unitários para utils/business/base_processor.py"""
import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from utils.business.base_processor import BaseBusinessProcessor
from utils.config.settings import AppConfig


class ConcreteProcessor(BaseBusinessProcessor):
    """Implementação concreta para testes."""
    
    def _read_data(self, **kwargs):
        return MagicMock()
    
    def _transform_data(self, df, **kwargs):
        return {'test': 'data'}
    
    def _get_congregado_key(self, **kwargs):
        return 'test_key'


class TestBaseBusinessProcessor(unittest.TestCase):
    """Testes para BaseBusinessProcessor."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        self.config = AppConfig()
        
        self.processor = ConcreteProcessor(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.config
        )
    
    def test_init(self):
        """Testa inicialização."""
        self.assertEqual(self.processor.glue_handler, self.mock_glue_handler)
        self.assertEqual(self.processor.journey_controller, self.mock_journey_controller)
        self.assertEqual(self.processor.dynamodb_handler, self.mock_dynamodb_handler)
        self.assertEqual(self.processor.config, self.config)
    
    def test_process_template_method(self):
        """Testa que o template method funciona corretamente."""
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        self.processor._read_data = MagicMock(return_value=mock_df)
        
        self.mock_dynamodb_handler.save_congregado.return_value = {
            'id': 'test_id',
            'status': 'created'
        }
        
        result = self.processor.process(test_param='value')
        
        self.assertEqual(result['status'], 'success')
        self.assertEqual(result['record_count'], 100)
        self.mock_dynamodb_handler.save_congregado.assert_called_once()
    
    def test_get_processor_name(self):
        """Testa retorno do nome do processador."""
        name = self.processor.get_processor_name()
        self.assertEqual(name, 'ConcreteProcessor')
    
    def test_should_write_output_default(self):
        """Testa comportamento padrão de _should_write_output."""
        self.assertTrue(self.processor._should_write_output())


if __name__ == '__main__':
    unittest.main()
