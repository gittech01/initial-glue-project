"""Testes unitários para utils/business/processor_factory.py"""
import unittest
import os
from unittest.mock import MagicMock
from utils.business.processor_factory import ProcessorFactory
from utils.business.base_processor import BaseBusinessProcessor
from utils.config.settings import AppConfig

# Configurar região AWS para testes
os.environ['AWS_DEFAULT_REGION'] = 'sa-east-1'


class TestProcessor(BaseBusinessProcessor):
    """Processador de teste."""
    
    def _read_data(self, **kwargs):
        return MagicMock()
    
    def _transform_data(self, df, **kwargs):
        return {}
    
    def _get_congregado_key(self, **kwargs):
        return 'test'


class TestProcessorFactory(unittest.TestCase):
    """Testes para ProcessorFactory."""
    
    def setUp(self):
        """Setup para cada teste."""
        # Limpar registry antes de cada teste
        ProcessorFactory._processors.clear()
    
    def test_register(self):
        """Testa registro de processador."""
        ProcessorFactory.register('test_processor', TestProcessor)
        self.assertTrue(ProcessorFactory.is_registered('test_processor'))
    
    def test_register_invalid_class(self):
        """Testa erro ao registrar classe inválida."""
        with self.assertRaises(ValueError):
            ProcessorFactory.register('invalid', str)
    
    def test_create(self):
        """Testa criação de processador."""
        ProcessorFactory.register('test_processor', TestProcessor)
        
        mock_glue = MagicMock()
        mock_journey = MagicMock()
        mock_dynamodb = MagicMock()
        config = AppConfig()
        
        processor = ProcessorFactory.create(
            processor_type='test_processor',
            glue_handler=mock_glue,
            journey_controller=mock_journey,
            dynamodb_handler=mock_dynamodb,
            config=config
        )
        
        self.assertIsInstance(processor, TestProcessor)
        self.assertEqual(processor.glue_handler, mock_glue)
    
    def test_create_not_registered(self):
        """Testa erro ao criar processador não registrado."""
        with self.assertRaises(ValueError) as context:
            ProcessorFactory.create(
                processor_type='nonexistent',
                glue_handler=MagicMock(),
                journey_controller=MagicMock(),
                dynamodb_handler=MagicMock(),
                config=AppConfig()
            )
        
        self.assertIn('não encontrado', str(context.exception))
    
    def test_list_available(self):
        """Testa listagem de processadores disponíveis."""
        ProcessorFactory.register('processor1', TestProcessor)
        ProcessorFactory.register('processor2', TestProcessor)
        
        available = ProcessorFactory.list_available()
        self.assertIn('processor1', available)
        self.assertIn('processor2', available)
    
    def test_is_registered(self):
        """Testa verificação de registro."""
        ProcessorFactory.register('test', TestProcessor)
        self.assertTrue(ProcessorFactory.is_registered('test'))
        self.assertFalse(ProcessorFactory.is_registered('nonexistent'))


if __name__ == '__main__':
    unittest.main()
