"""Testes unitários para utils/config/settings.py"""
import unittest
import os
from unittest.mock import patch

class TestAppConfig(unittest.TestCase):
    """Testes para AppConfig."""
    
    def setUp(self):
        """Setup para cada teste."""
        # Limpar variáveis de ambiente antes de cada teste
        env_vars = [
            'AWS_REGION', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
            'JOURNEY_TABLE_NAME', 'CONGREGADO_TABLE_NAME', 'GLUE_DATABASE',
            'OUTPUT_FORMAT', 'MAX_RETRIES', 'RETRY_DELAY', 'LOG_LEVEL',
            'BATCH_SIZE', 'ENABLE_PARTITIONING'
        ]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]
    
    def test_default_values(self):
        """Testa valores padrão quando variáveis de ambiente não estão definidas."""
        from utils.config.settings import AppConfig
        
        config = AppConfig()
        
        self.assertEqual(config.aws_region, 'us-east-1')
        self.assertIsNone(config.aws_access_key_id)
        self.assertIsNone(config.aws_secret_access_key)
        self.assertEqual(config.journey_table_name, 'journey_control')
        self.assertEqual(config.congregado_table_name, 'congregado_data')
        self.assertEqual(config.default_database, 'default_database')
        self.assertEqual(config.default_output_format, 'parquet')
        self.assertEqual(config.max_retries, 3)
        self.assertEqual(config.retry_delay, 2)
        self.assertEqual(config.log_level, 'INFO')
        self.assertEqual(config.batch_size, 1000)
        self.assertTrue(config.enable_partitioning)
    
    def test_custom_values_from_env(self):
        """Testa valores customizados a partir de variáveis de ambiente."""
        os.environ['AWS_REGION'] = 'us-west-2'
        os.environ['JOURNEY_TABLE_NAME'] = 'custom_journey'
        os.environ['CONGREGADO_TABLE_NAME'] = 'custom_congregado'
        os.environ['MAX_RETRIES'] = '5'
        os.environ['RETRY_DELAY'] = '10'
        os.environ['BATCH_SIZE'] = '5000'
        os.environ['ENABLE_PARTITIONING'] = 'false'
        
        from utils.config.settings import AppConfig
        
        config = AppConfig()
        
        self.assertEqual(config.aws_region, 'us-west-2')
        self.assertEqual(config.journey_table_name, 'custom_journey')
        self.assertEqual(config.congregado_table_name, 'custom_congregado')
        self.assertEqual(config.max_retries, 5)
        self.assertEqual(config.retry_delay, 10)
        self.assertEqual(config.batch_size, 5000)
        self.assertFalse(config.enable_partitioning)
    
    def test_repr(self):
        """Testa representação string da configuração."""
        from utils.config.settings import AppConfig
        
        config = AppConfig()
        repr_str = repr(config)
        
        self.assertIn('AppConfig', repr_str)
        self.assertIn('region', repr_str)
        self.assertIn('journey_table', repr_str)
        self.assertIn('congregado_table', repr_str)

if __name__ == '__main__':
    unittest.main()
