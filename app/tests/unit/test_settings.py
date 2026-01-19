"""Testes unitários para utils/config/settings.py"""
import unittest
from utils.config.settings import AppConfig


class TestAppConfig(unittest.TestCase):
    """Testes para AppConfig."""
    
    def setUp(self):
        """Setup para cada teste."""
        pass
    
    def test_init_default_values(self):
        """Testa inicialização com valores padrão."""
        config = AppConfig()
        
        self.assertEqual(config.aws_region, 'sa-east-1')
        self.assertEqual(config.journey_table_name, 'journey_control')
        # congregado_table_name foi removido - dados são salvos no S3/Glue Catalog
        self.assertEqual(config.default_database_output, 'default_database')
        self.assertEqual(config.default_output_format, 'parquet')
        self.assertEqual(config.max_retries, 3)
        self.assertEqual(config.retry_delay, 2)
        self.assertEqual(config.log_level, 'INFO')
        self.assertEqual(config.batch_size, 1000)
        self.assertTrue(config.enable_partitioning)
    

  
    def test_repr(self):
        """Testa representação string do config."""
        config = AppConfig()
        repr_str = repr(config)
        
        # Verificar que é uma representação válida do objeto
        self.assertIn('AppConfig', repr_str)
        # Verificar que os atributos existem
        self.assertIsNotNone(config.aws_region)
        self.assertIsNotNone(config.journey_table_name)
        # congregado_table_name foi removido - dados são salvos no S3/Glue Catalog



if __name__ == '__main__':
    unittest.main()
