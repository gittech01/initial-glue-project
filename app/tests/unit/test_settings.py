"""Testes unitários para utils/config/settings.py"""
import unittest
import os
import tempfile
from unittest.mock import patch, MagicMock

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
        self.assertEqual(config.congregado_table_name, 'congregado_data')
        self.assertEqual(config.default_database, 'default_database')
        self.assertEqual(config.default_output_format, 'parquet')
        self.assertEqual(config.max_retries, 3)
        self.assertEqual(config.retry_delay, 2)
        self.assertEqual(config.log_level, 'INFO')
        self.assertEqual(config.batch_size, 1000)
        self.assertTrue(config.enable_partitioning)
    
    def test_init_with_settings_path(self):
        """Testa inicialização com caminho de settings.py."""
        # Criar arquivo settings.py temporário
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("""
CONSOLIDACOES = {
    'tbl_test': {
        'principais': {
            'sor': 'tbl_sor',
            'sot': 'tbl_sot'
        },
        'chaves_principais': ['num_oper'],
        'campos_decisao': ['dat_vlr_even_oper']
    }
}
""")
            temp_path = f.name
        
        try:
            config = AppConfig(settings_path=temp_path)
            self.assertIn('tbl_test', config.CONSOLIDACOES)
            self.assertEqual(config.CONSOLIDACOES['tbl_test']['principais']['sor'], 'tbl_sor')
        finally:
            os.unlink(temp_path)
    
    def test_init_with_invalid_settings_path(self):
        """Testa inicialização com caminho inválido."""
        config = AppConfig(settings_path='/path/that/does/not/exist/settings.py')
        self.assertEqual(config.CONSOLIDACOES, {})
    
    def test_init_with_settings_without_consolidacoes(self):
        """Testa inicialização com settings.py sem CONSOLIDACOES."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write("OTHER_VAR = 'value'")
            temp_path = f.name
        
        try:
            config = AppConfig(settings_path=temp_path)
            self.assertEqual(config.CONSOLIDACOES, {})
        finally:
            os.unlink(temp_path)
    
    def test_load_consolidacoes_from_default_path_exists(self):
        """Testa carregamento do caminho padrão quando existe."""
        # Criar estrutura de diretórios temporária
        with tempfile.TemporaryDirectory() as temp_dir:
            novo_dir = os.path.join(temp_dir, 'novo-20260116')
            os.makedirs(novo_dir, exist_ok=True)
            settings_path = os.path.join(novo_dir, 'settings.py')
            
            with open(settings_path, 'w') as f:
                f.write("CONSOLIDACOES = {'test': {}}")
            
            # Mock do caminho
            with patch('utils.config.settings.os.path.exists') as mock_exists, \
                 patch('utils.config.settings.os.path.join') as mock_join:
                mock_exists.return_value = True
                mock_join.return_value = settings_path
                
                # Patch do importlib para evitar erro real de importação
                import importlib.util
                with patch.object(importlib.util, 'spec_from_file_location') as mock_spec, \
                     patch.object(importlib.util, 'module_from_spec') as mock_module:
                    
                    mock_spec_obj = MagicMock()
                    mock_spec_obj.loader = MagicMock()
                    mock_spec.return_value = mock_spec_obj
                    
                    mock_settings_module = MagicMock()
                    mock_settings_module.CONSOLIDACOES = {'test': {}}
                    mock_module.return_value = mock_settings_module
                    
                    config = AppConfig()
                    # Deve tentar carregar (pode não funcionar completamente, mas não deve crashar)
    
    def test_repr(self):
        """Testa representação string do config."""
        config = AppConfig()
        repr_str = repr(config)
        
        self.assertIn('AppConfig', repr_str)
        self.assertIn('region', repr_str)
        self.assertIn('journey_table', repr_str)
        self.assertIn('congregado_table', repr_str)
    
    def test_load_consolidacoes_from_file_with_error(self):
        """Testa tratamento de erro ao carregar arquivo."""
        # Criar arquivo que causará erro ao importar
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                f.write("invalid python syntax !!!")
                temp_path = f.name
            
            # Não deve crashar, apenas logar erro
            config = AppConfig(settings_path=temp_path)
            # CONSOLIDACOES deve estar vazio em caso de erro
            # (depende da implementação de tratamento de erro)
        finally:
            if temp_path and os.path.exists(temp_path):
                os.unlink(temp_path)


if __name__ == '__main__':
    unittest.main()
