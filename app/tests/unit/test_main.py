"""Testes unitários para src/main.py"""
import unittest
from unittest.mock import MagicMock, patch, Mock
import sys

class TestMain(unittest.TestCase):
    """Testes para o entry point da aplicação."""
    
    @patch('src.main.initialize_glue_context')
    @patch('src.main.AppConfig')
    @patch('src.main.GlueDataHandler')
    @patch('src.main.JourneyController')
    @patch('src.main.DynamoDBHandler')
    @patch('src.main.ProcessorFactory')
    @patch('src.main.BusinessRuleOrchestrator')
    def test_main_success(
        self,
        mock_orchestrator_class,
        mock_factory_class,
        mock_dynamodb_class,
        mock_journey_class,
        mock_glue_handler_class,
        mock_config_class,
        mock_init_glue
    ):
        """Testa execução bem-sucedida do main."""
        # Setup mocks
        mock_sc = MagicMock()
        mock_glue_context = MagicMock()
        mock_job = MagicMock()
        mock_args = {
            'JOB_NAME': 'test_job',
            'database': 'test_db',
            'tabela_consolidada': None  # None executa todas as consolidações
        }
        mock_init_glue.return_value = (mock_sc, mock_glue_context, mock_job, mock_args)
        
        mock_config = MagicMock()
        mock_config.journey_table_name = 'journey_table'
        mock_config.congregado_table_name = 'congregado_table'
        mock_config.aws_region = 'us-east-1'
        mock_config.default_output_format = 'parquet'
        # Mock CONSOLIDACOES para permitir execução
        mock_config.CONSOLIDACOES = {
            'tbl_test': {
                'principais': {'sor': 'tbl_sor', 'sot': 'tbl_sot'},
                'chaves_principais': ['key1'],
                'campos_decisao': ['field1']
            }
        }
        mock_config_class.return_value = mock_config
        
        mock_glue_handler = MagicMock()
        mock_glue_handler_class.return_value = mock_glue_handler
        
        mock_journey_controller = MagicMock()
        mock_journey_class.return_value = mock_journey_controller
        
        mock_dynamodb_handler = MagicMock()
        mock_dynamodb_class.return_value = mock_dynamodb_handler
        
        # Mock ProcessorFactory
        mock_processor = MagicMock()
        mock_processor.get_processor_name.return_value = 'DataProcessor'
        mock_factory_class.create.return_value = mock_processor
        
        # Mock Orchestrator
        mock_orchestrator = MagicMock()
        mock_orchestrator.execute_rule.return_value = {
            'status': 'success',
            'result': {'status': 'success', 'record_count': 2}
        }
        mock_orchestrator_class.return_value = mock_orchestrator
        
        # Import e execução
        from src.main import main
        
        result = main()
        
        # Assertions
        mock_init_glue.assert_called_once()
        mock_config_class.assert_called_once()
        mock_glue_handler_class.assert_called_once_with(mock_glue_context)
        mock_journey_class.assert_called_once()
        mock_dynamodb_class.assert_called_once()
        mock_factory_class.create.assert_called_once()
        # execute_rule é chamado para cada consolidação configurada
        self.assertGreaterEqual(mock_orchestrator.execute_rule.call_count, 1)
        mock_job.commit.assert_called_once()
        self.assertIn(result['status'], ['success', 'partial_success'])
    
    @patch('src.main.initialize_glue_context')
    def test_main_exception(self, mock_init_glue):
        """Testa tratamento de exceção no main."""
        mock_init_glue.side_effect = Exception("Test error")
        
        from src.main import main
        
        with self.assertRaises(Exception):
            main()
    
    @patch('src.main.SparkContext')
    @patch('src.main.GlueContext')
    @patch('src.main.Job')
    @patch('src.main.getResolvedOptions')
    def test_initialize_glue_context_success(
        self,
        mock_get_resolved,
        mock_job_class,
        mock_glue_class,
        mock_spark_class
    ):
        """Testa inicialização bem-sucedida do contexto Glue."""
        mock_get_resolved.return_value = {
            'JOB_NAME': 'test_job',
            'database': 'test_db',
            'tabela_consolidada': None  # None executa todas as consolidações
        }
        
        mock_sc = MagicMock()
        mock_spark_class.return_value = mock_sc
        
        mock_glue_context = MagicMock()
        mock_glue_class.return_value = mock_glue_context
        
        mock_job = MagicMock()
        mock_job_class.return_value = mock_job
        
        from src.main import initialize_glue_context
        
        sc, glue_context, job, args = initialize_glue_context()
        
        self.assertEqual(sc, mock_sc)
        self.assertEqual(glue_context, mock_glue_context)
        self.assertEqual(job, mock_job)
        self.assertEqual(args['JOB_NAME'], 'test_job')
        mock_job.init.assert_called_once()
    
    @patch('src.main.SparkContext')
    def test_initialize_glue_context_exception(self, mock_spark_class):
        """Testa tratamento de exceção na inicialização."""
        mock_spark_class.side_effect = Exception("Init error")
        
        from src.main import initialize_glue_context
        
        with self.assertRaises(Exception):
            initialize_glue_context()

if __name__ == '__main__':
    unittest.main()
