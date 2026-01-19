"""Testes unitários para utils/business/orchestrator.py"""
import unittest
import os
from unittest.mock import MagicMock
from utils.business.orchestrator import BusinessRuleOrchestrator, ExecutionResult
from utils.business.base_processor import BaseBusinessProcessor
from utils.config.settings import AppConfig

# Configurar região AWS para testes
os.environ['AWS_DEFAULT_REGION'] = 'sa-east-1'


class TestProcessor(BaseBusinessProcessor):
    """Processador de teste."""
    
    def _read_data(self, **kwargs):
        return MagicMock()
    
    def _transform_data(self, df, **kwargs):
        return {'test': 'data'}
    
    def _get_congregado_key(self, **kwargs):
        return 'test_key'


class TestBusinessRuleOrchestrator(unittest.TestCase):
    """Testes para BusinessRuleOrchestrator."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_journey_controller = MagicMock()
        self.orchestrator = BusinessRuleOrchestrator(
            journey_controller=self.mock_journey_controller,
            continue_on_error=True
        )
        
        self.processor = TestProcessor(
            glue_handler=MagicMock(),
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=MagicMock(),
            config=AppConfig()
        )
    
    def test_init(self):
        """Testa inicialização."""
        self.assertEqual(self.orchestrator.journey_controller, self.mock_journey_controller)
        self.assertTrue(self.orchestrator.continue_on_error)
        self.assertEqual(self.orchestrator.consecutive_failures, 0)
    
    def test_execute_rule_success(self):
        """Testa execução bem-sucedida."""
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'SUCCESS'}
        
        result = self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='test_key',
            test_param='value'
        )
        
        self.assertEqual(result['status'].upper(), ExecutionResult.SUCCESS.value.upper())
        self.assertEqual(self.orchestrator.consecutive_failures, 0)
    
    def test_execute_rule_failure_continue_on_error(self):
        """Testa falha com continue_on_error=True."""
        self.mock_journey_controller.execute_with_journey.side_effect = Exception("Test error")
        
        result = self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='test_key'
        )
        
        self.assertEqual(result['status'].upper(), ExecutionResult.FAILED.value.upper())
        self.assertIn('error', result)
        self.assertEqual(self.orchestrator.consecutive_failures, 1)
    
    def test_execute_rule_failure_stop_on_error(self):
        """Testa falha com continue_on_error=False."""
        orchestrator = BusinessRuleOrchestrator(
            journey_controller=self.mock_journey_controller,
            continue_on_error=False
        )
        
        self.mock_journey_controller.execute_with_journey.side_effect = Exception("Test error")
        
        with self.assertRaises(Exception):
            orchestrator.execute_rule(
                processor=self.processor,
                idempotency_key='test_key'
            )
    
    def test_execute_multiple_rules(self):
        """Testa execução de múltiplas regras."""
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'success'}
        
        rules = [
            {
                'processor': self.processor,
                'idempotency_key': 'key1',
                'test_param': 'value1'
            },
            {
                'processor': self.processor,
                'idempotency_key': 'key2',
                'test_param': 'value2'
            }
        ]
        
        result = self.orchestrator.execute_multiple_rules(rules)
        
        self.assertEqual(result['total'], 2)
        self.assertEqual(result['successful'], 2)
        self.assertEqual(result['failed'], 0)
    
    def test_execute_multiple_rules_with_failures(self):
        """Testa execução de múltiplas regras com algumas falhas."""
        # Ambas execuções falham
        self.mock_journey_controller.execute_with_journey.side_effect = [
            Exception("Error 1"),
            Exception("Error 2")
        ]
        
        rules = [
            {
                'processor': self.processor,
                'idempotency_key': 'key1'
            },
            {
                'processor': self.processor,
                'idempotency_key': 'key2'
            }
        ]
        
        result = self.orchestrator.execute_multiple_rules(rules)
        
        self.assertEqual(result['total'], 2)
        self.assertEqual(result['successful'], 0)
        self.assertEqual(result['failed'], 2)
    
    def test_reset_failure_counter(self):
        """Testa reset do contador de falhas."""
        self.orchestrator.consecutive_failures = 5
        self.orchestrator.reset_failure_counter()
        self.assertEqual(self.orchestrator.consecutive_failures, 0)


if __name__ == '__main__':
    unittest.main()
