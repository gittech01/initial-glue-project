"""Testes adicionais para orchestrator.py para aumentar cobertura"""
import unittest
from unittest.mock import MagicMock
from utils.core.orchestrator import BusinessRuleOrchestrator, ExecutionResult
from utils.core.base_processor import BaseBusinessProcessor
from utils.config.settings import AppConfig
import os

os.environ['AWS_DEFAULT_REGION'] = 'sa-east-1'


class TestProcessor(BaseBusinessProcessor):
    """Processador de teste."""
    
    def _read_data(self, **kwargs):
        return MagicMock()
    
    def _transform_data(self, df, **kwargs):
        return {'test': 'data'}
    
    def _get_congregado_key(self, **kwargs):
        return 'test_key'


class TestBusinessRuleOrchestratorAdditional(unittest.TestCase):
    """Testes adicionais para aumentar cobertura."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_journey_controller = MagicMock()
        self.orchestrator = BusinessRuleOrchestrator(
            journey_controller=self.mock_journey_controller,
            continue_on_error=True,
            max_concurrent_failures=3
        )
        
        self.processor = TestProcessor(
            glue_handler=MagicMock(),
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=MagicMock(),
            config=AppConfig()
        )
    
    def test_execute_rule_max_concurrent_failures(self):
        """Testa que execução para após max_concurrent_failures."""
        self.mock_journey_controller.execute_with_journey.side_effect = Exception("Error")
        
        # Primeira falha
        result1 = self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='key1'
        )
        self.assertEqual(self.orchestrator.consecutive_failures, 1)
        
        # Segunda falha
        result2 = self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='key2'
        )
        self.assertEqual(self.orchestrator.consecutive_failures, 2)
        
        # Terceira falha - deve parar
        with self.assertRaises(Exception):
            self.orchestrator.execute_rule(
                processor=self.processor,
                idempotency_key='key3'
            )
    
    def test_execute_rule_with_metadata(self):
        """Testa execute_rule com metadata."""
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'success'}
        
        result = self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='test_key',
            metadata={'operation': 'test'}
        )
        
        self.assertEqual(result['status'], 'success')
        # Verificar que metadata foi passado
        call_args = self.mock_journey_controller.execute_with_journey.call_args
        self.assertIn('metadata', call_args.kwargs)
        self.assertEqual(call_args.kwargs['metadata']['operation'], 'test')
    
    def test_execute_rule_with_kwargs(self):
        """Testa execute_rule com kwargs adicionais."""
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'success'}
        
        result = self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='test_key',
            database='db_test',
            tabela_consolidada='tbl_test'
        )
        
        self.assertEqual(result['status'], 'success')
        # Verificar que kwargs foram passados
        call_args = self.mock_journey_controller.execute_with_journey.call_args
        self.assertEqual(call_args.kwargs['database'], 'db_test')
        self.assertEqual(call_args.kwargs['tabela_consolidada'], 'tbl_test')
    
    def test_execute_multiple_rules_missing_processor(self):
        """Testa execute_multiple_rules quando processor está ausente."""
        rules = [
            {
                'idempotency_key': 'key1'
                # Sem processor
            },
            {
                'processor': self.processor,
                'idempotency_key': 'key2'
            }
        ]
        
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'success'}
        
        result = self.orchestrator.execute_multiple_rules(rules)
        
        # Deve executar apenas a segunda regra
        self.assertEqual(result['total'], 2)
        self.assertEqual(result['successful'], 1)
    
    def test_execute_multiple_rules_missing_idempotency_key(self):
        """Testa execute_multiple_rules quando idempotency_key está ausente."""
        rules = [
            {
                'processor': self.processor
                # Sem idempotency_key
            },
            {
                'processor': self.processor,
                'idempotency_key': 'key2'
            }
        ]
        
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'success'}
        
        result = self.orchestrator.execute_multiple_rules(rules)
        
        # Deve executar apenas a segunda regra
        self.assertEqual(result['total'], 2)
        self.assertEqual(result['successful'], 1)
    
    def test_execute_multiple_rules_with_skipped(self):
        """Testa execute_multiple_rules quando algumas regras são skipped."""
        # Simular resultado skipped
        def side_effect(*args, **kwargs):
            result = {'status': 'skipped'}
            return result
        
        self.mock_journey_controller.execute_with_journey.side_effect = side_effect
        
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
        
        self.assertEqual(result['skipped'], 2)
        self.assertEqual(result['successful'], 0)
    
    def test_execute_multiple_rules_empty_list(self):
        """Testa execute_multiple_rules com lista vazia."""
        result = self.orchestrator.execute_multiple_rules([])
        
        self.assertEqual(result['total'], 0)
        self.assertEqual(result['successful'], 0)
        self.assertEqual(result['failed'], 0)
    
    def test_reset_failure_counter(self):
        """Testa reset do contador de falhas."""
        self.mock_journey_controller.execute_with_journey.side_effect = Exception("Error")
        
        # Causar algumas falhas
        self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='key1'
        )
        self.assertEqual(self.orchestrator.consecutive_failures, 1)
        
        # Resetar
        self.orchestrator.reset_failure_counter()
        self.assertEqual(self.orchestrator.consecutive_failures, 0)
    
    def test_execute_rule_success_resets_counter(self):
        """Testa que sucesso reseta contador de falhas."""
        # Primeiro causar uma falha
        self.mock_journey_controller.execute_with_journey.side_effect = Exception("Error")
        self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='key1'
        )
        self.assertEqual(self.orchestrator.consecutive_failures, 1)
        
        # Depois sucesso
        self.mock_journey_controller.execute_with_journey.return_value = {'status': 'success'}
        self.orchestrator.execute_rule(
            processor=self.processor,
            idempotency_key='key2'
        )
        
        # Contador deve ser resetado
        self.assertEqual(self.orchestrator.consecutive_failures, 0)


if __name__ == '__main__':
    unittest.main()
