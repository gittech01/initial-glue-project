"""Testes adicionais para journey_controller.py para aumentar cobertura"""
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from utils.journey_controller import JourneyController, JourneyStatus, journey_controlled


class TestJourneyControllerAdditional(unittest.TestCase):
    """Testes adicionais para aumentar cobertura."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.controller = JourneyController(
            table_name="test_journey",
            region_name="us-east-1",
            dynamodb_client=None  # Modo em memória
        )
    
    def test_add_step(self):
        """Testa adição de etapa à jornada."""
        journey_id = self.controller.start_journey()
        
        self.controller.add_step(journey_id, "step1", {"data": "test"})
        
        journey = self.controller.get_journey(journey_id)
        self.assertIsNotNone(journey)
        self.assertIn('steps', journey)
        self.assertEqual(len(journey['steps']), 1)
        self.assertEqual(journey['steps'][0]['name'], 'step1')
    
    def test_add_step_journey_not_found(self):
        """Testa que add_step levanta erro se jornada não existir."""
        with self.assertRaises(ValueError) as context:
            self.controller.add_step("nonexistent", "step1")
        self.assertIn('não encontrada', str(context.exception))
    
    def test_add_step_multiple_steps(self):
        """Testa adição de múltiplas etapas."""
        journey_id = self.controller.start_journey()
        
        self.controller.add_step(journey_id, "step1")
        self.controller.add_step(journey_id, "step2", {"value": 123})
        self.controller.add_step(journey_id, "step3")
        
        journey = self.controller.get_journey(journey_id)
        self.assertEqual(len(journey['steps']), 3)
    
    def test_cleanup_old_journeys(self):
        """Testa limpeza de jornadas antigas."""
        # Criar jornada antiga
        old_date = (datetime.utcnow() - timedelta(days=60)).isoformat()
        journey_id = self.controller.start_journey()
        journey = self.controller.get_journey(journey_id)
        journey['created_at'] = old_date
        self.controller._in_memory_store[journey_id] = journey
        
        # Criar jornada recente
        recent_journey_id = self.controller.start_journey()
        
        # Limpar jornadas com mais de 30 dias
        self.controller.cleanup_old_journeys(days_old=30)
        
        # Jornada antiga deve ser removida
        self.assertIsNone(self.controller.get_journey(journey_id))
        # Jornada recente deve permanecer
        self.assertIsNotNone(self.controller.get_journey(recent_journey_id))
    
    def test_cleanup_old_journeys_no_old(self):
        """Testa limpeza quando não há jornadas antigas."""
        journey_id = self.controller.start_journey()
        
        self.controller.cleanup_old_journeys(days_old=30)
        
        # Jornada deve permanecer
        self.assertIsNotNone(self.controller.get_journey(journey_id))
    
    def test_execute_with_journey_with_metadata(self):
        """Testa execute_with_journey com metadata."""
        def test_func(x, y):
            return x + y
        
        result = self.controller.execute_with_journey(
            test_func,
            metadata={"operation": "sum"},
            *[10, 20]
        )
        
        self.assertEqual(result, 30)
        journey = self.controller.get_journey(self.controller._last_journey_id)
        self.assertEqual(journey['metadata']['operation'], 'sum')
    
    def test_execute_with_journey_with_kwargs(self):
        """Testa execute_with_journey com kwargs."""
        def test_func(a, b, c=0):
            return a + b + c
        
        result = self.controller.execute_with_journey(
            test_func,
            a=10,
            b=20,
            c=5
        )
        
        self.assertEqual(result, 35)
    
    def test_execute_with_journey_existing_completed_retry(self):
        """Testa execute_with_journey quando jornada já está completada e permite retry."""
        def test_func():
            return "result"
        
        # Primeira execução
        journey_id = self.controller.start_journey(idempotency_key="retry_key")
        self.controller.update_status(journey_id, JourneyStatus.COMPLETED)
        
        # Segunda execução com mesmo idempotency_key
        result = self.controller.execute_with_journey(
            test_func,
            journey_id=journey_id,
            idempotency_key="retry_key"
        )
        
        # Deve retornar resultado da jornada existente
        self.assertEqual(result, "result")
    
    def test_update_item_with_expression_names(self):
        """Testa _update_item com expression_names."""
        journey_id = self.controller.start_journey()
        
        update_expr = "SET #status = :status"
        expr_values = {':status': JourneyStatus.COMPLETED.value}
        expr_names = {'#status': 'status'}
        
        self.controller._update_item(journey_id, update_expr, expr_values, expr_names)
        
        journey = self.controller.get_journey(journey_id)
        self.assertEqual(journey['status'], JourneyStatus.COMPLETED.value)
    
    def test_update_item_in_memory_add_expression(self):
        """Testa _update_item em memória com expressão ADD."""
        journey_id = self.controller.start_journey()
        
        update_expr = "ADD retry_count :inc"
        expr_values = {':inc': 1}
        
        self.controller._update_item(journey_id, update_expr, expr_values)
        
        journey = self.controller.get_journey(journey_id)
        self.assertEqual(journey.get('retry_count', 0), 1)


class TestJourneyControlledDecoratorAdditional(unittest.TestCase):
    """Testes adicionais para o decorator journey_controlled."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.controller = JourneyController(
            table_name="test_journey",
            dynamodb_client=None
        )
    
    def test_decorator_success(self):
        """Testa decorator com execução bem-sucedida."""
        @journey_controlled(idempotency_key="decorator_test")
        def test_func(x, y):
            return x * y
        
        result = test_func(5, 6, journey_controller=self.controller)
        
        self.assertEqual(result, 30)
    
    def test_decorator_with_metadata(self):
        """Testa decorator com metadata."""
        @journey_controlled(
            idempotency_key="decorator_metadata",
            metadata={"test": "data"}
        )
        def test_func():
            return "success"
        
        result = test_func(journey_controller=self.controller)
        
        self.assertEqual(result, "success")
        journey = self.controller.get_journey(self.controller._last_journey_id)
        self.assertEqual(journey['metadata']['test'], 'data')
    
    def test_decorator_with_journey_id(self):
        """Testa decorator com journey_id específico."""
        @journey_controlled(journey_id="custom_journey_id")
        def test_func():
            return "result"
        
        result = test_func(journey_controller=self.controller)
        
        self.assertEqual(result, "result")
        journey = self.controller.get_journey("custom_journey_id")
        self.assertIsNotNone(journey)


if __name__ == '__main__':
    unittest.main()
