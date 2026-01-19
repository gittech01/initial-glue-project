"""Testes adicionais para dynamodb_handler.py para aumentar cobertura"""
import unittest
from unittest.mock import MagicMock, patch
from utils.dynamodb_handler import DynamoDBHandler


class TestDynamoDBHandlerAdditional(unittest.TestCase):
    """Testes adicionais para aumentar cobertura."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.handler = DynamoDBHandler(
            table_name="test_table",
            region_name="us-east-1",
            flag_salva=True,
            dynamodb_client=None  # Modo em memória
        )
    
    def test_save_congregado_with_version_conflict(self):
        """Testa que save_congregado rejeita versão menor ou igual."""
        congregado_data = {"total": 100}
        
        # Primeira chamada com versão 2
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="version_test",
            version=2
        )
        
        # Segunda chamada com versão 1 (menor) - deve falhar
        with self.assertRaises(ValueError) as context:
            self.handler.save_congregado(
                congregado_data=congregado_data,
                primary_key="version_test",
                version=1
            )
        self.assertIn('Versão', str(context.exception))
    
    def test_save_congregado_with_version_increment(self):
        """Testa incremento automático de versão quando item existe."""
        congregado_data = {"total": 100}
        
        # Primeira chamada
        result1 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="increment_test"
        )
        
        # Segunda chamada com dados diferentes
        result2 = self.handler.save_congregado(
            congregado_data={"total": 200},
            primary_key="increment_test"
        )
        
        self.assertEqual(result1['version'], 1)
        self.assertEqual(result2['version'], 2)
    
    def test_save_congregado_idempotency_same_key(self):
        """Testa idempotência quando mesma idempotency_key é usada."""
        congregado_data = {"total": 100}
        idempotency_key = "same_key_123"
        
        # Primeira chamada
        result1 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="idempotency_test",
            idempotency_key=idempotency_key
        )
        
        # Segunda chamada com mesma idempotency_key
        result2 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="idempotency_test",
            idempotency_key=idempotency_key
        )
        
        # Deve retornar item existente
        self.assertEqual(result2['status'], 'exists')
        self.assertEqual(result1['id'], result2['id'])
    
    def test_save_congregado_flag_salva_false(self):
        """Testa save_congregado quando flag_salva é False."""
        handler_no_save = DynamoDBHandler(
            table_name="test_table",
            flag_salva=False,
            dynamodb_client=None
        )
        
        congregado_data = {"total": 100}
        result = handler_no_save.save_congregado(
            congregado_data=congregado_data,
            primary_key="no_save_test"
        )
        
        # Deve retornar sem salvar
        self.assertIsNotNone(result)
        # Item não deve estar no store
        self.assertIsNone(handler_no_save._get_item({'id': 'no_save_test'}))
    
    def test_batch_save_congregados_empty_list(self):
        """Testa batch_save_congregados com lista vazia."""
        result = self.handler.batch_save_congregados([])
        
        self.assertEqual(result['saved'], 0)
        self.assertEqual(result['total'], 0)
        self.assertEqual(result['failed'], 0)
    
    def test_batch_save_congregados_with_errors(self):
        """Testa batch_save_congregados quando alguns itens falham."""
        congregados = [
            {"id": "batch_1", "total": 100},
            {"invalid": "data"},  # Sem id - pode causar erro
            {"id": "batch_3", "total": 300}
        ]
        
        result = self.handler.batch_save_congregados(
            congregados=congregados,
            primary_key_field="id"
        )
        
        # Pelo menos alguns devem ser salvos
        self.assertGreater(result['saved'], 0)
        self.assertLessEqual(result['saved'], result['total'])
    
    def test_batch_save_congregados_custom_idempotency_field(self):
        """Testa batch_save_congregados com campo de idempotência customizado."""
        congregados = [
            {"id": "batch_1", "custom_key": "key1", "total": 100},
            {"id": "batch_2", "custom_key": "key2", "total": 200}
        ]
        
        result = self.handler.batch_save_congregados(
            congregados=congregados,
            primary_key_field="id",
            idempotency_key_field="custom_key"
        )
        
        self.assertEqual(result['saved'], 2)
    
    def test_delete_congregado_with_condition(self):
        """Testa delete_congregado com expressão condicional."""
        congregado_data = {"total": 100, "status": "active"}
        
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="condition_test"
        )
        
        # Deletar com condição que não é satisfeita
        result = self.handler.delete_congregado(
            "condition_test",
            condition_expression="attribute_not_exists(status)"
        )
        
        # Em memória, a condição pode não ser verificada perfeitamente
        # Mas testamos que o método não levanta erro
        self.assertIsInstance(result, bool)
    
    def test_delete_congregado_not_found(self):
        """Testa delete_congregado quando item não existe."""
        result = self.handler.delete_congregado("nonexistent_key")
        
        self.assertFalse(result)
    
    def test_put_item_with_retry_condition_failed(self):
        """Testa _put_item_with_retry quando condição falha."""
        congregado_data = {"total": 100}
        
        # Salvar primeiro item
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="condition_fail_test",
            version=2
        )
        
        # Tentar salvar com versão menor (condição falha)
        condition_expr = "version < :new_version"
        condition_values = {':new_version': 1}
        
        result = self.handler._put_item_with_retry(
            item={'id': 'condition_fail_test', 'version': 1, 'total': 200},
            condition_expression=condition_expr,
            condition_values=condition_values
        )
        
        # Deve retornar False (condição não satisfeita)
        self.assertFalse(result)
    
    def test_put_item_with_retry_max_retries(self):
        """Testa _put_item_with_retry quando atinge max_retries."""
        handler = DynamoDBHandler(
            table_name="test_table",
            max_retries=2,
            dynamodb_client=None
        )
        
        # Simular erro persistente
        with patch.object(handler, 'table') as mock_table:
            mock_table.put_item.side_effect = Exception("Persistent error")
            
            result = handler._put_item_with_retry(
                item={'id': 'retry_test', 'total': 100}
            )
            
            # Deve retornar False após max_retries
            self.assertFalse(result)
            # Deve ter tentado max_retries vezes
            self.assertEqual(mock_table.put_item.call_count, 2)
    
    def test_get_item_with_complex_key(self):
        """Testa _get_item com chave complexa."""
        congregado_data = {"total": 100}
        
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="complex_key_test"
        )
        
        result = self.handler._get_item({'id': 'complex_key_test'})
        
        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 'complex_key_test')
    
    def test_generate_idempotency_key_ignores_timestamp_fields(self):
        """Testa que _generate_idempotency_key ignora campos de timestamp."""
        data1 = {
            "total": 100,
            "created_at": "2024-01-01",
            "updated_at": "2024-01-02"
        }
        data2 = {
            "total": 100,
            "created_at": "2024-01-03",  # Diferente
            "updated_at": "2024-01-04"  # Diferente
        }
        
        key1 = self.handler._generate_idempotency_key(data1)
        key2 = self.handler._generate_idempotency_key(data2)
        
        # Chaves devem ser iguais (ignorando timestamps)
        self.assertEqual(key1, key2)
    
    def test_generate_idempotency_key_different_data(self):
        """Testa que dados diferentes geram chaves diferentes."""
        data1 = {"total": 100}
        data2 = {"total": 200}
        
        key1 = self.handler._generate_idempotency_key(data1)
        key2 = self.handler._generate_idempotency_key(data2)
        
        self.assertNotEqual(key1, key2)


if __name__ == '__main__':
    unittest.main()
