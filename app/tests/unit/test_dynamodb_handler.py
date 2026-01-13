"""Testes unitários para utils/dynamodb_handler.py"""
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from utils.dynamodb_handler import DynamoDBHandler

class TestDynamoDBHandler(unittest.TestCase):
    """Testes para DynamoDBHandler."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.table_name = "test_congregado_table"
        self.handler = DynamoDBHandler(
            table_name=self.table_name,
            region_name="us-east-1",
            max_retries=3,
            retry_delay=1,
            dynamodb_client=None  # Usa modo em memória
        )
    
    def test_init(self):
        """Testa inicialização do handler."""
        self.assertEqual(self.handler.table_name, self.table_name)
        self.assertEqual(self.handler.max_retries, 3)
        self.assertEqual(self.handler.retry_delay, 1)
        self.assertIsNotNone(self.handler._in_memory_store)
    
    def test_generate_idempotency_key(self):
        """Testa geração de chave de idempotência."""
        data1 = {"key": "value", "number": 123}
        data2 = {"key": "value", "number": 123}
        data3 = {"key": "different", "number": 123}
        
        key1 = self.handler._generate_idempotency_key(data1)
        key2 = self.handler._generate_idempotency_key(data2)
        key3 = self.handler._generate_idempotency_key(data3)
        
        # Mesmos dados devem gerar mesma chave
        self.assertEqual(key1, key2)
        # Dados diferentes devem gerar chaves diferentes
        self.assertNotEqual(key1, key3)
        # Chave deve ser string hexadecimal
        self.assertEqual(len(key1), 64)  # SHA256 produz 64 caracteres hex
    
    def test_save_congregado_new(self):
        """Testa salvamento de novo congregado."""
        congregado_data = {
            "total": 1000,
            "category": "sales"
        }
        
        result = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="test_key_1"
        )
        
        self.assertEqual(result['status'], 'created')
        self.assertEqual(result['id'], 'test_key_1')
        self.assertIn('idempotency_key', result)
        self.assertEqual(result['version'], 1)
    
    def test_save_congregado_idempotent(self):
        """Testa idempotência ao salvar congregado."""
        congregado_data = {
            "total": 1000,
            "category": "sales"
        }
        
        # Primeira chamada
        result1 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="idempotent_key"
        )
        
        # Segunda chamada com mesmos dados
        result2 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="idempotent_key"
        )
        
        # Deve retornar item existente
        self.assertEqual(result2['status'], 'exists')
        self.assertEqual(result1['id'], result2['id'])
    
    def test_save_congregado_with_version(self):
        """Testa salvamento com controle de versão."""
        congregado_data = {"total": 100}
        
        # Primeira versão
        result1 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="versioned_key",
            version=1
        )
        
        # Segunda versão (maior)
        result2 = self.handler.save_congregado(
            congregado_data={"total": 200},
            primary_key="versioned_key",
            version=2
        )
        
        self.assertEqual(result1['version'], 1)
        self.assertEqual(result2['version'], 2)
    
    def test_save_congregado_version_conflict(self):
        """Testa erro ao tentar salvar versão menor ou igual."""
        congregado_data = {"total": 100}
        
        # Primeira versão
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="version_conflict_key",
            version=5
        )
        
        # Tentar salvar versão menor (deve falhar)
        with self.assertRaises(ValueError):
            self.handler.save_congregado(
                congregado_data={"total": 200},
                primary_key="version_conflict_key",
                version=3
            )
    
    def test_save_congregado_with_metadata(self):
        """Testa salvamento com metadados."""
        congregado_data = {"total": 500}
        metadata = {"source": "glue_job", "timestamp": "2024-01-01"}
        
        result = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="metadata_key",
            metadata=metadata
        )
        
        saved_item = self.handler.get_congregado("metadata_key")
        self.assertEqual(saved_item['metadata'], metadata)
    
    def test_get_congregado(self):
        """Testa recuperação de congregado."""
        congregado_data = {"total": 750}
        
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="get_test_key"
        )
        
        retrieved = self.handler.get_congregado("get_test_key")
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved['id'], 'get_test_key')
        self.assertEqual(retrieved['total'], 750)
    
    def test_get_congregado_not_found(self):
        """Testa recuperação de congregado inexistente."""
        retrieved = self.handler.get_congregado("nonexistent")
        self.assertIsNone(retrieved)
    
    def test_batch_save_congregados(self):
        """Testa salvamento em lote."""
        congregados = [
            {"id": "batch_1", "total": 100},
            {"id": "batch_2", "total": 200},
            {"id": "batch_3", "total": 300}
        ]
        
        result = self.handler.batch_save_congregados(
            congregados=congregados,
            primary_key_field="id"
        )
        
        self.assertEqual(result['saved'], 3)
        self.assertEqual(result['total'], 3)
        self.assertEqual(result['failed'], 0)
    
    def test_batch_save_congregados_with_errors(self):
        """Testa salvamento em lote com alguns erros."""
        congregados = [
            {"id": "batch_1", "total": 100},
            {"invalid": "data"},  # Sem campo id
            {"id": "batch_3", "total": 300}
        ]
        
        result = self.handler.batch_save_congregados(
            congregados=congregados,
            primary_key_field="id"
        )
        
        # Pelo menos alguns devem ser salvos
        self.assertGreater(result['saved'], 0)
    
    def test_delete_congregado(self):
        """Testa remoção de congregado."""
        congregado_data = {"total": 999}
        
        self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="delete_test_key"
        )
        
        # Verificar que existe
        self.assertIsNotNone(self.handler.get_congregado("delete_test_key"))
        
        # Deletar
        result = self.handler.delete_congregado("delete_test_key")
        self.assertTrue(result)
        
        # Verificar que foi removido
        self.assertIsNone(self.handler.get_congregado("delete_test_key"))
    
    def test_delete_congregado_not_found(self):
        """Testa remoção de congregado inexistente."""
        result = self.handler.delete_congregado("nonexistent")
        # Em memória, retorna False se não existe
        self.assertFalse(result)
    
    def test_save_congregado_auto_version(self):
        """Testa incremento automático de versão."""
        congregado_data = {"total": 100}
        
        # Primeira chamada
        result1 = self.handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="auto_version_key"
        )
        
        # Segunda chamada (deve incrementar versão se dados diferentes)
        result2 = self.handler.save_congregado(
            congregado_data={"total": 200},  # Dados diferentes
            primary_key="auto_version_key"
        )
        
        self.assertEqual(result1['version'], 1)
        self.assertEqual(result2['version'], 2)

if __name__ == '__main__':
    unittest.main()
