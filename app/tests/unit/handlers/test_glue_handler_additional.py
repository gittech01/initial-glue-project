"""Testes adicionais para utils/handlers/glue_handler.py para aumentar cobertura"""
import unittest
from unittest.mock import MagicMock, patch
from utils.handlers.glue_handler import GlueDataHandler, _get_glue_client


class TestGlueDataHandlerAdditional(unittest.TestCase):
    """Testes adicionais para aumentar cobertura."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_context = MagicMock()
        self.mock_spark = MagicMock()
        self.mock_glue_context.spark_session = self.mock_spark
        self.handler = GlueDataHandler(self.mock_glue_context)
    
    def test_read_from_s3_list_path(self):
        """Testa leitura do S3 com lista de paths."""
        mock_dynamic_frame = MagicMock()
        mock_df = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df
        self.mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dynamic_frame
        
        paths = ["s3://bucket/path1", "s3://bucket/path2"]
        result = self.handler.read_from_s3(paths)
        
        self.mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
            connection_type="s3",
            connection_options={"paths": paths},
            format="parquet",
            format_options={}
        )
        self.assertEqual(result, mock_df)
    
    @patch('utils.handlers.glue_handler.DynamicFrame')
    def test_write_to_s3_with_compression(self, mock_dynamic_frame_class):
        """Testa escrita no S3 com compressão customizada."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        self.handler.write_to_s3(
            mock_df,
            "s3://bucket/path",
            format="parquet",
            compression="gzip"
        )
        
        args, kwargs = self.mock_glue_context.write_dynamic_frame.from_options.call_args
        self.assertEqual(kwargs['format_options']['compression'], 'gzip')
    
    @patch('utils.handlers.glue_handler.DynamicFrame')
    @patch('boto3.client')
    def test_write_to_catalog_with_partition_keys(self, mock_boto3_client, mock_dynamic_frame_class):
        """Testa escrita no catálogo com partition keys."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = {
            'Table': {
                'StorageDescriptor': {'Location': 's3://bucket/path/'},
                'PartitionKeys': [{'Name': 'year'}, {'Name': 'month'}]
            }
        }
        mock_boto3_client.return_value = mock_glue_client
        
        mock_sink = MagicMock()
        self.mock_glue_context.getSink.return_value = mock_sink
        
        self.handler.write_to_catalog(mock_df, "db", "table")
        
        args, kwargs = self.mock_glue_context.getSink.call_args
        self.assertEqual(kwargs['partitionKeys'], ['year', 'month'])
    
    @patch('utils.handlers.glue_handler.DynamicFrame')
    @patch('boto3.client')
    def test_write_to_catalog_fallback(self, mock_boto3_client, mock_dynamic_frame_class):
        """Testa fallback para from_catalog quando getSink falha."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        # Simular erro no get_table
        mock_glue_client = MagicMock()
        mock_glue_client.get_table.side_effect = Exception("Unable to locate credentials")
        mock_boto3_client.return_value = mock_glue_client
        
        # Não deve ter getSink chamado, deve usar fallback
        self.handler.write_to_catalog(mock_df, "db", "table")
        
        # Verificar que from_catalog foi chamado (fallback)
        self.mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()
    
    @patch('utils.handlers.glue_handler._get_glue_client')
    def test_get_last_partition_multiple_columns(self, mock_get_client):
        """Testa obtenção da última partição com múltiplas colunas."""
        mock_client = MagicMock()
        mock_client.get_partitions.return_value = {
            'Partitions': [
                {'Values': ['2024', '01', '15']},
                {'Values': ['2024', '01', '16']},
                {'Values': ['2024', '01', '14']}
            ]
        }
        mock_get_client.return_value = mock_client
        
        result = self.handler.get_last_partition('db', 'table', 'date')
        
        # Deve retornar lista para múltiplas colunas
        self.assertEqual(result, ['2024', '01', '15'])
    
    @patch('utils.handlers.glue_handler._get_glue_client')
    def test_get_last_partition_empty_values(self, mock_get_client):
        """Testa obtenção quando partições têm valores vazios."""
        mock_client = MagicMock()
        mock_client.get_partitions.return_value = {
            'Partitions': [
                {'Values': []},
                {'Values': ['2024-01-16']}
            ]
        }
        mock_get_client.return_value = mock_client
        
        result = self.handler.get_last_partition('db', 'table', 'date')
        
        # Deve retornar a partição válida
        self.assertEqual(result, '2024-01-16')
    
    @patch('boto3.client')
    def test_get_glue_client_with_region(self, mock_boto3_client):
        """Testa _get_glue_client com região especificada."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        result = _get_glue_client('us-east-1')
        
        mock_boto3_client.assert_called_once_with('glue', region_name='us-east-1')
        self.assertEqual(result, mock_client)
    
    @patch('boto3.client')
    def test_get_glue_client_without_region(self, mock_boto3_client):
        """Testa _get_glue_client sem região (usa padrão)."""
        mock_client = MagicMock()
        mock_boto3_client.return_value = mock_client
        
        result = _get_glue_client(None)
        
        mock_boto3_client.assert_called_once_with('glue', region_name='sa-east-1')
        self.assertEqual(result, mock_client)
    
    @patch('boto3.client')
    def test_get_glue_client_exception_fallback(self, mock_boto3_client):
        """Testa fallback quando boto3.client falha."""
        mock_boto3_client.side_effect = Exception("No credentials")
        
        result = _get_glue_client(None)
        
        # Deve retornar MagicMock como fallback
        self.assertIsInstance(result, MagicMock)


if __name__ == '__main__':
    unittest.main()
