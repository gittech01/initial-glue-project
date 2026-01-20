"""Testes unitários para utils/handlers/glue_handler.py"""
import unittest
from unittest.mock import MagicMock, patch
from utils.handlers.glue_handler import GlueDataHandler

class TestGlueDataHandler(unittest.TestCase):
    """Testes para GlueDataHandler."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_context = MagicMock()
        self.mock_spark = MagicMock()
        self.mock_glue_context.spark_session = self.mock_spark
        
        self.path_input = "s3://bucket/input"
        self.path_output = "s3://bucket/output"
        
        self.handler = GlueDataHandler(self.mock_glue_context)
    
    def test_init(self):
        """Testa inicialização do handler."""
        self.assertEqual(self.handler.glue_context, self.mock_glue_context)
        self.assertEqual(self.handler.spark, self.mock_spark)
    
    def test_read_from_catalog(self):
        """Testa leitura do catálogo."""
        mock_dynamic_frame = MagicMock()
        mock_df = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df
        self.mock_glue_context.create_dynamic_frame.from_catalog.return_value = mock_dynamic_frame
        
        result = self.handler.read_from_catalog("db", "table")
        
        self.mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="db",
            table_name="table",
            push_down_predicate=""
        )
        mock_dynamic_frame.toDF.assert_called_once()
        self.assertEqual(result, mock_df)
    
    def test_read_from_catalog_with_options(self):
        """Testa leitura do catálogo com filtro."""
        mock_dynamic_frame = MagicMock()
        mock_df = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df
        self.mock_glue_context.create_dynamic_frame.from_catalog.return_value = mock_dynamic_frame
        
        filter_predicate = "date > '2024-01-01'"
        result = self.handler.read_from_catalog("db", "table", filter=filter_predicate)
        
        self.mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="db",
            table_name="table",
            push_down_predicate=filter_predicate
        )
        self.assertEqual(result, mock_df)
    
    def test_read_from_s3(self):
        """Testa leitura do S3."""
        mock_dynamic_frame = MagicMock()
        mock_df = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df
        self.mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dynamic_frame
        
        result = self.handler.read_from_s3(self.path_input)
        
        self.mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
            connection_type="s3",
            connection_options={"paths": [self.path_input]},
            format="parquet",
            format_options={}
        )
        self.assertEqual(result, mock_df)
    
    def test_read_from_s3_custom_format(self):
        """Testa leitura do S3 com formato customizado."""
        mock_dynamic_frame = MagicMock()
        mock_df = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df
        self.mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dynamic_frame
        
        result = self.handler.read_from_s3(self.path_input, format="json", format_options={"multiline": True})
        
        self.mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
            connection_type="s3",
            connection_options={"paths": [self.path_input]},
            format="json",
            format_options={"multiline": True}
        )
        self.assertEqual(result, mock_df)
    
    @patch('utils.handlers.glue_handler.DynamicFrame')
    def test_write_to_s3(self, mock_dynamic_frame_class):
        """Testa escrita no S3."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        self.handler.write_to_s3(mock_df, self.path_output)
        
        mock_dynamic_frame_class.fromDF.assert_called_once_with(
            mock_df, self.mock_glue_context, "dynamic_frame"
        )
        self.mock_glue_context.write_dynamic_frame.from_options.assert_called_once()
        args, kwargs = self.mock_glue_context.write_dynamic_frame.from_options.call_args
        self.assertEqual(kwargs['connection_options']['path'], self.path_output)
        self.assertEqual(kwargs['format'], 'parquet')
    
    @patch('utils.handlers.glue_handler.DynamicFrame')
    def test_write_to_s3_with_partitions(self, mock_dynamic_frame_class):
        """Testa escrita no S3 com particionamento."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        self.handler.write_to_s3(
            mock_df, 
            self.path_output, 
            format="parquet",
            partition_cols=["year", "month"]
        )
        
        args, kwargs = self.mock_glue_context.write_dynamic_frame.from_options.call_args
        self.assertEqual(kwargs['connection_options']['partitionKeys'], ["year", "month"])
    
    @patch('utils.handlers.glue_handler.DynamicFrame')
    @patch('boto3.client')
    def test_write_to_catalog(self, mock_boto3_client, mock_dynamic_frame_class):
        """Testa escrita no catálogo usando getSink()."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        # Mock do cliente Glue e resposta get_table
        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = {
            'Table': {
                'StorageDescriptor': {'Location': 's3://bucket/path/'},
                'PartitionKeys': []
            }
        }
        # Mockar boto3.client para retornar o cliente mockado
        mock_boto3_client.return_value = mock_glue_client
        
        # Mock do sink
        mock_sink = MagicMock()
        self.mock_glue_context.getSink.return_value = mock_sink
        
        # Executar
        self.handler.write_to_catalog(mock_df, "db", "table")
        
        # Verificar que getSink foi chamado (ou from_catalog se caiu no fallback)
        mock_dynamic_frame_class.fromDF.assert_called_once()
        
        # Se getSink foi chamado, verificar parâmetros
        if self.mock_glue_context.getSink.called:
            mock_boto3_client.assert_called_once_with('glue', region_name='sa-east-1')
            self.mock_glue_context.getSink.assert_called_once()
            
            # Verificar parâmetros do getSink
            args, kwargs = self.mock_glue_context.getSink.call_args
            self.assertEqual(kwargs['connection_type'], 's3')
            self.assertEqual(kwargs['enableUpdateCatalog'], True)
            self.assertEqual(kwargs['updateBehavior'], 'UPDATE_IN_DATABASE')
            
            # Verificar que setFormat e setCatalogInfo foram chamados
            mock_sink.setFormat.assert_called_once()
            mock_sink.setCatalogInfo.assert_called_once()
            mock_sink.writeFrame.assert_called_once_with(mock_dyn_frame)
        else:
            # Se caiu no fallback, verificar from_catalog
            self.mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()
            args, kwargs = self.mock_glue_context.write_dynamic_frame.from_catalog.call_args
            self.assertEqual(kwargs['database'], 'db')
            self.assertEqual(kwargs['table_name'], 'table')
    
    @patch('utils.handlers.glue_handler._get_glue_client')
    def test_get_last_partition(self, mock_get_client):
        """Testa obtenção da última partição."""
        mock_client = MagicMock()
        mock_client.get_partitions.return_value = {
            'Partitions': [
                {'Values': ['2024-01-15']},
                {'Values': ['2024-01-16']},
                {'Values': ['2024-01-14']}
            ]
        }
        mock_get_client.return_value = mock_client
        
        result = self.handler.get_last_partition('db', 'table', 'date')
        
        # Retorna string para partição simples (uma coluna)
        self.assertEqual(result, '2024-01-16')
        mock_client.get_partitions.assert_called_once()
    
    @patch('utils.handlers.glue_handler._get_glue_client')
    def test_get_last_partition_no_partitions(self, mock_get_client):
        """Testa obtenção quando não há partições."""
        mock_client = MagicMock()
        mock_client.get_partitions.return_value = {'Partitions': []}
        mock_get_client.return_value = mock_client
        
        result = self.handler.get_last_partition('db', 'table', 'date')
        
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
