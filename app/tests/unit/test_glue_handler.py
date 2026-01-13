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
            additional_options={}
        )
        mock_dynamic_frame.toDF.assert_called_once()
        self.assertEqual(result, mock_df)
    
    def test_read_from_catalog_with_options(self):
        """Testa leitura do catálogo com opções adicionais."""
        mock_dynamic_frame = MagicMock()
        mock_df = MagicMock()
        mock_dynamic_frame.toDF.return_value = mock_df
        self.mock_glue_context.create_dynamic_frame.from_catalog.return_value = mock_dynamic_frame
        
        options = {"pushdownPredicate": "date > '2024-01-01'"}
        result = self.handler.read_from_catalog("db", "table", options)
        
        self.mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="db",
            table_name="table",
            additional_options=options
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
    def test_write_to_catalog(self, mock_dynamic_frame_class):
        """Testa escrita no catálogo."""
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyn_frame
        
        self.handler.write_to_catalog(mock_df, "db", "table")
        
        mock_dynamic_frame_class.fromDF.assert_called_once()
        self.mock_glue_context.write_dynamic_frame.from_catalog.assert_called_once()
        args, kwargs = self.mock_glue_context.write_dynamic_frame.from_catalog.call_args
        self.assertEqual(kwargs['database'], 'db')
        self.assertEqual(kwargs['table_name'], 'table')

if __name__ == '__main__':
    unittest.main()
