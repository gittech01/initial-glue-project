import unittest
from unittest.mock import MagicMock, patch
from src.glue_handler import GlueDataHandler

class TestGlueDataHandler(unittest.TestCase):
    def setUp(self):
        # Mock do GlueContext e SparkSession
        self.mock_glue_context = MagicMock()
        self.mock_spark = MagicMock()
        self.mock_glue_context.spark_session = self.mock_spark

        self.path_input = "s3://bucket/input"
        self.path_output = "s3://bucket/output"

        self.handler = GlueDataHandler(self.mock_glue_context)

    def test_read_from_catalog(self):
        # Configurar mock para retornar um DynamicFrame que pode ser convertido em DF
        mock_dynamic_frame = MagicMock()
        self.mock_glue_context.create_dynamic_frame.from_catalog.return_value = mock_dynamic_frame
        
        self.handler.read_from_catalog("db", "table")
        
        self.mock_glue_context.create_dynamic_frame.from_catalog.assert_called_once_with(
            database="db",
            table_name="table",
            additional_options={}
        )
        mock_dynamic_frame.toDF.assert_called_once()

    def test_read_from_s3(self):
        mock_dynamic_frame = MagicMock()
        self.mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dynamic_frame
        
        self.handler.read_from_s3(self.path_input)
        
        self.mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
            connection_type="s3",
            connection_options={"paths": [self.path_input]},
            format="parquet",
            format_options={}
        )

    @patch('src.glue_handler.DynamicFrame.fromDF')
    def test_write_to_s3(self, mock_from_df):
        mock_df = MagicMock()
        mock_dyn_frame = MagicMock()
        mock_from_df.return_value = mock_dyn_frame
        
        self.handler.write_to_s3(mock_df, self.path_output)
        
        self.mock_glue_context.write_dynamic_frame.from_options.assert_called_once()
        args, kwargs = self.mock_glue_context.write_dynamic_frame.from_options.call_args
        self.assertEqual(kwargs['connection_options']['path'], self.path_output)

if __name__ == '__main__':
    unittest.main()
