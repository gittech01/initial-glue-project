"""Testes unitários para utils/business/nova_regra.py"""
import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime
import pyspark.sql.functions as F

from utils.business.nova_regra import (
    NovaRegraConsolidacao,
    ConfigConsolidacaoPosicao
)
from utils.config. settings import AppConfig


class TestConfigConsolidacaoPosicao(unittest. TestCase):
    """Testes para ConfigConsolidacaoPosicao."""
    
    def test_init_com_defaults(self):
        """Testa inicialização com valores padrão."""
        config = ConfigConsolidacaoPosicao()
        
        self. assertEqual(config.coluna_numero_operacao, "num_oper")
        self.assertEqual(config.coluna_origem, "origem_registro")
        self.assertEqual(config.origem_preferida_desempate, "ONLINE")
        self.assertTrue(config.nulos_como_minimo)
        self.assertEqual(config.prioridade_minima, 1)
    
    def test_init_com_valores_customizados(self):
        """Testa inicialização com valores customizados."""
        config = ConfigConsolidacaoPosicao(
            coluna_numero_operacao="op_number",
            origem_preferida_desempate="BATCH"
        )
        
        self.assertEqual(config.coluna_numero_operacao, "op_number")
        self.assertEqual(config.origem_preferida_desempate, "BATCH")


class TestNovaRegraConsolidacao(unittest.TestCase):
    """Testes para NovaRegraConsolidacao."""
    
    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        self.mock_config = MagicMock()
        self.mock_config.output_path = "s3://bucket/output"
        self.mock_config.default_output_format = "parquet"
        
        self.regra = NovaRegraConsolidacao(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.mock_config
        )
    
    def test_init(self):
        """Testa inicialização da regra."""
        self.assertEqual(self.regra.glue_handler, self.mock_glue_handler)
        self.assertEqual(self.regra.journey_controller, self.mock_journey_controller)
        self.assertEqual(self.regra.dynamodb_handler, self.mock_dynamodb_handler)
        self.assertEqual(self.regra.config, self.mock_config)
        self.assertIsNotNone(self.regra. cfg)
    
    def test_ler_dados_success(self):
        """Testa leitura de dados com sucesso."""
        mock_df = MagicMock()
        self.mock_glue_handler. read_from_catalog.return_value = mock_df
        
        resultado = self. regra._ler_dados("db_test", "table_test")
        
        self.assertEqual(resultado, mock_df)
        self.mock_glue_handler.read_from_catalog.assert_called_once_with(
            database="db_test",
            table_name="table_test"
        )
    
    def test_ler_dados_error(self):
        """Testa erro ao ler dados."""
        self.mock_glue_handler. read_from_catalog.side_effect = Exception("Erro de leitura")
        
        with self.assertRaises(Exception):
            self.regra._ler_dados("db_test", "table_test")
    
    @patch('pyspark.sql.functions.to_timestamp')
    @patch('pyspark.sql. functions.col')
    def test_normalizar_tipos_e_nulos_success(self, mock_col, mock_to_timestamp):
        """Testa normalização de tipos e nulos."""
        mock_df = MagicMock()
        mock_df_norm = MagicMock()
        mock_df. withColumn.return_value = mock_df_norm
        mock_df_norm.withColumn.return_value = mock_df_norm
        
        resultado = self.regra._normalizar_tipos_e_nulos(mock_df)
        
        self.assertEqual(resultado, mock_df_norm)
        # Verificar que withColumn foi chamado pelo menos 3 vezes (3 colunas)
        self.assertGreaterEqual(mock_df. withColumn.call_count, 3)
    
    @patch('pyspark.sql.functions.to_timestamp')
    def test_normalizar_tipos_e_nulos_com_nulos_como_minimo(self, mock_to_timestamp):
        """Testa tratamento de nulos como mínimo."""
        mock_df = MagicMock()
        mock_df_intermediario = MagicMock()
        mock_df.withColumn.return_value = mock_df_intermediario
        mock_df_intermediario.withColumn.return_value = mock_df_intermediario
        
        self.regra. cfg.nulos_como_minimo = True
        
        resultado = self.regra._normalizar_tipos_e_nulos(mock_df)
        
        # Deve ter chamado withColumn para tratar nulos
        self.assertGreater(mock_df.withColumn. call_count, 0)
    
    @patch('pyspark.sql.functions.row_number')
    @patch('pyspark.sql.functions. when')
    @patch('pyspark.sql. window.Window. partitionBy')
    def test_aplicar_regra_consolidacao_success(self, mock_partition, mock_when, mock_row_number):
        """Testa aplicação da regra de consolidação."""
        mock_df = MagicMock()
        mock_df_com_rank = MagicMock()
        mock_df_consolidado = MagicMock()
        
        mock_df.withColumn.return_value = mock_df_com_rank
        mock_df_com_rank.filter.return_value = mock_df_consolidado
        mock_df_consolidado.drop.return_value = mock_df_consolidado
        
        resultado = self.regra._aplicar_regra_consolidacao(mock_df)
        
        self.assertEqual(resultado, mock_df_consolidado)
        mock_df. withColumn.assert_called()
    
    def test_obter_colunas_retorno(self):
        """Testa obtenção de colunas de retorno."""
        mock_df = MagicMock()
        mock_df. columns = ['col1', 'col2', 'col3']
        
        colunas = self.regra._obter_colunas_retorno(mock_df)
        
        self.assertEqual(colunas, ['col1', 'col2', 'col3'])
    
    def test_selecionar_colunas_retorno_sem_origem(self):
        """Testa seleção de colunas quando origem não está presente."""
        mock_df = MagicMock()
        mock_df_select = MagicMock()
        mock_df.select.return_value = mock_df_select
        
        colunas = ['col1', 'col2']
        resultado = self.regra._selecionar_colunas_retorno(mock_df, colunas)
        
        # Deve ter adicionado coluna origem
        chamada = mock_df.select.call_args
        self.assertIn('origem_registro', chamada[0])
    
    def test_selecionar_colunas_retorno_com_origem(self):
        """Testa seleção de colunas quando origem já está presente."""
        mock_df = MagicMock()
        mock_df_select = MagicMock()
        mock_df.select.return_value = mock_df_select
        
        colunas = ['col1', 'origem_registro', 'col2']
        resultado = self.regra._selecionar_colunas_retorno(mock_df, colunas)
        
        # Deve chamar select com as mesmas colunas
        mock_df.select.assert_called()
    
    def test_salvar_resultado_com_output_path(self):
        """Testa salvamento com path fornecido."""
        mock_df = MagicMock()
        
        resultado = self.regra._salvar_resultado(
            mock_df,
            output_path="s3://bucket/custom"
        )
        
        self.mock_glue_handler.write_to_s3.assert_called_once()
        chamada = self.mock_glue_handler.write_to_s3.call_args
        self.assertEqual(chamada[1]['path'], "s3://bucket/custom")
    
    def test_salvar_resultado_sem_output_path(self):
        """Testa salvamento sem path (usa default)."""
        mock_df = MagicMock()
        
        resultado = self.regra._salvar_resultado(mock_df)
        
        self.mock_glue_handler. write_to_s3.assert_called_once()
        chamada = self.mock_glue_handler.write_to_s3.call_args
        self.assertEqual(chamada[1]['path'], "s3://bucket/output")
    
    def test_salvar_resultado_error(self):
        """Testa erro ao salvar."""
        mock_df = MagicMock()
        self.mock_glue_handler. write_to_s3.side_effect = Exception("Erro de escrita")
        
        with self.assertRaises(Exception):
            self.regra._salvar_resultado(mock_df)
    
    @patch. object(NovaRegraConsolidacao, '_ler_dados')
    @patch.object(NovaRegraConsolidacao, '_normalizar_tipos_e_nulos')
    @patch.object(NovaRegraConsolidacao, '_aplicar_regra_consolidacao')
    @patch.object(NovaRegraConsolidacao, '_obter_colunas_retorno')
    @patch.object(NovaRegraConsolidacao, '_selecionar_colunas_retorno')
    @patch.object(NovaRegraConsolidacao, '_salvar_resultado')
    def test_processar_consolidacao_success(
        self,
        mock_salvar,
        mock_selecionar,
        mock_obter_colunas,
        mock_aplicar_regra,
        mock_normalizar,
        mock_ler
    ):
        """Testa processamento completo com sucesso."""
        mock_df_entrada = MagicMock()
        mock_df_entrada.count.return_value = 100
        mock_df_normalizado = MagicMock()
        mock_df_consolidado = MagicMock()
        mock_df_consolidado.count.return_value = 50
        mock_df_final = MagicMock()
        
        mock_ler.return_value = mock_df_entrada
        mock_normalizar.return_value = mock_df_normalizado
        mock_aplicar_regra.return_value = mock_df_consolidado
        mock_obter_colunas.return_value = ['col1', 'col2']
        mock_selecionar.return_value = mock_df_final
        mock_salvar.return_value = {
            'output_path': 's3://bucket/output',
            'duration_seconds': 10
        }
        
        resultado = self.regra.processar_consolidacao(
            database="test_db",
            table_name="test_table"
        )
        
        self.assertEqual(resultado['status'], 'success')
        self.assertEqual(resultado['registry_count'], 50)
        self.assertEqual(resultado['output_path'], 's3://bucket/output')
        
        mock_ler.assert_called_once()
        mock_normalizar.assert_called_once()
        mock_aplicar_regra.assert_called_once()
        mock_salvar.assert_called_once()
    
    @patch.object(NovaRegraConsolidacao, '_ler_dados')
    def test_processar_consolidacao_error(self, mock_ler):
        """Testa erro no processamento."""
        mock_ler.side_effect = Exception("Erro de processamento")
        
        with self.assertRaises(Exception):
            self.regra.processar_consolidacao(
                database="test_db",
                table_name="test_table"
            )
    
    @patch.object(NovaRegraConsolidacao, '_ler_dados')
    @patch.object(NovaRegraConsolidacao, '_normalizar_tipos_e_nulos')
    @patch.object(NovaRegraConsolidacao, '_aplicar_regra_consolidacao')
    @patch.object(NovaRegraConsolidacao, '_obter_colunas_retorno')
    @patch.object(NovaRegraConsolidacao, '_selecionar_colunas_retorno')
    @patch.object(NovaRegraConsolidacao, '_salvar_resultado')
    def test_processar_consolidacao_com_output_path(
        self,
        mock_salvar,
        mock_selecionar,
        mock_obter_colunas,
        mock_aplicar_regra,
        mock_normalizar,
        mock_ler
    ):
        """Testa processamento com output_path customizado."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        
        mock_ler.return_value = mock_df
        mock_normalizar.return_value = mock_df
        mock_aplicar_regra.return_value = mock_df
        mock_obter_colunas.return_value = ['col1']
        mock_selecionar. return_value = mock_df
        mock_salvar.return_value = {
            'output_path': 's3://bucket/custom',
            'duration_seconds': 5
        }
        
        resultado = self.regra.processar_consolidacao(
            database="test_db",
            table_name="test_table",
            output_path="s3://bucket/custom"
        )
        
        chamada_salvar = mock_salvar. call_args
        self.assertEqual(chamada_salvar[1]. get('output_path'), 's3://bucket/custom')
    
    @patch.object(NovaRegraConsolidacao, '_ler_dados')
    @patch.object(NovaRegraConsolidacao, '_normalizar_tipos_e_nulos')
    @patch.object(NovaRegraConsolidacao, '_aplicar_regra_consolidacao')
    @patch.object(NovaRegraConsolidacao, '_obter_colunas_retorno')
    @patch.object(NovaRegraConsolidacao, '_selecionar_colunas_retorno')
    @patch.object(NovaRegraConsolidacao, '_salvar_resultado')
    def test_processar_consolidacao_idempotencia(
        self,
        mock_salvar,
        mock_selecionar,
        mock_obter_colunas,
        mock_aplicar_regra,
        mock_normalizar,
        mock_ler
    ):
        """Testa idempotência - múltiplas chamadas não causam problemas."""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        
        mock_ler.return_value = mock_df
        mock_normalizar.return_value = mock_df
        mock_aplicar_regra.return_value = mock_df
        mock_obter_colunas.return_value = ['col1']
        mock_selecionar.return_value = mock_df
        mock_salvar.return_value = {
            'output_path':  's3://bucket/output',
            'duration_seconds':  5
        }
        
        # Primeira execução
        resultado1 = self.regra.processar_consolidacao(
            database="test_db",
            table_name="test_table"
        )
        
        # Segunda execução
        resultado2 = self.regra.processar_consolidacao(
            database="test_db",
            table_name="test_table"
        )
        
        # Resultados devem ser iguais
        self.assertEqual(resultado1['status'], resultado2['status'])
        self.assertEqual(resultado1['registry_count'], resultado2['registry_count'])


class TestIntegracaoComHandlers(unittest.TestCase):
    """Testes de integração com handlers."""
    
    def setUp(self):
        """Setup para integração."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        self.mock_config = MagicMock()
        self.mock_config.output_path = "s3://bucket/output"
        self.mock_config.default_output_format = "parquet"
    
    def test_fluxo_completo_com_handlers(self):
        """Testa fluxo completo integrando todos os handlers."""
        regra = NovaRegraConsolidacao(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.mock_config
        )
        
        # Simulação de dados
        mock_df_entrada = MagicMock()
        mock_df_entrada.count. return_value = 100
        
        # Mock do fluxo completo
        self.mock_glue_handler. read_from_catalog.return_value = mock_df_entrada
        
        with patch. object(regra, '_normalizar_tipos_e_nulos', return_value=mock_df_entrada):
            with patch.object(regra, '_aplicar_regra_consolidacao', return_value=mock_df_entrada):
                with patch.object(regra, '_obter_colunas_retorno', return_value=['col1']):
                    with patch.object(regra, '_selecionar_colunas_retorno', return_value=mock_df_entrada):
                        with patch.object(regra, '_salvar_resultado', return_value={'output_path': 's3://bucket/output'}):
                            resultado = regra.processar_consolidacao(
                                database="test_db",
                                table_name="test_table"
                            )
        
        self.assertEqual(resultado['status'], 'success')
        self.mock_glue_handler.read_from_catalog.assert_called_once()
        self.mock_glue_handler.write_to_s3.assert_called()


if __name__ == '__main__':
    unittest.main()