from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union, Any

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.business.base_processor import BaseBusinessProcessor
from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig

logger = logging.getLogger(__name__)

@dataclass
class ConfigConsolidacaoPosicao:
    """
    Regra de comparação ENTRE origens (A vs B):
        1) data_valor_evento_operacao (desc)
        2) prioridade_evento_operacao (desc)
        3) data_recebimento_evento_operacao (desc)
        4) desempate determinístico por origem preferida (ONLINE por padrão)
    """
    coluna_numero_operacao: str = "num_oper"
    coluna_codigo_versao_operacao: str = "cod_idef_vers_oper"
    coluna_codigo_evento_processado: str = "cod_idef_even_prcs"
    coluna_data_valor_evento: str = "dat_vlr_even_oper"
    coluna_prioridade_evento: str = "num_prio_even_oper"
    coluna_data_recebimento_evento: str = "dat_recm_even_oper"
    coluna_origem: str = "origem_registro"
    origem_preferida_desempate: str = "ONLINE"
    nulos_como_minimo: bool = True
    timestamp_minimo: str = "1900-01-01 00:00:00"
    prioridade_minima: int = 1
    colunas_ordem_ultima_ocorrencia: Optional[List[str]] = None
    reduzir_cada_lado_para_ultima_ocorrencia: bool = True

@dataclass(frozen=True)
class JoinSpec:
    left_alias: str
    right_alias: str
    how: str = "inner"
    on: Sequence[Tuple[str, str]] = ()
    rename_collisions: bool = True
    collision_prefix: Optional[str] = None

@dataclass(frozen=True)
class OrigemSpec:
    tables: Dict[str, str]
    joins: Sequence[JoinSpec]
    select_cols: Optional[List[str]] = None
    where_expr: Optional[str] = None

InputDF = Union[DataFrame, List[DataFrame], OrigemSpec]

class ConsolidadorPosicaoOperacao(BaseBusinessProcessor):
    """
    Consolidador de Posição de Operação - Implementação Completa.
    
    Esta classe integra a lógica de negócio de consolidação entre origens (A vs B)
    dentro do framework de processamento do Glue.
    """

    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig,
        configuracao: ConfigConsolidacaoPosicao = ConfigConsolidacaoPosicao()
    ):
        super().__init__(glue_handler, journey_controller, dynamodb_handler, config)
        self.spark = glue_handler.spark
        self.cfg = configuracao
        self._definir_ordem_padrao_ultima_ocorrencia()

    def _definir_ordem_padrao_ultima_ocorrencia(self):
        if not self.cfg.colunas_ordem_ultima_ocorrencia:
            self.cfg.colunas_ordem_ultima_ocorrencia = [
                self.cfg.coluna_data_valor_evento,
                self.cfg.coluna_prioridade_evento,
                self.cfg.coluna_data_recebimento_evento
            ]

    # -------------------------------------------------------------------------
    # IMPLEMENTAÇÃO DOS HOOKS DO BASE PROCESSOR
    # -------------------------------------------------------------------------

    def _read_data(self, **kwargs) -> DataFrame:
        """
        No fluxo do BaseProcessor, este método é chamado para ler os dados.
        Para consolidação, materializamos a Origem A como o DF principal.
        """
        origem_a = kwargs.get('origem_a')
        if not origem_a:
            raise ValueError("Parâmetro 'origem_a' é obrigatório")
        return self._materializar_input(origem_a)

    def _transform_data(self, df: DataFrame, **kwargs) -> Dict:
        """
        Aplica a lógica de consolidação completa.
        """
        origem_a = kwargs.get('origem_a')
        origem_b = kwargs.get('origem_b')
        nome_a = kwargs.get('nome_origem_a', "ONLINE")
        nome_b = kwargs.get('nome_origem_b', "BATCH")
        colunas_retorno = kwargs.get('colunas_retorno')

        if not origem_b:
            raise ValueError("Parâmetro 'origem_b' é obrigatório para consolidação")

        # Executa a consolidação
        vencedores_df = self.consolidar_posicoes(
            origem_a=origem_a,
            origem_b=origem_b,
            nome_origem_a=nome_a,
            nome_origem_b=nome_b,
            colunas_retorno=colunas_retorno
        )

        # Armazena o DF resultante para o hook de escrita
        self._last_result_df = vencedores_df

        return {
            "total_vencedores": vencedores_df.count(),
            "origem_a": nome_a,
            "origem_b": nome_b,
            "status": "sucesso"
        }

    def _write_output(self, df: DataFrame, transformed_data: Dict, output_path: str, **kwargs):
        """Sobrescreve para escrever o DF de vencedores em vez do original."""
        if hasattr(self, '_last_result_df'):
            self.glue_handler.write_to_s3(
                df=self._last_result_df,
                path=output_path,
                format=self.config.default_output_format
            )

    def _get_congregado_key(self, **kwargs) -> str:
        return f"consolidacao_{kwargs.get('nome_origem_a', 'A')}_vs_{kwargs.get('nome_origem_b', 'B')}"

    # -------------------------------------------------------------------------
    # API DE CONSOLIDAÇÃO (LÓGICA ENVIADA PELO USUÁRIO)
    # -------------------------------------------------------------------------

    def consolidar_posicoes(
        self,
        origem_a: InputDF,
        origem_b: InputDF,
        nome_origem_a: str = "ONLINE",
        nome_origem_b: str = "BATCH",
        colunas_retorno: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Retorna 1 linha vencedora por operação (num_oper), incluindo origem_registro.
        """
        df_a = self._materializar_input(origem_a)
        df_b = self._materializar_input(origem_b)

        lado_a = self._marcar_origem(self._normalizar_tipos_e_nulos(df_a), nome_origem_a)
        lado_b = self._marcar_origem(self._normalizar_tipos_e_nulos(df_b), nome_origem_b)

        if self.cfg.reduzir_cada_lado_para_ultima_ocorrencia:
            lado_a = self._selecionar_ultima_ocorrencia_por_origem(lado_a)
            lado_b = self._selecionar_ultima_ocorrencia_por_origem(lado_b)

        unificado = lado_a.unionByName(lado_b, allowMissingColumns=True)
        vencedores = self._selecionar_vencedor_por_operacao(unificado)

        if colunas_retorno:
            vencedores = self._selecionar_colunas_retorno(vencedores, colunas_retorno)

        return vencedores

    def _materializar_input(self, input_data: InputDF) -> DataFrame:
        if isinstance(input_data, DataFrame):
            return input_data
        
        if isinstance(input_data, list):
            if not input_data:
                raise ValueError("Lista de DataFrames vazia")
            df_final = input_data[0]
            for df in input_data[1:]:
                df_final = df_final.unionByName(df, allowMissingColumns=True)
            return df_final
            
        if isinstance(input_data, OrigemSpec):
            return self._montar_df_da_origem_spec(input_data)
            
        raise ValueError(f"Tipo de input não suportado: {type(input_data)}")

    def _montar_df_da_origem_spec(self, spec: OrigemSpec) -> DataFrame:
        """Lê tabelas e aplica joins conforme OrigemSpec."""
        dfs = {}
        for alias, table_path in spec.tables.items():
            dfs[alias] = self.spark.table(table_path)

        # Aplica joins
        result_df = None
        for join in spec.joins:
            left = dfs[join.left_alias] if result_df is None else result_df
            right = dfs[join.right_alias]
            
            # Monta condição de join
            condition = None
            for l_col, r_col in join.on:
                part = (F.col(f"{join.left_alias}.{l_col}") == F.col(f"{join.right_alias}.{r_col}"))
                condition = part if condition is None else condition & part
            
            result_df = left.alias(join.left_alias).join(
                right.alias(join.right_alias),
                on=condition,
                how=join.how
            )

        if result_df is None:
            # Se não houve joins, retorna a primeira tabela
            first_alias = list(spec.tables.keys())[0]
            result_df = dfs[first_alias]

        if spec.where_expr:
            result_df = result_df.where(spec.where_expr)

        if spec.select_cols:
            result_df = result_df.select(*spec.select_cols)

        return result_df

    def _normalizar_tipos_e_nulos(self, df: DataFrame) -> DataFrame:
        """Normaliza colunas de data e prioridade para evitar erros no desempate."""
        c = self.cfg
        
        # Trata nulos se configurado
        if c.nulos_como_minimo:
            df = df.withColumn(c.coluna_data_valor_evento, F.coalesce(F.col(c.coluna_data_valor_evento), F.lit(c.timestamp_minimo)))
            df = df.withColumn(c.coluna_prioridade_evento, F.coalesce(F.col(c.coluna_prioridade_evento), F.lit(c.prioridade_minima)))
            df = df.withColumn(c.coluna_data_recebimento_evento, F.coalesce(F.col(c.coluna_data_recebimento_evento), F.lit(c.timestamp_minimo)))
            
        return df

    def _marcar_origem(self, df: DataFrame, nome: str) -> DataFrame:
        return df.withColumn(self.cfg.coluna_origem, F.lit(nome))

    def _selecionar_ultima_ocorrencia_por_origem(self, df: DataFrame) -> DataFrame:
        """Garante que cada origem tenha apenas uma linha por operação antes da união."""
        window = Window.partitionBy(self.cfg.coluna_numero_operacao).orderBy(
            *[F.col(c).desc() for c in self.cfg.colunas_ordem_ultima_ocorrencia]
        )
        return df.withColumn("_rn", F.row_number().over(window)).filter("_rn == 1").drop("_rn")

    def _selecionar_vencedor_por_operacao(self, df: DataFrame) -> DataFrame:
        """Aplica a regra de ouro para escolher entre A e B."""
        c = self.cfg
        
        # Regra de desempate por origem preferida
        order_origem = F.when(F.col(c.coluna_origem) == c.origem_preferida_desempate, 1).otherwise(2)
        
        window = Window.partitionBy(c.coluna_numero_operacao).orderBy(
            F.col(c.coluna_data_valor_evento).desc(),
            F.col(c.coluna_prioridade_evento).desc(),
            F.col(c.coluna_data_recebimento_evento).desc(),
            order_origem.asc()
        )
        
        return df.withColumn("_rn", F.row_number().over(window)).filter("_rn == 1").drop("_rn")

    def _selecionar_colunas_retorno(self, df: DataFrame, colunas: List[str]) -> DataFrame:
        return df.select(*colunas)
