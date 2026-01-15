
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple, Union

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dataclass
class ConfigConsolidacaoPosicao:
    """
    Regra de comparação ENTRE origens (A vs B):
        1) data_valor_evento_operacao (desc)
        2) prioridade_evento_operacao (desc)
        3) data_recebimento_evento_operacao (desc)
        4) desempate determinístico por origem preferida (ONLINE por padrão)

    Importante:
        - cod_idef_vers_oper NÃO participa da regra de escolha.
        - A versão é usada apenas para join no consolidar_registro_completo.
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

# -----------------------------------------------------------------------------
# 2) SPECS PARA MONTAR DF POR ORIGEM (GLUE CATALOG)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class JoinSpec:
    """
    Define um join (com suporte a múltiplas chaves).

    on: lista de pares (col_left, col_right) - suporta N chaves.
        Ex.: [("num_oper", "num_oper"), ("cod_idef_vers_oper", "cod_idef_vers_oper")]
    """

    left_alias: str
    right_alias: str
    how: str = "inner"
    on: Sequence[Tuple[str, str]] = ()
    
    # Se True, previne ambiguidade renomeando colunas do right que colidem com left (exceto keys)
    rename_collisions: bool = True
    
    # Prefixo usado quando ocorre colisão (ex.: "evt__coluna")
    collision_prefix: Optional[str] = None


@dataclass(frozen=True)
class OrigemSpec:
    """
    Config declarativa para montar um DF:
        - lê tabelas do Glue Catalog via spark.table
        - aplica joins em sequência (com N chaves por join)
        - aplica where_expr (ex.: partição)
        - aplica select final (opcional)
    """

    tables: Dict[str, str]                # alias -> "db.table" (Glue Catalog)
    joins: Sequence[JoinSpec]             # joins em ordem
    select_cols: Optional[List[str]] = None # se None, retorna todas
    where_expr: Optional[str] = None      # ex.: "ano_mes_dia = '20260113'"


InputDF = Union[DataFrame, List[DataFrame], OrigemSpec]


class ConsolidadorPosicaoOperacao:

    def __init__(self, spark: SparkSession, configuracao: ConfigConsolidacaoPosicao = ConfigConsolidacaoPosicao()):
        self.spark = spark
        self.cfg = configuracao
        self. _definir_ordem_padrao_ultima_ocorrencia()

    # -------------------------------------------------------------------------
    # API PRINCIPAL
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

        origem_a / origem_b podem ser:
            - DataFrame pronto
            - List[DataFrame] (faz unionByName)
            - OrigemSpec (monta DF lendo Glue Catalog + joins)
        """

        c = self.cfg

        df_a = self._materializar_input(origem_a)
        df_b = self._materializar_input(origem_b)

        lado_a = self._marcar_origem(self._normalizar_tipos_e_nulos(df_a), nome_origem_a)
        lado_b = self._marcar_origem(self._normalizar_tipos_e_nulos(df_b), nome_origem_b)

        if c.reduzir_cada_lado_para_ultima_ocorrencia:
            lado_a = self._selecionar_ultima_ocorrencia_por_origem(lado_a)
            lado_b = self._selecionar_ultima_ocorrencia_por_origem(lado_b)

        unificado = lado_a.unionByName(lado_b, allowMissingColumns=True)
        vencedores = self._selecionar_vencedor_por_operacao(unificado)

        if colunas_retorno:
            vencedores = self._selecionar_colunas_retorno(vencedores, colunas_retorno)

        return vencedores
    
    def consolidar_registro_completo(
            self,
            df_vencedores: DataFrame,
            df_completo_a: DataFrame,
            df_completo_b: DataFrame,
            nome_origem_a: str = "ONLINE",
            nome_origem_b: str = "BATCH",
        ) -> DataFrame:
            """
            Retorna o registro completo do lado vencedor.
            Join por (num_oper, cod_idef_vers_oper).
            """

            c = self.cfg
            chaves = [c.coluna_numero_operacao, c.coluna_codigo_versao_operacao]

            vencedores_a = df_vencedores.filter(F.col(c.coluna_origem) == F.lit(nome_origem_a))
            vencedores_b = df_vencedores.filter(F.col(c.coluna_origem) == F.lit(nome_origem_b))

            completo_a = vencedores_a.join(df_completo_a, on=chaves, how="inner")
            completo_b = vencedores_b.join(df_completo_b, on=chaves, how="inner")

            return completo_a.unionByName(completo_b, allowMissingColumns=True)

    # ==============================================================================
    # 4) BUILDER (GLUE CATALOG + JOINS COM N CHAVES)
    # ==============================================================================

    def _materializar_input(self, inp: InputDF) -> DataFrame:
        """Converte DataFrame | lista de DataFrames | OrigemSpec em um único DataFrame."""
        if isinstance(inp, DataFrame):
            return inp

        if isinstance(inp, list):
            if not inp:
                # Evita erro em pipeline: cria DF vazio com schema mínimo
                c = self.cfg
                return self.spark.createDataFrame([], schema=f"{c.coluna_numero_operacao} long, {c.coluna_codigo_versao_o}")
            df = inp[0]
            for d in inp[1:]:
                df = df.unionByName(d, allowMissingColumns=True)
            return df

        # OrigemSpec
        return self.self._montar_df_origem(inp)

    def _montar_df_origem(self, spec: OrigemSpec) -> DataFrame:
        """
        Monta DF lendo tabelas do Glue Catalog (spark.table) e aplicando joins em sequência.
        Suporta múltiplas chaves por join e múltiplos joins (oper->pos e pos->evt, etc).
        """

        # 1) Ler tabelas do Glue Catalog
        dfs = {alias: self.spark.table(tbl).alias(alias) for alias, tbl in spec.tables.items()}

        # 2) Se não houver join, precisa haver exatamente 1 tabela
        if not spec.joins:
            if len(dfs) != 1:
                raise ValueError("OrigemSpec sem joins deve ter exatamente 1 tabela.")
            df = next(iter(dfs.values()))
        else:
            # 3) Construir DF inicial a partir do left_alias do primeiro join
            first = spec.joins[0]
            df = dfs[first.left_alias]

            # 4) Aplicar joins na ordem
            for j in spec.joins:
                right = dfs[j.right_alias]

                # Normaliza chaves: se left col != right col, renomeia right para nome do left
                join_keys_left: List[str] = []
                right2 = right
                for left_col, right_col in j.on:
                    join_keys_left.append(left_col)
                    if left_col != right_col:
                        right2 = right2.withColumnRenamed(right_col, left_col)

                # Renomeia colisões (exceto join keys) para evitar ambiguous columns
                if j.rename_collisions:
                    prefix = j.collision_prefix or f"{j.right_alias}__"
                    left_cols = set(df.columns)
                    right_cols = set(right2.columns)
                    collisions = (left_cols & right_cols) - set(join_keys_left)
                    for colname in collisions:
                        right2 = right2.withColumnRenamed(colname, f"{prefix}{colname}")

                # Join com lista de chaves (Spark remove duplicidade das keys automaticamente)
                df = df.join(right2, on=join_keys_left, how=j.how)

        # 5) Filtro/partição opcional
        if spec.where_expr:
            df = df.where(spec.where_expr)

        # 6) Select final opcional
        if spec.select_cols:
            df = df.select(*spec.select_cols)
        
        return df
    
    # ==========================================================
    # 5) REGRA DE CONSOLIDAÇÃO (IGUAL AO SEU, SEM PERDER NEGÓCIO)
    # ==========================================================

    def _definir_ordem_padrao_ultima_ocorrencia(self) -> None:
        c = self.cfg
        if c.colunas_ordem_ultima_ocorrencia is None:
            c.colunas_ordem_ultima_ocorrencia = [
                c.coluna_data_recebimento_evento,
                c.coluna_data_valor_evento,
                c.coluna_prioridade_evento,
            ]

    def _normalizar_tipos_e_nulos(self, df: DataFrame) -> DataFrame:
        c = self.cfg

        df2 = (
            df.withColumn(c.coluna_data_valor_evento, F.to_timestamp(F.col(c.coluna_data_valor_evento)))
            .withColumn(c.coluna_data_recebimento_evento, F.to_timestamp(F.col(c.coluna_data_recebimento_evento)))
            .withColumn(c.coluna_prioridade_evento, F.col(c.coluna_prioridade_evento).cast("long"))
        )

        if c.nulos_como_minimo:
            ts_min = F.to_timestamp(F.lit(c.timestamp_minimo))
            pr_min = F.lit(c.prioridade_minima).cast("long")

            df2 = (
                df2.withColumn(c.coluna_data_valor_evento, F.coalesce(F.col(c.coluna_data_valor_evento), ts_min))
                .withColumn(c.coluna_data_recebimento_evento, F.coalesce(F.col(c.coluna_data_recebimento_evento), ts_min))
                .withColumn(c.coluna_prioridade_evento, F.coalesce(F.col(c.coluna_prioridade_evento), pr_min))
            )

        return df2
    
    def _marcar_origem(self, df: DataFrame, origem: str) -> DataFrame:
        return df.withColumn(self.cfg.coluna_origem, F.lit(origem))

    def _colunas_ordem_desc(self, nomes_colunas: Sequence[str]) -> List[Column]:
        return [F.col(nome).desc_nulls_last() for nome in nomes_colunas]

    def _selecionar_ultima_ocorrencia_por_origem(self, df: DataFrame) -> DataFrame:
        c = self.cfg
        ordem = self._colunas_ordem_desc(c.colunas_ordem_ultima_ocorrencia)
        janela = Window.partitionBy(c.coluna_numero_operacao).orderBy(*ordem)

        return (
            df.withColumn("_rn_ultima", F.row_number().over(janela))
            .filter(F.col("_rn_ultima") == 1)
            .drop("_rn_ultima")
        )

    def _expressao_preferencia_origem(self) -> Column:
        c = self.cfg
        return F.when(F.col(c.coluna_origem) == F.lit(c.origem_preferida_desempate), F.lit(1)).otherwise(F.lit(0))

    def _selecionar_vencedor_por_operacao(self, df_unificado: DataFrame) -> DataFrame:
        c = self.cfg
        pref_origem = self._expressao_preferencia_origem()

        janela = Window.partitionBy(c.coluna_numero_operacao).orderBy(
            F.col(c.coluna_data_valor_evento).desc(),
            F.col(c.coluna_prioridade_evento).desc(),
            F.col(c.coluna_data_recebimento_evento).desc(),
            pref_origem.desc(),
        )

        return (
            df_unificado.withColumn("_rn_vencedor", F.row_number().over(janela))
            .filter(F.col("_rn_vencedor") == 1)
            .drop("_rn_vencedor")
        )

    def _selecionar_colunas_retorno(self, df: DataFrame, colunas_retorno: List[str]) -> DataFrame:
        c = self.cfg
        if c.coluna_origem not in colunas_retorno:
            colunas_retorno = colunas_retorno + [c.coluna_origem]
        return df.select(*colunas_retorno)
