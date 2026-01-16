from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.business.base_business_processor import BaseBusinessProcessor
from utils.journey_controller import JourneyController
from utils.handlers.glue_handler import GlueDataHandler
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig


class NovaRegraConsolidacao(BaseBusinessProcessor):
    """
    Regra de consolidação entre SoR (online) e SoT (batch),
    escolhendo o registro vencedor com base em:
      - dat_vlr_even_oper DESC
      - num_prio_even_oper DESC
      - dat_recm_even_oper DESC

    Totalmente dirigida por settings.py
    """

    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ):
        self.glue = glue_handler
        self.journey = journey_controller
        self.dynamodb = dynamodb_handler
        self.config = config

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------
    def _read_data(
        self,
        database: str,
        tabela_consolidada: str,
        **_
    ) -> Dict[str, DataFrame]:

        regra_cfg = self.config.CONSOLIDACOES[tabela_consolidada]

        def read_origem(origem_cfg: Dict[str, Any]) -> DataFrame:
            tabela_principal = origem_cfg["tabela_principal"]
            auxiliares = origem_cfg.get("auxiliares", [])

            # Descobrir última partição da tabela principal
            particao = self.glue.get_latest_partition(
                database=database,
                table_name=tabela_principal
            )

            filtro = f"anomesdia = '{particao}'"

            df_base = self.glue.read_from_catalog(
                database=database,
                table_name=tabela_principal,
                filter=filtro
            )

            # Aplicar joins auxiliares dinamicamente
            for aux in auxiliares:
                df_aux = self.glue.read_from_catalog(
                    database=database,
                    table_name=aux["tabela"]
                )

                condicoes = [
                    df_base[c["left"]] == df_aux[c["right"]]
                    for c in aux["join_keys"]
                ]

                df_base = df_base.join(
                    df_aux,
                    on=condicoes,
                    how=aux.get("join_type", "inner")
                )

            return df_base

        return {
            "sor": read_origem(regra_cfg["sor"]),
            "sot": read_origem(regra_cfg["sot"])
        }

    # ------------------------------------------------------------------
    # TRANSFORM
    # ------------------------------------------------------------------
    def _transform_data(
        self,
        data: Dict[str, DataFrame],
        tabela_consolidada: str,
        **_
    ) -> DataFrame:

        regra_cfg = self.config.CONSOLIDACOES[tabela_consolidada]

        colunas_ordem = regra_cfg["ordenacao"]
        chaves_negocio = regra_cfg["chaves_negocio"]

        df_sor = data["sor"].withColumn("origem", F.lit("online"))
        df_sot = data["sot"].withColumn("origem", F.lit("batch"))

        df_union = df_sor.unionByName(df_sot)

        window_spec = Window.partitionBy(
            *[F.col(c) for c in chaves_negocio]
        ).orderBy(
            *[F.col(c).desc() for c in colunas_ordem]
        )

        df_ranked = (
            df_union
            .withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") == 1)
            .drop("rank")
        )

        return df_ranked

    # ------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------
    def _write_data(
        self,
        df: DataFrame,
        database: str,
        tabela_consolidada: str,
        **_
    ) -> None:

        self.glue.write_to_catalog(
            df=df,
            database=database,
            table_name=tabela_consolidada
        )

    # ------------------------------------------------------------------
    # PUBLIC ENTRYPOINT
    # ------------------------------------------------------------------
    def processar(self, **kwargs):
        """
        Entry point padrão exigido pela arquitetura.
        """
        return super().processar(**kwargs)

    def get_processor_name(self) -> str:
        return "NovaRegraConsolidacao"
