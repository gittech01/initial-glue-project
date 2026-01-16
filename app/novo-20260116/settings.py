## Settings:
CONSOLIDACOES = {
    "tbl_outra_consolidada_n1": {

        # Tabelas com mesmo schema
        "principais": {
            "sor": "tbl_processado_operacao_sor",
            "sot": "tbl_processado_operacao_apropriada"
        },

        # Auxiliares (opcionais)
        "auxiliares": {
            "sor": {
                "oper": "tbl_operecao_sor",
                "event": "tbl_evento_processado_sor",
                "posi": "tbl_posicao_operacao_sor"
            },
            "sot": {
                "oper": "tbl_operecao_apropriada",
                "event": "tbl_evento_processado_apropriada",
                "posi": "tbl_posicao_operacao_apropriada"
            }
        },

        # 🔹 JOINS DINÂMICOS ENTRE AUXILIARES
        "joins_auxiliares": {
            "sor": [
                {
                    "left": "oper",
                    "right": "event",
                    "on": [
                        ["num_oper", "num_oper"],
                        ["cod_idef_ver_oper", "cod_idef_ver_oper"]
                    ]
                },
                {
                    "left": "oper",
                    "right": "posi",
                    "on": [
                        ["num_oper", "num_oper"],
                        ["cod_idef_even_prcs", "cod_idef_even_prcs"]
                    ]
                }
            ],
            "sot": [
                {
                    "left": "oper",
                    "right": "event",
                    "on": [
                        ["num_oper", "num_oper"],
                        ["cod_idef_ver_oper", "cod_idef_ver_oper"]
                    ]
                },
                {
                    "left": "oper",
                    "right": "posi",
                    "on": [
                        ["num_oper", "num_oper"],
                        ["cod_idef_even_prcs", "cod_idef_even_prcs"]
                    ]
                }
            ]
        },
        # Chaves para comparar/filtrar nas principais
        "chaves_principais": ["num_oper", "cod_idef_ver_oper"],

        # Campos usados no ranking
        "campos_decisao": [
            "dat_vlr_even_oper",
            "num_prio_even_oper",
            "dat_recm_even_oper"
        ]
    },

    # Exemplo: consolidação SEM auxiliares
    "tbl_outra_consolidada_n2": {
        "principais": {
            "sor": "tbl_outra_sor",
            "sot": "tbl_outra_sot"
        },
        "auxiliares": {},
        "joins_auxiliares": {},
        "chaves_principais": ["num_oper", "cod_idef_even_prcs"],
        # já contem os campos de tomada de decisão
        "campos_decisao": [
            "dat_vlr_even_oper",
            "num_prio_even_oper",
            "dat_recm_even_oper"
        ]
    },
    
    # Exemplo: consolidação SOMENTE CONTENDO UMA PRINCIPAL
    # PARA ESSE CASO NÃO PRECISA DE CRUZAMENTO
    "tbl_outra_consolidada_n3": {
        "principais": {
            "sor": "tbl_outra_sor",
        },
        "auxiliares": {},
        "joins_auxiliares": {},
        "chaves_principais": [],
        # já contem os campos de tomada de decisão
        "campos_decisao": []
    }
}




# utils/business/nova_regra_consolidacao.py

from pyspark.sql import functions as F, Window
from typing import Dict
from utils.business.base_business_processor import BaseBusinessProcessor


class NovaRegraConsolidacao(BaseBusinessProcessor):

    def get_processor_name(self) -> str:
        return "nova_regra_consolidacao"

    def processar(
        self,
        database: str,
        tabela_consolidada: str
    ) -> Dict:

        cfg = self.config.CONSOLIDACOES[tabela_consolidada]

        resultado_origens = []

        for origem in ["sor", "sot"]:

            if origem not in cfg["principais"]:
                continue

            tabela_principal = cfg["principais"][origem]

            part = self.glue_handler.get_latest_partition(
                database=database,
                table_name=tabela_principal
            )

            if not part:
                continue

            # ----------------------------
            # BASE AUXILIAR (se existir)
            # ----------------------------
            if cfg.get("auxiliares", {}).get(origem):

                dfs_aux = {}

                for alias, tabela_aux in cfg["auxiliares"][origem].items():
                    dfs_aux[alias] = self.glue_handler.read_from_catalog(
                        database,
                        tabela_aux,
                        push_down_predicate=f"anomesdia='{part}'"
                    )

                base_df = None
                for join_cfg in cfg["joins_auxiliares"][origem]:
                    left = join_cfg["left"]
                    right = join_cfg["right"]

                    cond = [
                        dfs_aux[left][l] == dfs_aux[right][r]
                        for l, r in join_cfg["on"]
                    ]

                    if base_df is None:
                        base_df = dfs_aux[left].join(dfs_aux[right], cond, "inner")
                    else:
                        base_df = base_df.join(dfs_aux[right], cond, "inner")

            else:
                base_df = self.glue_handler.read_from_catalog(
                    database,
                    tabela_principal,
                    push_down_predicate=f"anomesdia='{part}'"
                )

            # ----------------------------
            # SELEÇÃO CAMPOS DE NEGÓCIO
            # ----------------------------
            select_cols = cfg["chaves_principais"] + cfg["campos_decisao"]

            base_df = base_df.select(*select_cols).withColumn(
                "origem", F.lit(origem)
            )

            resultado_origens.append(base_df)

        # ----------------------------
        # UNION SOR + SOT
        # ----------------------------
        union_df = resultado_origens[0]
        for df in resultado_origens[1:]:
            union_df = union_df.unionByName(df)

        # ----------------------------
        # RANKING DE NEGÓCIO
        # ----------------------------
        window = Window.partitionBy(
            *cfg["chaves_principais"]
        ).orderBy(
            *[F.col(c).desc() for c in cfg["campos_decisao"]]
        )

        ranked_df = union_df.withColumn(
            "rank", F.row_number().over(window)
        ).filter("rank = 1").drop("rank")

        # ----------------------------
        # LEITURA BASE FINAL
        # ----------------------------
        df_sor = self.glue_handler.read_from_catalog(
            database,
            cfg["principais"].get("sor"),
            push_down_predicate=f"anomesdia='{part}'"
        )

        df_sot = self.glue_handler.read_from_catalog(
            database,
            cfg["principais"].get("sot"),
            push_down_predicate=f"anomesdia='{part}'"
        )

        final_df = (
            df_sor.join(
                ranked_df.filter("origem = 'sor'"),
                cfg["chaves_principais"],
                "inner"
            )
            .unionByName(
                df_sot.join(
                    ranked_df.filter("origem = 'sot'"),
                    cfg["chaves_principais"],
                    "inner"
                )
            )
        )

        # ----------------------------
        # ESCRITA FINAL
        # ----------------------------
        self.glue_handler.write_to_catalog(
            final_df,
            database=database,
            table_name=tabela_consolidada
        )

        return {
            "tabela": tabela_consolidada,
            "status": "OK",
            "particao_usada": part
        }


