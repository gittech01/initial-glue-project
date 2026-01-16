import logging
from typing import Dict, Tuple, Optional

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from utils.business.base_processor import BaseBusinessProcessor

logger = logging.getLogger(__name__)


class NovaRegraConsolidacao(BaseBusinessProcessor):
    """
    Regra de consolidação totalmente dinâmica, seguindo o contrato da arquitetura
    via BaseBusinessProcessor.

    Regras:
      1) Lê sempre a ÚLTIMA partição das tabelas principais (SoR e SoT).
      2) Se as partições forem DIFERENTES -> ingestão direta da maior partição.
      3) Se forem IGUAIS:
         - Se houver auxiliares -> aplica ranking (data, prioridade, recência).
         - Se NÃO houver auxiliares -> ingestão direta.
      4) Escreve no S3 e atualiza o Glue Data Catalog.

    Nada é hardcoded: nomes de tabelas, chaves e campos vêm de configuração.
    """

    PARTITION_KEY = "anomesdia"

    # ------------------------------------------------------------------
    # Hook: leitura (BaseBusinessProcessor)
    # ------------------------------------------------------------------
    def _read_data(self, **kwargs) -> Dict:
        """
        Lê os datasets necessários:
          - Tabelas principais (SoR e SoT) na última partição
          - Auxiliares (se existirem), também na última partição
        """
        database = kwargs["database"]
        tabela_consolidada = kwargs["tabela_consolidada"]

        cfg = self.config.CONSOLIDACOES[tabela_consolidada]

        tbl_sor = cfg["principais"]["sor"]
        tbl_sot = cfg["principais"]["sot"]
        aux_cfg = cfg.get("auxiliares", {})

        # Principais
        df_sor_proc, part_sor = self._read_latest_partition(database, tbl_sor)
        df_sot_proc, part_sot = self._read_latest_partition(database, tbl_sot)

        data = {
            "database": database,
            "tabela_consolidada": tabela_consolidada,
            "cfg": cfg,
            "df_sor_proc": df_sor_proc,
            "df_sot_proc": df_sot_proc,
            "part_sor": part_sor,
            "part_sot": part_sot,
            "aux_dfs": {}
        }

        # Auxiliares (opcionais)
        if aux_cfg:
            for camada in ["sor", "sot"]:
                if camada in aux_cfg:
                    data["aux_dfs"][camada] = {}
                    for alias, table_name in aux_cfg[camada].items():
                        df_aux, _ = self._read_latest_partition(database, table_name)
                        data["aux_dfs"][camada][alias] = df_aux

        logger.info(
            f"[{tabela_consolidada}] Leitura concluída. "
            f"Partições: SoR={part_sor}, SoT={part_sot}"
        )

        return data

    # ------------------------------------------------------------------
    # Hook: transformação (BaseBusinessProcessor)
    # ------------------------------------------------------------------
    def _transform_data(self, data: Dict, **kwargs) -> Dict:
        """
        Aplica a regra de negócio:
          - Partições diferentes -> ingestão direta
          - Partições iguais:
              * sem auxiliares -> ingestão direta
              * com auxiliares -> ranking e filtro das principais
        """
        cfg = data["cfg"]
        tabela_consolidada = data["tabela_consolidada"]

        df_sor_proc = data["df_sor_proc"]
        df_sot_proc = data["df_sot_proc"]
        part_sor = data["part_sor"]
        part_sot = data["part_sot"]

        chaves = cfg["chaves_principais"]
        campos_decisao = cfg.get("campos_decisao", [])
        aux_cfg = cfg.get("auxiliares", {})

        # 1) Partições diferentes -> ingestão direta da maior
        if part_sor != part_sot:
            if part_sor > part_sot:
                df_final = df_sor_proc
                origem = "online"
                part_escolhida = part_sor
            else:
                df_final = df_sot_proc
                origem = "batch"
                part_escolhida = part_sot

            logger.info(
                f"[{tabela_consolidada}] Partições diferentes. "
                f"Ingestão direta ({origem}) - partição {part_escolhida}"
            )

            return {
                "df_final": df_final,
                "particao": part_escolhida,
                "modo": "INGESTAO_DIRETA",
                "origem_escolhida": origem
            }

        # 2) Partições iguais e SEM auxiliares -> ingestão direta
        if not aux_cfg:
            logger.info(
                f"[{tabela_consolidada}] Sem auxiliares configuradas. "
                f"Ingestão direta (online) - partição {part_sor}"
            )

            return {
                "df_final": df_sor_proc,
                "particao": part_sor,
                "modo": "INGESTAO_DIRETA_SEM_AUX",
                "origem_escolhida": "online"
            }

        # 3) Partições iguais e COM auxiliares -> ranking
        logger.info(f"[{tabela_consolidada}] Aplicando regra de ranking.")

        df_sor_rank = self._build_rank_dataset(
            aux_dfs=data["aux_dfs"]["sor"],
            origem="online",
            campos_decisao=campos_decisao
        )

        df_sot_rank = self._build_rank_dataset(
            aux_dfs=data["aux_dfs"]["sot"],
            origem="batch",
            campos_decisao=campos_decisao
        )

        union_df = df_sor_rank.unionByName(df_sot_rank)
        filtro_origem = self._apply_ranking(union_df, campos_decisao)

        df_consolidado = self._filter_principais(
            filtro_origem=filtro_origem,
            df_sor_proc=df_sor_proc,
            df_sot_proc=df_sot_proc,
            chaves=chaves
        )

        return {
            "df_final": df_consolidado,
            "particao": part_sor,
            "modo": "CONSOLIDACAO",
            "origem_escolhida": "ranking"
        }

    # ------------------------------------------------------------------
    # Hook: escrita (BaseBusinessProcessor)
    # ------------------------------------------------------------------
    def _write_data(self, result: Dict, **kwargs) -> Dict:
        """
        Persiste no S3 e atualiza o Glue Data Catalog.
        """
        database = kwargs["database"]
        tabela_consolidada = kwargs["tabela_consolidada"]

        df_final = result["df_final"]
        particao = result["particao"]

        self.glue_handler.write_to_s3_and_catalog(
            df=df_final,
            database=database,
            table_name=tabela_consolidada,
            partition_key=self.PARTITION_KEY,
            partition_value=particao
        )

        logger.info(
            f"[{tabela_consolidada}] Escrita concluída no S3 e catálogo atualizado "
            f"(partição {particao})"
        )

        return {
            "status": "SUCCESS",
            "tabela": tabela_consolidada,
            "modo": result.get("modo"),
            "origem_escolhida": result.get("origem_escolhida"),
            "particao": particao
        }

    # ==================================================================
    # Métodos auxiliares internos
    # ==================================================================

    def _read_latest_partition(self, database: str, table_name: str) -> Tuple[DataFrame, str]:
        """
        Lê a última partição (maior valor) via Glue Catalog.
        """
        last_partition = self.glue_handler.get_last_partition(database, table_name)
        if not last_partition:
            raise RuntimeError(f"Nenhuma partição encontrada para {database}.{table_name}")

        df = self.glue_handler.read_from_catalog(
            database=database,
            table_name=table_name,
            push_down_predicate=f"{self.PARTITION_KEY}='{last_partition}'"
        )
        return df, last_partition

    def _build_rank_dataset(
        self,
        aux_dfs: Dict[str, DataFrame],
        origem: str,
        campos_decisao: list
    ) -> DataFrame:
        """
        Monta dinamicamente o dataset de ranking a partir das auxiliares.
        Espera um dicionário de DataFrames com aliases (ex.: oper, event, posi).
        """

        # 'oper' é a base
        if "oper" not in aux_dfs:
            raise ValueError("Configuração inválida: auxiliar base 'oper' é obrigatória")

        dfs = {k: v.alias(k) for k, v in aux_dfs.items()}
        df_join = dfs["oper"]

        # Aplica joins por alias conhecido (padrão do seu domínio)
        for alias, df in dfs.items():
            if alias == "oper":
                continue

            if alias == "event":
                df_join = df_join.join(
                    df,
                    (F.col("oper.num_oper") == F.col("event.num_oper")) &
                    (F.col("oper.cod_idef_ver_oper") == F.col("event.cod_idef_ver_oper")),
                    "inner"
                )
            elif alias == "posi":
                df_join = df_join.join(
                    df,
                    (F.col("oper.num_oper") == F.col("posi.num_oper")) &
                    (F.col("oper.cod_idef_even_prcs") == F.col("posi.cod_idef_even_prcs")),
                    "inner"
                )
            else:
                raise ValueError(f"Alias auxiliar não reconhecido: {alias}")

        # Seleção: chaves + campos de decisão + origem
        select_cols = [
            F.col("oper.num_oper").alias("num_oper"),
            F.col("oper.cod_idef_ver_oper").alias("cod_idef_ver_oper"),
        ]

        for campo in campos_decisao:
            select_cols.append(F.col(f"posi.{campo}").alias(campo))

        select_cols.append(F.lit(origem).alias("origem"))

        return df_join.select(*select_cols)

    def _apply_ranking(self, df_union: DataFrame, campos_decisao: list) -> DataFrame:
        order_expr = [F.col(c).desc() for c in campos_decisao]

        window = Window.partitionBy(
            "num_oper", "cod_idef_ver_oper"
        ).orderBy(*order_expr)

        ranked = df_union.withColumn("rank", F.row_number().over(window))
        return ranked.filter(F.col("rank") == 1)

    def _filter_principais(
        self,
        filtro_origem: DataFrame,
        df_sor_proc: DataFrame,
        df_sot_proc: DataFrame,
        chaves: list
    ) -> DataFrame:
        """
        Filtra as tabelas principais (schemas idênticos) conforme a origem vencedora.
        """
        cond_sor = (F.col("f.origem") == F.lit("online"))
        cond_sot = (F.col("f.origem") == F.lit("batch"))

        for k in chaves:
            cond_sor = cond_sor & (F.col(f"f.{k}") == F.col(f"sor.{k}"))
            cond_sot = cond_sot & (F.col(f"f.{k}") == F.col(f"sot.{k}"))

        joined = (
            filtro_origem.alias("f")
            .join(df_sor_proc.alias("sor"), cond_sor, "left")
            .join(df_sot_proc.alias("sot"), cond_sot, "left")
        )

        # Retorna a linha vencedora (schemas idênticos)
        return joined.select(F.coalesce(F.col("sor.*"), F.col("sot.*")))
