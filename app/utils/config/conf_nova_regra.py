from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional


@dataclass
class JoinConfig:
    """
    Representa um join dinâmico entre duas tabelas auxiliares.
    Exemplo:
        left = "oper"
        right = "event"
        on = [("num_oper", "num_oper"), ("cod_idef_ver_oper", "cod_idef_ver_oper")]
    """
    left: str
    right: str
    on: List[Tuple[str, str]]


@dataclass
class AuxiliaresConfig:
    """
    Tabelas auxiliares de uma camada (SoR ou SoT).
    Exemplo:
        {
            "oper": "tbl_operecao_sor",
            "event": "tbl_evento_processado_sor",
            "posi": "tbl_posicao_operacao_sor"
        }
    """
    tabelas: Dict[str, str] = field(default_factory=dict)


@dataclass
class CamadaConfig:
    """
    Configuração de uma camada (SoR ou SoT):
      - Tabela principal
      - Tabelas auxiliares
      - Joins entre auxiliares
    """
    principal: str
    auxiliares: AuxiliaresConfig = field(default_factory=AuxiliaresConfig)
    joins_auxiliares: List[JoinConfig] = field(default_factory=list)


@dataclass
class ConsolidacaoConfig:
    """
    Configuração de uma regra de consolidação.
    """
    sor: CamadaConfig
    sot: CamadaConfig

    # Chaves para comparar registros entre as tabelas principais
    chaves_principais: List[str]

    # Campos usados no ranking (pode ser vazio)
    campos_decisao: List[str] = field(default_factory=list)


@dataclass
class ConsolidacoesConfig:
    """
    Container para múltiplas consolidações.
    Chave: nome da tabela consolidada
    Valor: ConsolidacaoConfig
    """
    regras: Dict[str, ConsolidacaoConfig] = field(default_factory=dict)


# Exemplo:

CONSOLIDACOES_CONFIG = ConsolidacoesConfig(
    regras={
        "tbl_processado_operacao_consolidada": ConsolidacaoConfig(
            sor=CamadaConfig(
                principal="tbl_processado_operacao_sor",
                auxiliares=AuxiliaresConfig(
                    tabelas={
                        "oper": "tbl_operecao_sor",
                        "event": "tbl_evento_processado_sor",
                        "posi": "tbl_posicao_operacao_sor"
                    }
                ),
                joins_auxiliares=[
                    JoinConfig(
                        left="oper",
                        right="event",
                        on=[
                            ("num_oper", "num_oper"),
                            ("cod_idef_ver_oper", "cod_idef_ver_oper")
                        ]
                    ),
                    JoinConfig(
                        left="oper",
                        right="posi",
                        on=[
                            ("num_oper", "num_oper"),
                            ("cod_idef_even_prcs", "cod_idef_even_prcs")
                        ]
                    )
                ]
            ),
            sot=CamadaConfig(
                principal="tbl_processado_operacao_apropriada",
                auxiliares=AuxiliaresConfig(
                    tabelas={
                        "oper": "tbl_operecao_apropriada",
                        "event": "tbl_evento_processado_apropriada",
                        "posi": "tbl_posicao_operacao_apropriada"
                    }
                ),
                joins_auxiliares=[
                    JoinConfig(
                        left="oper",
                        right="event",
                        on=[
                            ("num_oper", "num_oper"),
                            ("cod_idef_ver_oper", "cod_idef_ver_oper")
                        ]
                    ),
                    JoinConfig(
                        left="oper",
                        right="posi",
                        on=[
                            ("num_oper", "num_oper"),
                            ("cod_idef_even_prcs", "cod_idef_even_prcs")
                        ]
                    )
                ]
            ),
            chaves_principais=["num_oper", "cod_idef_ver_oper"],
            campos_decisao=[
                "dat_vlr_even_oper",
                "num_prio_even_oper",
                "dat_recm_even_oper"
            ]
        ),

        # Exemplo: consolidação sem auxiliares
        "tbl_outra_consolidada": ConsolidacaoConfig(
            sor=CamadaConfig(
                principal="tbl_outra_sor"
            ),
            sot=CamadaConfig(
                principal="tbl_outra_sot"
            ),
            chaves_principais=["id_registro"],
            campos_decisao=[]
        )
    }
)
