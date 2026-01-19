"""
Configurações da aplicação.

Centraliza todas as configurações da aplicação, permitindo fácil
customização e manutenção.
"""
import os
from typing import Optional, Dict, Any


class AppConfig:
    """
    Classe de configuração da aplicação.
    
    Carrega configurações de variáveis de ambiente ou usa valores padrão.
    Suporta carregamento de configurações de consolidação do settings.py.
    """
    
    def __init__(self, settings_path: Optional[str] = None):
        """
        Inicializa configurações a partir de variáveis de ambiente.
        
        Args:
            settings_path: Caminho opcional para arquivo settings.py com CONSOLIDACOES
        """
        
        # AWS Configuration
        self.aws_region: str = 'sa-east-1'
        
        # DynamoDB Tables
        self.journey_table_name: str = 'journey_control'
        self.congregado_table_name: str = 'congregado_data'
        
        # Glue Configuration
        self.default_database: str = 'default_database'
        self.default_output_format: str = 'parquet'
        
        # Retry Configuration
        self.max_retries: int = 3
        self.retry_delay: int = 2
        
        # Logging Configuration
        self.log_level: str = 'INFO'
        
        # Processing Configuration
        self.batch_size: int = 1000
        self.enable_partitioning: bool = True

        # Definition tables cores:
        # Tables of consolidation
        self.tbu2_ = ...
        
        # Consolidações Configuration (carregado do settings.py se disponível)
        self.consolidacoes_tabelas: Dict[str, Any] = {
            
            "tbl_processado_operacao_consolidada_n1": {
                "principais": {
                    "sor": "tbl_processado_operacao_sor_n1",
                    "sot": "tbl_processado_operacao_apropriada_n1"
                },
                "auxiliares": {
                    "sor": {
                        "oper": "tbl_operecao_sor_n1",
                        "event": "tbl_evento_processado_sor_n1",
                        "posi": "tbl_posicao_operacao_sor_n1"
                    },
                    "sot": {
                        "oper": "tbl_operecao_apropriada_n1",
                        "event": "tbl_evento_processado_apropriada_n1",
                        "posi": "tbl_posicao_operacao_apropriada_n1"
                    }
                },
                "joins_auxiliares": {
                    "sor": [
                        {
                            "left": "oper",
                            "right": "event",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_ver_oper", "cod_idef_ver_oper")
                            ]
                        },
                        {
                            "left": "oper",
                            "right": "posi",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_even_prcs", "cod_idef_even_prcs")
                            ]
                        }
                    ],
                    "sot": [
                        {
                            "left": "oper",
                            "right": "event",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_ver_oper", "cod_idef_ver_oper")
                            ]
                        },
                        {
                            "left": "oper",
                            "right": "posi",
                            "on": [
                                ("num_oper", "num_oper"),   
                                ("cod_idef_even_prcs", "cod_idef_even_prcs")
                            ]
                        }
                    ]
                },
                "chaves_principais": ["num_oper", "cod_idef_ver_oper"],
                "campos_decisao": ["dat_vlr_even_oper", "num_prio_even_oper", "dat_recm_even_oper"]
            },
            "tbl_processado_operacao_consolidada_n2": {
                "principais": {
                    "sor": "tbl_processado_operacao_sor_n2",
                    "sot": "tbl_processado_operacao_apropriada_n2"
                },
                "auxiliares": {
                    "sor": {
                        "oper": "tbl_operecao_sor_n2",
                        "event": "tbl_evento_processado_sor_n2",
                        "posi": "tbl_posicao_operacao_sor_n2"
                    },
                    "sot": {
                        "oper": "tbl_operecao_apropriada_n2",
                        "event": "tbl_evento_processado_apropriada_n2",
                        "posi": "tbl_posicao_operacao_apropriada_n2"
                    }
                },
                "joins_auxiliares": {
                    "sor": [
                        {
                            "left": "oper",
                            "right": "event",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_ver_oper", "cod_idef_ver_oper")
                            ]
                        },
                        {
                            "left": "oper",
                            "right": "posi",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_even_prcs", "cod_idef_even_prcs")
                            ]
                        }
                    ],
                    "sot": [
                        {
                            "left": "oper",
                            "right": "event",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_ver_oper", "cod_idef_ver_oper")
                            ]
                        },
                        {
                            "left": "oper",
                            "right": "posi",
                            "on": [
                                ("num_oper", "num_oper"),
                                ("cod_idef_even_prcs", "cod_idef_even_prcs")
                            ]
                        }
                    ]
                },
                "chaves_principais": ["num_oper", "cod_idef_ver_oper"], 
                "campos_decisao": ["dat_vlr_even_oper", "num_prio_even_oper", "dat_recm_even_oper"]
            },
            "tbl_processado_operacao_consolidada_n3": {
                "principais": {
                    "sor": "tbl_processado_operacao_sor_n3",
                    "sot": "tbl_processado_operacao_apropriada_n3"
                },
                "auxiliares": {},
                "joins_auxiliares": {},
                "chaves_principais": [],
                "campos_decisao": []
            },
            "tbl_processado_operacao_consolidada_n4": {
                "principais": {
                    "sor": "tbl_processado_operacao_sor_n4",
                    "sot": "tbl_processado_operacao_apropriada_n4"
                },
                "auxiliares": {},
                "joins_auxiliares": {},
                "chaves_principais": [],
                "campos_decisao": []
            },
            "tbl_processado_operacao_consolidada_n5": {
                "principais": {
                    "sor": "tbl_processado_operacao_sor_n5"
                },
                "auxiliares": {},
                "joins_auxiliares": {},
                "chaves_principais": [],
                "campos_decisao": []
            }
        }
            

