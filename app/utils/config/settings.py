"""
Configurações da aplicação.

Centraliza todas as configurações da aplicação, permitindo fácil
customização e manutenção.
"""

from typing import Dict, Any


class AppConfig:
    """
    Classe de configuração da aplicação.
    
    Carrega configurações de variáveis de ambiente ou usa valores padrão.
    Suporta carregamento de configurações de consolidação do settings.py.
    """
    
    def __init__(self):
        """
        Inicializa configurações a partir de variáveis de necessárias para o funcionamento da aplicação.
        """
        
        # AWS Configuration
        self.aws_region: str = 'sa-east-1'
        
        # DynamoDB Tables
        # Apenas journey_table_name é usado para controle de jornada (idempotência e retry)
        # Os dados consolidados são salvos diretamente no S3 e Glue Catalog
        self.journey_table_name: str = 'journey_control'
        
        # Glue Configuration
        self.default_database_output: str = 'default_database'
        self.default_output_format: str = 'parquet'
        self.default_compression: str = 'snappy'  # Compressão padrão para Parquet
        
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
                    "sor": {
                        "database": "db_sor",
                        "table": "tbl_processado_operacao_sor_n1"
                    },
                    "sot": {
                        "database": "db_sot",
                        "table": "tbl_processado_operacao_apropriada_n1"
                    }
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
                    "sor": {
                        "database": "db_sor",
                        "table": "tbl_processado_operacao_sor_n2"
                    },
                    "sot": {
                        "database": "db_sot",
                        "table": "tbl_processado_operacao_apropriada_n2"
                    }
                },
                "auxiliares": {
                    "sor": {
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
                            "left": "event",
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
                "chaves_principais": ["num_oper", "cod_idef_even_prcs"], 
                "campos_decisao": ["dat_vlr_even_oper", "num_prio_even_oper", "dat_recm_even_oper"]
            },
            "tbl_processado_operacao_consolidada_n3": {
                "principais": {
                    "sor": {
                        "database": "db_sor",
                        "table": "tbl_processado_operacao_sor_n3"
                    },
                    "sot": {
                        "database": "db_sot",
                        "table": "tbl_processado_operacao_apropriada_n3"
                    }
                },
                "auxiliares": {},
                "joins_auxiliares": {},
                "chaves_principais": [],
                "campos_decisao": ["dat_vlr_even_oper", "num_prio_even_oper", "dat_recm_even_oper"]
            },

            # Aqui há uma substituição direta da
            "tbl_processado_operacao_consolidada_n4": {
                "principais": {

                    "sor": { "database": "db_sor", "table": "tbl_processado_operacao_sor_n4" }
                },
                "auxiliares": {},
                "joins_auxiliares": {},
                "chaves_principais": [],
                "campos_decisao": []
            }
        }
    
    def __repr__(self) -> str:
        """Representação string da configuração."""
        return (
            f"AppConfig("
            f"region={self.aws_region}, "
            f"journey_table={self.journey_table_name}"
            f")"
        )
            

