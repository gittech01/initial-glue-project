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




