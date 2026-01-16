"""
Entry Point da aplicação AWS Glue - Agnóstico para múltiplas regras de negócio.

Este módulo é o ponto de entrada principal da aplicação, responsável por
inicializar o contexto do Glue e orquestrar a execução dos processos de negócio.

Design Patterns aplicados:
- Factory: Cria processadores através de ProcessorFactory
- Strategy: Diferentes regras de negócio são estratégias diferentes
- Orchestrator: BusinessRuleOrchestrator coordena execuções
- Dependency Injection: Todas as dependências são injetadas
"""
import sys
import logging
from typing import Optional, Dict, Any

try:
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
except ImportError:
    # Fallback para ambiente local/testes
    def getResolvedOptions(args, options):
        return {opt: f"mock_{opt}" for opt in options}
    class GlueContext: pass
    class Job: pass
    class SparkContext: pass

    logging.warning("Bibliotecas AWS Glue não encontradas. Modo de desenvolvimento ativado.")

from utils.config.settings import AppConfig
from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.business.nova_regra_consolidacao import NovaRegraConsolidacao
from utils.business.processor_factory import ProcessorFactory
from utils.business.orchestrator import BusinessRuleOrchestrator

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def initialize_glue_context() -> tuple:
    """
    Inicializa o contexto do AWS Glue.

    Returns:
        Tupla (sc, glue_context, job, args)
    """
    try:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        job = Job(glue_context)

        # Obter argumentos do job
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'processor_type',  # Tipo de processador (ex: 'data_processor', 'sales_analyzer')
            'database',
            'table_name',
            'output_path',
            'periodo',  # Opcional, para sales_analyzer
            'continue_on_error'  # Opcional, padrão True
        ])

        job.init(args['JOB_NAME'], args)

        logger.info(f"Contexto Glue inicializado para job: {args['JOB_NAME']}")
        return sc, glue_context, job, args

    except Exception as e:
        logger.error(f"Erro ao inicializar contexto Glue: {e}")
        raise

def main():
    sc, glue_context, job, args = initialize_glue_context()

    config = AppConfig()

    glue_handler = GlueDataHandler(glue_context)
    dynamodb_handler = DynamoDBHandler(
        table_name=config.congregado_table_name,
        region_name=config.aws_region
    )
    journey_controller = JourneyController(
        table_name=config.journey_table_name,
        region_name=config.aws_region
    )

    regra = NovaRegraConsolidacao(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )

    resultados = {}

    # 🔹 EXECUTA UMA VEZ PARA CADA TABELA CONSOLIDADA
    for tabela_consolidada in config.CONSOLIDACOES_CONFIG.regras.keys():

        idempotency_key = (
            f"{args['JOB_NAME']}"
            f"::{tabela_consolidada}"
            f"::{args['execution_date']}"
        )

        resultado = journey_controller.execute_with_journey(
            regra.processar,
            idempotency_key=idempotency_key,
            database=args["database"],
            tabela_consolidada=tabela_consolidada
        )

        resultados[tabela_consolidada] = resultado

    job.commit()

    return resultados




## Settings:
CONSOLIDACOES = {
    "tbl_processado_operacao_consolidada": {

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
    "tbl_outra_consolidada": {
        "principais": {
            "sor": "tbl_outra_sor",
            "sot": "tbl_outra_sot"
        },
        "auxiliares": {},
        "joins_auxiliares": {},
        "chaves_principais": ["id_registro"],
        "campos_decisao": []
    }
}
