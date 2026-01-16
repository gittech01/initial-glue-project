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


    class GlueContext:
        pass


    class Job:
        pass


    class SparkContext:
        pass


    logging.warning("Bibliotecas AWS Glue não encontradas. Modo de desenvolvimento ativado.")

from utils.config.settings import AppConfig
from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
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
            'processor_type',  # Tipo de processador (ex: 'nova_regra_consolidacao')
            'database',
            'execution_date',  # Data de referência (ex: 20260115)
            'continue_on_error'  # Opcional, padrão True
        ])

        job.init(args['JOB_NAME'], args)

        logger.info(f"Contexto Glue inicializado para job: {args['JOB_NAME']}")
        return sc, glue_context, job, args

    except Exception as e:
        logger.error(f"Erro ao inicializar contexto Glue: {e}")
        raise


def main():
    """
    Função principal que orquestra a execução do job.

    Executa TODAS as tabelas definidas em CONSOLIDACOES com:
    - Idempotência por tabela
    - Tolerância a falhas
    - Registro de jornada no DynamoDB
    """
    try:
        # ---------------------------------------------------------------------
        # Inicializar contexto Glue
        sc, glue_context, job, args = initialize_glue_context()

        # Carregar configurações
        config = AppConfig()
        logger.info("Configurações carregadas")

        # Inicializar handlers
        glue_handler = GlueDataHandler(glue_context)
        journey_controller = JourneyController(
            table_name=config.journey_table_name,
            region_name=config.aws_region
        )
        dynamodb_handler = DynamoDBHandler(
            table_name=config.congregado_table_name,
            region_name=config.aws_region
        )

        # Criar orquestrador (não interrompe fluxo em caso de falha)
        continue_on_error = args.get('continue_on_error', 'true').lower() == 'true'
        orchestrator = BusinessRuleOrchestrator(
            journey_controller=journey_controller,
            continue_on_error=continue_on_error
        )
        # ----------------------------------------------------------------------

        # Tipo de processador (sua nova regra)
        processor_type = args.get('processor_type', 'nova_regra_consolidacao')
        logger.info(f"Tipo de processador: {processor_type}")

        # Criar processador usando Factory Pattern
        try:
            processor = ProcessorFactory.create(
                processor_type=processor_type,
                glue_handler=glue_handler,
                journey_controller=journey_controller,
                dynamodb_handler=dynamodb_handler,
                config=config
            )
            logger.info(f"Processador criado: {processor.get_processor_name()}")
        except ValueError as e:
            logger.error(f"Erro ao criar processador: {e}")
            logger.info(f"Processadores disponíveis: {ProcessorFactory.list_available()}")
            raise

        # ----------------------------------------------------------------------
        # EXECUTAR TODAS AS CONSOLIDAÇÕES
        # ----------------------------------------------------------------------

        resultados = {}
        database = args.get('database')
        execution_date = args.get('execution_date')

        if not execution_date:
            raise ValueError("Parâmetro 'execution_date' é obrigatório para execução das consolidações.")

        # Espera-se algo como: config.CONSOLIDACOES_CONFIG.regras
        consolidacoes = config.CONSOLIDACOES_CONFIG.regras

        logger.info(f"Total de tabelas consolidadas a processar: {len(consolidacoes)}")

        for tabela_consolidada in consolidacoes.keys():
            try:
                logger.info(f"Iniciando processamento da tabela: {tabela_consolidada}")


                idempotency_key = (
                    f"{args['JOB_NAME']}"
                    f"::{processor_type}"
                    f"::{tabela_consolidada}"
                    f"::{execution_date}"
                )

                result = orchestrator.execute_rule(
                    processor=processor,
                    idempotency_key=idempotency_key,
                    metadata={
                        'job_name': args.get('JOB_NAME', 'unknown'),
                        'processor_type': processor_type,
                        'database': database,
                        'tabela_consolidada': tabela_consolidada,
                        'execution_date': execution_date
                    },
                    # Parâmetros que chegam no método processar()
                    database=database,
                    tabela_consolidada=tabela_consolidada,
                    execution_date=execution_date
                )

                resultados[tabela_consolidada] = result

                if result.get('status') == 'failed':
                    logger.error(
                        f"Falha no processamento da tabela {tabela_consolidada}: "
                        f"{result.get('error', 'Erro desconhecido')}"
                    )
                    if not continue_on_error:
                        raise Exception(f"Falha no processamento da tabela {tabela_consolidada}")
                else:
                    logger.info(f"Tabela {tabela_consolidada} processada com sucesso.")

            except Exception as e:
                logger.error(f"Erro ao processar tabela {tabela_consolidada}: {e}", exc_info=True)
                resultados[tabela_consolidada] = {
                    'status': 'failed',
                    'error': str(e)
                }
                if not continue_on_error:
                    raise
                # Se continue_on_error=True, apenas loga e segue para a próxima


        logger.info("Job finalizado com sucesso")
        job.commit()

        return resultados

    except Exception as e:
        logger.error(f"Erro na execução do job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
