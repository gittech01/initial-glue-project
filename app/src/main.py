"""
Entry Point da aplicação AWS Glue.

Este módulo é o ponto de entrada principal da aplicação, responsável por
inicializar o contexto do Glue e orquestrar a execução dos processos de negócio.
"""
import sys
import logging
from typing import Optional

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
from utils.business.data_processor import DataProcessor
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler


# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_glue_context() -> tuple:
    """
    Inicializa o contexto do AWS Glue.
    
    Returns:
        Tupla (sc, glue_context, job)
    """
    try:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        job = Job(glue_context)
        
        # Obter argumentos do job
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'database',
            'table_name',
            'output_path'
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
    """
    try:
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
        
        # Criar processador de dados (regra de negócio)
        processor = DataProcessor(
            glue_handler=glue_handler,
            journey_controller=journey_controller,
            dynamodb_handler=dynamodb_handler,
            config=config
        )
        
        # Executar processamento com controle de jornada
        idempotency_key = f"job_{args.get('JOB_NAME', 'default')}_{args.get('table_name', 'unknown')}"
        
        result = journey_controller.execute_with_journey(
            processor.process_data,
            idempotency_key=idempotency_key,
            metadata={
                'job_name': args.get('JOB_NAME', 'unknown'),
                'database': args.get('database', 'unknown'),
                'table_name': args.get('table_name', 'unknown')
            },
            database=args.get('database'),
            table_name=args.get('table_name'),
            output_path=args.get('output_path')
        )
        
        logger.info(f"Processamento concluído com sucesso: {result}")
        
        # Finalizar job
        job.commit()
        logger.info("Job finalizado com sucesso")
        
        return result
        
    except Exception as e:
        logger.error(f"Erro na execução do job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
