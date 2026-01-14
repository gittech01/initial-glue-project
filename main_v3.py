"""
Entry Point da aplicação AWS Glue - Agnóstico para múltiplas regras de negócio.
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
from utils.business.processor_factory import ProcessorFactory
from utils.business.orchestrator import BusinessRuleOrchestrator


# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_glue_context() -> tuple:
    try:
        sc = SparkContext()
        glue_context = GlueContext(sc)
        job = Job(glue_context)
        
        # Obter argumentos do job (Simplificado: removidas origens dinâmicas)
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'processor_type',
            'database',
            'table_name',
            'output_path',
            'continue_on_error'
        ])
        
        job.init(args['JOB_NAME'], args)
        return sc, glue_context, job, args
    except Exception as e:
        logger.error(f"Erro ao inicializar contexto Glue: {e}")
        raise


def main():
    try:
        sc, glue_context, job, args = initialize_glue_context()
        config = AppConfig()
        
        glue_handler = GlueDataHandler(glue_context)
        journey_controller = JourneyController(
            table_name=config.journey_table_name,
            region_name=config.aws_region
        )
        dynamodb_handler = DynamoDBHandler(
            table_name=config.congregado_table_name,
            region_name=config.aws_region
        )
        
        continue_on_error = args.get('continue_on_error', 'true').lower() == 'true'
        orchestrator = BusinessRuleOrchestrator(
            journey_controller=journey_controller,
            continue_on_error=continue_on_error
        )
        
        processor_type = args.get('processor_type', 'data_processor')
        
        processor = ProcessorFactory.create(
            processor_type=processor_type,
            glue_handler=glue_handler,
            journey_controller=journey_controller,
            dynamodb_handler=dynamodb_handler,
            config=config
        )
        
        processor_kwargs = {
            'database': args.get('database'),
            'table_name': args.get('table_name'),
            'output_path': args.get('output_path')
        }
        
        # Lógica para Consolidação usando parâmetros FIXOS da AppConfig
        if processor_type in ['consolidador_posicao', 'consolidacao']:
            from utils.business.consolidador_posicao import OrigemSpec
            
            # Injeta os valores fixos definidos no settings.py
            processor_kwargs['origem_a'] = OrigemSpec(tables={'a': f"{config.db_online}.{config.tabela_online}"}, joins=[])
            processor_kwargs['origem_b'] = OrigemSpec(tables={'b': f"{config.db_batch}.{config.tabela_batch}"}, joins=[])
            processor_kwargs['nome_origem_a'] = "ONLINE"
            processor_kwargs['nome_origem_b'] = "BATCH"

        idempotency_key = f"{processor_type}_{args.get('JOB_NAME')}_{args.get('table_name', 'consolidado')}"
        
        result = orchestrator.execute_rule(
            processor=processor,
            idempotency_key=idempotency_key,
            metadata={'job_name': args.get('JOB_NAME'), 'processor_type': processor_type},
            **processor_kwargs
        )
        
        job.commit()
        return result
        
    except Exception as e:
        logger.error(f"Erro na execução do job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
