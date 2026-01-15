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
    """
    Função principal que orquestra a execução do job.
    
    Agora é agnóstico para múltiplas regras de negócio através do ProcessorFactory.
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
        
        # Obter tipo de processador (padrão: data_processor)
        processor_type = args.get('processor_type', 'data_processor')
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
        
        # Preparar parâmetros para o processador
        processor_kwargs = {
            'database': args.get('database'),
            'table_name': args.get('table_name'),
            'output_path': args.get('output_path')
        }
        
        # Adicionar parâmetros específicos do sales_analyzer
        if processor_type == 'sales_analyzer':
            periodo = args.get('periodo')
            if not periodo:
                raise ValueError("Parâmetro 'periodo' é obrigatório para sales_analyzer")
            processor_kwargs['periodo'] = periodo
        
        # Gerar chave de idempotência
        idempotency_key = (
            f"{processor_type}_{args.get('JOB_NAME', 'default')}_"
            f"{args.get('table_name', 'unknown')}"
        )
        if processor_type == 'sales_analyzer' and args.get('periodo'):
            idempotency_key += f"_{args.get('periodo')}"
        
        # Executar regra de negócio via orquestrador (não interrompe em caso de falha)
        result = orchestrator.execute_rule(
            processor=processor,
            idempotency_key=idempotency_key,
            metadata={
                'job_name': args.get('JOB_NAME', 'unknown'),
                'processor_type': processor_type,
                'database': args.get('database', 'unknown'),
                'table_name': args.get('table_name', 'unknown')
            },
            **processor_kwargs
        )
        
        # Verificar resultado
        if result.get('status') == 'failed':
            error_msg = result.get('error', 'Erro desconhecido')
            logger.error(f"Processamento falhou: {error_msg}")
            # Se continue_on_error=False, re-raise
            if not continue_on_error:
                raise Exception(f"Processamento falhou: {error_msg}")
            # Se continue_on_error=True, apenas loga e continua
            logger.warning("Processamento falhou mas continue_on_error=True, continuando...")
        else:
            logger.info(f"Processamento concluído com sucesso: {result}")
        
        # Retornar resultado do processador (não do orchestrator)
        if result.get('status') == 'success' and 'result' in result:
            return result['result']
        return result
        
        # Finalizar job
        job.commit()
        logger.info("Job finalizado com sucesso")
        
        return result
        
    except Exception as e:
        logger.error(f"Erro na execução do job: {e}", exc_info=True)
        # Não re-raise aqui - deixa o sistema lidar com a falha
        # Em produção, você pode querer notificar ou fazer cleanup
        raise


if __name__ == "__main__":
    main()
