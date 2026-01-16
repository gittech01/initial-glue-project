"""
Entry point da aplicação AWS Glue - Integração de Regras de Negócio. 

Orquestra o fluxo completo de processamento de dados incluindo:
- Consolidação de posição de operações (NovaRegraConsolidacao)
- Outras regras de negócio via ProcessorFactory
"""
import sys
import logging
from typing import Dict

# AWS Glue imports
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Application imports
from utils.config. settings import AppConfig
from utils.handlers.glue_handler import GlueDataHandler
from utils.handlers.dynamodb_handler import DynamoDBHandler
from utils.journey_controller import JourneyController
from utils.business.nova_regra import NovaRegraConsolidacao
from app.utils.business.exemplo_implementa_nova_negocio.data_processor import DataProcessor

logger = logging.getLogger(__name__)


def initialize_glue_context():
    """
    Inicializa contexto AWS Glue. 
    
    Returns:
        Tupla com (SparkContext, GlueContext, Job, args)
    """
    try:
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'database',
            'table_name',
            'output_path'
        ])
        
        sc = SparkContext()
        glue_context = GlueContext(sc)
        job = Job(glue_context)
        job.init(args['JOB_NAME'], args)
        
        logger.info(f"Glue context inicializado:  {args['JOB_NAME']}")
        return sc, glue_context, job, args
        
    except Exception as e: 
        logger.error(f"Erro ao inicializar Glue context: {str(e)}")
        raise


def main() -> Dict:
    """
    Função principal que orquestra o fluxo de processamento.
    
    Returns:
        Dicionário com resultado do processamento
    """
    try:
        # Inicializar contexto Glue
        logger.info("Iniciando aplicação Glue")
        sc, glue_context, job, args = initialize_glue_context()
        
        # Inicializar configurações
        config = AppConfig()
        logger.info(f"Configurações carregadas: {config.aws_region}")
        
        # Inicializar handlers
        glue_handler = GlueDataHandler(glue_context)
        dynamodb_handler = DynamoDBHandler(
            table_name=config.congregado_table_name,
            region_name=config.aws_region
        )
        journey_controller = JourneyController(
            table_name=config.journey_table_name,
            region_name=config.aws_region
        )
        
        logger.info("Handlers inicializados com sucesso")
        
        # Instanciar nova regra de consolidação
        nova_regra = NovaRegraConsolidacao(
            glue_handler=glue_handler,
            journey_controller=journey_controller,
            dynamodb_handler=dynamodb_handler,
            config=config
        )
        
        logger.info("Nova regra de consolidação instanciada")
        
        # Executar consolidação com controle de jornada
        resultado = journey_controller.execute_with_journey(
            nova_regra. processar_consolidacao,
            idempotency_key=f"consolidacao_{args. get('table_name')}",
            database=args.get('database'),
            table_name=args.get('table_name'),
            output_path=args.get('output_path')
        )
        
        logger.info(f"Processamento concluído com resultado: {resultado['status']}")
        
        # Finalizar job Glue
        job.commit()
        
        return {
            'status': 'success',
            'result': resultado
        }
        
    except Exception as e:
        logger.error(f"Erro no fluxo principal: {str(e)}")
        raise


if __name__ == '__main__': 
    try:
        main()
    except Exception as e: 
        logger.error(f"Falha na execução:  {str(e)}")
        sys.exit(1)