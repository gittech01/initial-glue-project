"""
Entry Point da aplicação AWS Glue - Consolidação Flexível.

Este módulo executa consolidações de dados de forma flexível, permitindo:
- Executar uma consolidação específica ou múltiplas consolidações
- Não interromper outras consolidações caso uma falhe
- Configurações dinâmicas via settings.py

Design Patterns aplicados:
- Factory: Cria processadores através de ProcessorFactory
- Orchestrator: BusinessRuleOrchestrator coordena execuções
- Dependency Injection: Todas as dependências são injetadas
"""
import sys
import logging

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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
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
            'database',
            'tabela_consolidada',  # Opcional - se não fornecido, executa todas
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
    Função principal que executa consolidações de forma flexível.
    
    Comportamento:
    - Se 'tabela_consolidada' for fornecido: executa apenas essa consolidação
    - Se 'tabela_consolidada' não for fornecido: executa todas as consolidações do CONSOLIDACOES
    - Não interrompe outras consolidações caso uma falhe (continue_on_error=True)
    """
    try:
        # Inicializar contexto Glue
        sc, glue_context, job, args = initialize_glue_context()
        
        # Carregar configurações
        config = AppConfig()
        logger.info("Configurações carregadas")
        
        # Verificar se há consolidações configuradas
        consolidacoes_config = getattr(config, 'CONSOLIDACOES', {})
        if not consolidacoes_config:
            logger.warning("Nenhuma consolidação encontrada em CONSOLIDACOES")
            return {'status': 'warning', 'message': 'Nenhuma consolidação configurada'}
        
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
        
        # Criar processador de consolidação
        processor = ProcessorFactory.create(
            processor_type='flexible_consolidation',
            glue_handler=glue_handler,
            journey_controller=journey_controller,
            dynamodb_handler=dynamodb_handler,
            config=config
        )
        logger.info(f"Processador criado: {processor.get_processor_name()}")
        
        # Determinar quais consolidações executar
        tabela_consolidada = args.get('tabela_consolidada')
        database = args.get('database')
        
        if not database:
            raise ValueError("Parâmetro 'database' é obrigatório")
        
        # Se tabela_consolidada foi especificada, executar apenas essa
        if tabela_consolidada:
            if tabela_consolidada not in consolidacoes_config:
                raise ValueError(
                    f"Tabela consolidada '{tabela_consolidada}' não encontrada em CONSOLIDACOES. "
                    f"Disponíveis: {list(consolidacoes_config.keys())}"
                )
            tabelas_a_processar = [tabela_consolidada]
            logger.info(f"Executando consolidação específica: {tabela_consolidada}")
        else:
            # Executar todas as consolidações configuradas
            tabelas_a_processar = list(consolidacoes_config.keys())
            logger.info(f"Executando {len(tabelas_a_processar)} consolidações: {tabelas_a_processar}")
        
        # Executar cada consolidação
        resultados = {}
        sucessos = 0
        falhas = 0
        
        for tabela_consolidada in tabelas_a_processar:
            try:
                logger.info(f"Processando consolidação: {tabela_consolidada}")
                
                # Gerar chave de idempotência única para cada consolidação
                idempotency_key = (
                    f"consolidacao_{args.get('JOB_NAME', 'default')}_"
                    f"{tabela_consolidada}_{database}"
                )
                
                # Executar via orquestrador (não interrompe se uma falhar)
                result = orchestrator.execute_rule(
                    processor=processor,
                    idempotency_key=idempotency_key,
                    metadata={
                        'job_name': args.get('JOB_NAME', 'unknown'),
                        'processor_type': 'flexible_consolidation',
                        'database': database,
                        'tabela_consolidada': tabela_consolidada
                    },
                    database=database,
                    tabela_consolidada=tabela_consolidada
                )
                
                resultados[tabela_consolidada] = result
                
                # Contar sucessos e falhas
                if result.get('status') == 'success':
                    sucessos += 1
                    logger.info(f"✓ Consolidação '{tabela_consolidada}' concluída com sucesso")
                else:
                    falhas += 1
                    error_msg = result.get('error', 'Erro desconhecido')
                    logger.error(f"✗ Consolidação '{tabela_consolidada}' falhou: {error_msg}")
                    
                    # Se continue_on_error=False, interromper execução
                    if not continue_on_error:
                        raise Exception(f"Consolidação '{tabela_consolidada}' falhou: {error_msg}")
                    
            except Exception as e:
                falhas += 1
                error_msg = str(e)
                logger.error(f"✗ Erro ao processar '{tabela_consolidada}': {error_msg}")
                resultados[tabela_consolidada] = {
                    'status': 'failed',
                    'error': error_msg
                }
                
                # Se continue_on_error=False, interromper execução
                if not continue_on_error:
                    raise
        
        # Resumo final
        logger.info("=" * 60)
        logger.info(f"Resumo da execução:")
        logger.info(f"  Total de consolidações: {len(tabelas_a_processar)}")
        logger.info(f"  Sucessos: {sucessos}")
        logger.info(f"  Falhas: {falhas}")
        logger.info("=" * 60)
        
        # Finalizar job
        job.commit()
        logger.info("Job finalizado com sucesso")
        
        # Retornar resumo
        return {
            'status': 'success' if falhas == 0 else 'partial_success',
            'total': len(tabelas_a_processar),
            'sucessos': sucessos,
            'falhas': falhas,
            'resultados': resultados
        }
        
    except Exception as e:
        logger.error(f"Erro na execução do job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
