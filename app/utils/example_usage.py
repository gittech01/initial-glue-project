"""
Exemplo de uso das classes JourneyController e DynamoDBHandler.

Este arquivo demonstra como usar as classes para controle de jornada
e persistência idempotente no DynamoDB.
"""
from journey_controller import JourneyController, JourneyStatus
from dynamodb_handler import DynamoDBHandler
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def exemplo_journey_controller():
    """Exemplo de uso do JourneyController."""
    logger.info("=== Exemplo JourneyController ===")
    
    # Inicializar controller
    controller = JourneyController(
        table_name="journey_control",
        region_name="us-east-1",
        max_retries=3,
        retry_delay=2
    )
    
    # Iniciar uma jornada com chave de idempotência
    journey_id = controller.start_journey(
        idempotency_key="process_data_2024-01-01",
        metadata={"source": "example", "date": "2024-01-01"}
    )
    logger.info(f"Jornada iniciada: {journey_id}")
    
    # Adicionar etapas
    controller.add_step(journey_id, "data_extraction", {"records": 1000})
    controller.add_step(journey_id, "data_transformation", {"transformed": 950})
    
    # Atualizar status
    controller.update_status(journey_id, JourneyStatus.IN_PROGRESS)
    
    # Executar função com controle de jornada
    def process_data():
        """Função de exemplo que processa dados."""
        logger.info("Processando dados...")
        return {"processed": 950, "errors": 50}
    
    try:
        result = controller.execute_with_journey(
            process_data,
            journey_id=journey_id,
            idempotency_key="process_data_2024-01-01"
        )
        logger.info(f"Resultado: {result}")
    except Exception as e:
        logger.error(f"Erro na execução: {e}")
    
    # Recuperar informações da jornada
    journey_info = controller.get_journey(journey_id)
    logger.info(f"Informações da jornada: {journey_info}")


def exemplo_dynamodb_handler():
    """Exemplo de uso do DynamoDBHandler."""
    logger.info("=== Exemplo DynamoDBHandler ===")
    
    # Inicializar handler
    handler = DynamoDBHandler(
        table_name="congregado_data",
        region_name="us-east-1",
        max_retries=3
    )
    
    # Dados agregados (congregado) de exemplo
    congregado_data = {
        "total_vendas": 150000.50,
        "total_clientes": 500,
        "periodo": "2024-01",
        "categoria": "vendas"
    }
    
    # Salvar congregado de forma idempotente
    try:
        result = handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="vendas_2024-01",
            metadata={"source": "glue_job", "timestamp": "2024-01-01T10:00:00"}
        )
        logger.info(f"Congregado salvo: {result}")
        
        # Tentar salvar novamente (deve ser idempotente)
        result2 = handler.save_congregado(
            congregado_data=congregado_data,
            primary_key="vendas_2024-01",
            metadata={"source": "glue_job", "timestamp": "2024-01-01T10:00:00"}
        )
        logger.info(f"Segunda tentativa (idempotente): {result2}")
        
        # Recuperar congregado
        retrieved = handler.get_congregado("vendas_2024-01")
        logger.info(f"Congregado recuperado: {retrieved}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar congregado: {e}")


def exemplo_integrado():
    """Exemplo integrado usando ambas as classes."""
    logger.info("=== Exemplo Integrado ===")
    
    # Inicializar ambos
    journey_controller = JourneyController(
        table_name="journey_control",
        region_name="us-east-1"
    )
    
    dynamodb_handler = DynamoDBHandler(
        table_name="congregado_data",
        region_name="us-east-1"
    )
    
    # Função que processa e salva dados
    def process_and_save():
        # Simular processamento
        logger.info("Processando dados...")
        processed_data = {
            "total_vendas": 200000.00,
            "total_clientes": 750,
            "periodo": "2024-02",
            "categoria": "vendas"
        }
        
        # Salvar no DynamoDB
        result = dynamodb_handler.save_congregado(
            congregado_data=processed_data,
            primary_key="vendas_2024-02"
        )
        logger.info(f"Dados salvos: {result}")
        return result
    
    # Executar com controle de jornada
    journey_id = journey_controller.start_journey(
        idempotency_key="process_and_save_2024-02"
    )
    
    try:
        result = journey_controller.execute_with_journey(
            process_and_save,
            journey_id=journey_id,
            idempotency_key="process_and_save_2024-02"
        )
        logger.info(f"Processo completo: {result}")
    except Exception as e:
        logger.error(f"Erro no processo: {e}")


if __name__ == "__main__":
    # Executar exemplos
    exemplo_journey_controller()
    exemplo_dynamodb_handler()
    exemplo_integrado()
