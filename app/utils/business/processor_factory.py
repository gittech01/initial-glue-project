"""
Processor Factory - Factory Pattern para criação de regras de negócio.

Permite criar diferentes tipos de processadores de forma agnóstica,
facilitando a adição de novas regras de negócio sem modificar código existente.
"""
import logging
from typing import Dict, Type, Optional

from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig
from utils.business.base_processor import BaseBusinessProcessor


logger = logging.getLogger(__name__)


class ProcessorFactory:
    """
    Factory para criar instâncias de processadores de negócio.
    
    Implementa Factory Pattern:
    - Centraliza a criação de objetos
    - Permite adicionar novos tipos sem modificar código existente
    - Facilita testes e manutenção
    
    Design Patterns aplicados:
    - Factory: Cria objetos sem especificar a classe exata
    - Registry: Mantém registro de processadores disponíveis
    """
    
    # Registry de processadores disponíveis
    _processors: Dict[str, Type[BaseBusinessProcessor]] = {}
    
    @classmethod
    def register(cls, name: str, processor_class: Type[BaseBusinessProcessor]):
        """
        Registra um novo tipo de processador.
        
        Args:
            name: Nome do processador (ex: 'data_processor', 'sales_analyzer')
            processor_class: Classe do processador
        """
        if not issubclass(processor_class, BaseBusinessProcessor):
            raise ValueError(f"{processor_class.__name__} deve herdar de BaseBusinessProcessor")
        
        cls._processors[name.lower()] = processor_class
        logger.info(f"Processador '{name}' registrado: {processor_class.__name__}")
    
    @classmethod
    def create(
        cls,
        processor_type: str,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ) -> BaseBusinessProcessor:
        """
        Cria uma instância do processador especificado.
        
        Args:
            processor_type: Tipo do processador (ex: 'data_processor', 'sales_analyzer')
            glue_handler: Handler para operações Glue
            journey_controller: Controller de jornada
            dynamodb_handler: Handler DynamoDB
            config: Configurações
        
        Returns:
            Instância do processador
        
        Raises:
            ValueError: Se o tipo de processador não estiver registrado
        """
        processor_type_lower = processor_type.lower()
        
        if processor_type_lower not in cls._processors:
            available = ', '.join(cls._processors.keys())
            raise ValueError(
                f"Processador '{processor_type}' não encontrado. "
                f"Processadores disponíveis: {available}"
            )
        
        processor_class = cls._processors[processor_type_lower]
        logger.info(f"Criando processador: {processor_class.__name__}")
        
        return processor_class(
            glue_handler=glue_handler,
            journey_controller=journey_controller,
            dynamodb_handler=dynamodb_handler,
            config=config
        )
    
    @classmethod
    def list_available(cls) -> list:
        """
        Lista todos os processadores disponíveis.
        
        Returns:
            Lista de nomes de processadores disponíveis
        """
        return list(cls._processors.keys())
    
    @classmethod
    def is_registered(cls, processor_type: str) -> bool:
        """
        Verifica se um processador está registrado.
        
        Args:
            processor_type: Tipo do processador
        
        Returns:
            True se registrado, False caso contrário
        """
        return processor_type.lower() in cls._processors


# Autoregistro de processadores (será feito quando módulos forem importados)
def _auto_register_processors():
    """Registra automaticamente os processadores disponíveis."""
    # try:
    #     from utils.business.data_processor import DataProcessor
    #     ProcessorFactory.register('data_processor', DataProcessor)
    #     ProcessorFactory.register('data', DataProcessor)  # Alias curto
    # except ImportError as e:
    #     logger.warning(f"DataProcessor não encontrado para registro: {e}")
    
    # try:
    #     from utils.business.sales_analyzer import SalesAnalyzer
    #     ProcessorFactory.register('sales_analyzer', SalesAnalyzer)
    #     ProcessorFactory.register('sales', SalesAnalyzer)  # Alias curto
    # except ImportError as e:
    #     logger.warning(f"SalesAnalyzer não encontrado para registro: {e}")
    
    # try:
    #     from utils.business.inventory_processor import InventoryProcessor
    #     ProcessorFactory.register('inventory_processor', InventoryProcessor)
    #     ProcessorFactory.register('inventory', InventoryProcessor)  # Alias curto
    # except ImportError as e:
    #     logger.debug(f"InventoryProcessor não encontrado (opcional): {e}")

    try:
        from utils.business.consolidador_posicao_operacao import ConsolidadorPosicaoOperacaoProcessor
        ProcessorFactory.register("consolidador_posicao_operacao", ConsolidadorPosicaoOperacaoProcessor)
        ProcessorFactory.register("consolidador", ConsolidadorPosicaoOperacaoProcessor) # Alias curto
    except ImportError as e:
        logger.debug(f"InventoryProcessor não encontrado (opcional): {e}")


# Executar auto-registro ao importar o módulo
_auto_register_processors()
