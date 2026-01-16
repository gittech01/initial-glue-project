"""Módulo de regras de negócio da aplicação."""

from .base_processor import BaseBusinessProcessor
# from .data_processor import DataProcessor
# from .sales_analyzer import SalesAnalyzer
from .processor_factory import ProcessorFactory
from .orchestrator import BusinessRuleOrchestrator, ExecutionResult

# Importar InventoryProcessor se disponível (exemplo)
try:
    from app.utils.business.exemplo_implementa_nova_negocio.inventory_processor import InventoryProcessor
    __all__ = [
        'BaseBusinessProcessor',
        # 'DataProcessor',
        # 'SalesAnalyzer',
        # 'InventoryProcessor',
        'ProcessorFactory',
        'BusinessRuleOrchestrator',
        'ExecutionResult'
    ]
except ImportError:
    __all__ = [
        'BaseBusinessProcessor',
        # 'DataProcessor',
        # 'SalesAnalyzer',
        'ProcessorFactory',
        'BusinessRuleOrchestrator',
        'ExecutionResult'
    ]
