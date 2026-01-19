"""Módulo de regras de negócio da aplicação."""

from .base_processor import BaseBusinessProcessor
from .processor_factory import ProcessorFactory
from .orchestrator import BusinessRuleOrchestrator, ExecutionResult

# Importar FlexibleConsolidationProcessor se disponível
try:
    from .flexible_consolidation_processor import FlexibleConsolidationProcessor
    __all__ = [
        'BaseBusinessProcessor',
        'FlexibleConsolidationProcessor',
        'ProcessorFactory',
        'BusinessRuleOrchestrator',
        'ExecutionResult'
    ]
except ImportError:
    __all__ = [
        'BaseBusinessProcessor',
        'ProcessorFactory',
        'BusinessRuleOrchestrator',
        'ExecutionResult'
    ]
