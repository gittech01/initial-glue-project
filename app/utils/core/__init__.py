"""
Core - Estrutura base da aplicação.

Contém componentes fundamentais que são agnósticos a regras de negócio:
- BaseBusinessProcessor: Template base para processadores
- BusinessRuleOrchestrator: Orquestrador de execuções
- ProcessorFactory: Factory para criação de processadores

Todos os componentes são agnósticos a jornada (journey_controller é opcional).
"""

from utils.core.base_processor import BaseBusinessProcessor
from utils.core.orchestrator import BusinessRuleOrchestrator, ExecutionResult
from utils.core.processor_factory import ProcessorFactory

__all__ = [
    'BaseBusinessProcessor',
    'BusinessRuleOrchestrator',
    'ExecutionResult',
    'ProcessorFactory'
]
