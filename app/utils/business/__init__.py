"""
Business - Regras de negócio da aplicação.

Contém implementações específicas de regras de negócio:
- FlexibleConsolidationProcessor: Processador de consolidação flexível

As regras de negócio herdam de utils.core.BaseBusinessProcessor.
"""

# Importar FlexibleConsolidationProcessor se disponível
try:
    from .flexible_consolidation_processor import FlexibleConsolidationProcessor
    __all__ = [
        'FlexibleConsolidationProcessor'
    ]
except ImportError:
    __all__ = []
