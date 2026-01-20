"""
Módulo utils contendo classes para controle de jornada, handlers, configurações e regras de negócio.

DynamoDB é usado APENAS para métricas de jornada (via JourneyController).
Dados consolidados são salvos diretamente no S3 e Glue Catalog.
"""

from .journey_controller import JourneyController, JourneyStatus, journey_controlled
from .config.settings import AppConfig
from .handlers.glue_handler import GlueDataHandler

__all__ = [
    'JourneyController',
    'JourneyStatus',
    'journey_controlled',
    'AppConfig',
    'GlueDataHandler',
]
