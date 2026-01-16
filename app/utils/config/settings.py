"""
Configurações da aplicação.

Centraliza todas as configurações da aplicação, permitindo fácil
customização e manutenção.
"""
import os
from typing import Optional


class AppConfig:
    """
    Classe de configuração da aplicação.
    
    Carrega configurações de variáveis de ambiente ou usa valores padrão.
    """
    
    def __init__(self):
        """Inicializa configurações a partir de variáveis de ambiente."""
        
        # AWS Configuration
        self.aws_region: str = 'sa-east-1'
        
        # DynamoDB Tables
        self.journey_table_name: str = 'journey_control'
        self.congregado_table_name: str = 'congregado_data'
        
        # Glue Configuration
        self.default_database: str = 'default_database'
        self.default_output_format: str = 'parquet'
        
        # Retry Configuration
        self.max_retries: int = 3
        self.retry_delay: int = 2
        
        # Logging Configuration
        self.log_level: str = 'INFO'
        
        # Processing Configuration
        self.batch_size: int = 1000
        self.enable_partitioning: bool = True


    
    def __repr__(self) -> str:
        """Representação string da configuração."""
        return (
            f"AppConfig("
            f"region={self.aws_region}, "
            f"journey_table={self.journey_table_name}, "
            f"congregado_table={self.congregado_table_name}"
            f")"
        )
