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
        self.aws_region: str = os.getenv('AWS_REGION', 'us-east-1')
        self.aws_access_key_id: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        # DynamoDB Tables
        self.journey_table_name: str = os.getenv('JOURNEY_TABLE_NAME', 'journey_control')
        self.congregado_table_name: str = os.getenv('CONGREGADO_TABLE_NAME', 'congregado_data')
        
        # Glue Configuration
        self.default_database: str = os.getenv('GLUE_DATABASE', 'default_database')
        self.default_output_format: str = os.getenv('OUTPUT_FORMAT', 'parquet')
        
        # Retry Configuration
        self.max_retries: int = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay: int = int(os.getenv('RETRY_DELAY', '2'))
        
        # Logging Configuration
        self.log_level: str = os.getenv('LOG_LEVEL', 'INFO')
        
        # Processing Configuration
        self.batch_size: int = int(os.getenv('BATCH_SIZE', '1000'))
        self.enable_partitioning: bool = os.getenv('ENABLE_PARTITIONING', 'true').lower() == 'true'
    
    def __repr__(self) -> str:
        """Representação string da configuração."""
        return (
            f"AppConfig("
            f"region={self.aws_region}, "
            f"journey_table={self.journey_table_name}, "
            f"congregado_table={self.congregado_table_name}"
            f")"
        )
