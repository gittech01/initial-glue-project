"""
Base Processor - Template Method Pattern para regras de negócio.

Define a interface comum e o template de execução para todas as regras de negócio,
garantindo consistência e permitindo extensão através de herança.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
import logging

from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig


logger = logging.getLogger(__name__)


class BaseBusinessProcessor(ABC):
    """
    Classe base abstrata para todas as regras de negócio.
    
    Implementa Template Method Pattern:
    - Define o esqueleto do algoritmo (process)
    - Delega etapas específicas para subclasses (hook methods)
    - Garante consistência entre todas as regras de negócio
    
    Design Patterns aplicados:
    - Template Method: process() define o template, subclasses implementam hooks
    - Strategy: Cada subclasse é uma estratégia diferente de processamento
    - Dependency Injection: Todas as dependências são injetadas
    """
    
    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: Optional[DynamoDBHandler] = None,
        config: AppConfig = None
    ):
        """
        Inicializa o processador base.
        
        Args:
            glue_handler: Handler para operações Glue
            journey_controller: Controller de jornada para idempotência e retry
            dynamodb_handler: Handler DynamoDB (opcional, não é mais usado - mantido para compatibilidade)
            config: Configurações da aplicação
        """
        self.glue_handler = glue_handler
        self.journey_controller = journey_controller
        self.dynamodb_handler = dynamodb_handler  # Mantido para compatibilidade, não é mais usado
        self.config = config
        logger.info(f"{self.__class__.__name__} inicializado")
    
    def process(
        self,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Template Method: Define o algoritmo de processamento.
        
        Este método define o fluxo padrão que todas as regras de negócio seguem:
        1. Ler dados
        2. Transformar dados (hook method)
        3. Escrever resultado no S3 e atualizar Glue Catalog
        
        Subclasses não devem sobrescrever este método, apenas os hooks.
        
        Args:
            **kwargs: Parâmetros específicos de cada regra de negócio
        
        Returns:
            Dicionário com resultado do processamento
        """
        try:
            # ETAPA 1: Ler dados (hook method)
            logger.info(f"{self.__class__.__name__}: Etapa 1 - Lendo dados")
            df = self._read_data(**kwargs)
            record_count = df.count() if df else 0
            logger.info(f"{self.__class__.__name__}: Dados lidos: {record_count} registros")
            
            # ETAPA 2: Transformar dados (hook method - implementado por subclasses)
            logger.info(f"{self.__class__.__name__}: Etapa 2 - Transformando dados")
            transformed_data = self._transform_data(df, **kwargs)
            logger.info(f"{self.__class__.__name__}: Dados transformados")
            
            # ETAPA 3: Escrever resultado no S3 e atualizar Glue Catalog
            # Sempre salva no S3 e atualiza a partição no catálogo do Glue
            if self._should_write_output(**kwargs):
                logger.info(f"{self.__class__.__name__}: Etapa 3 - Escrevendo resultado no S3 e atualizando Glue Catalog")
                # Obter output_path antes de removê-lo de kwargs (pode ser None)
                output_path = kwargs.get('output_path')
                # Remover output_path de kwargs para evitar duplicação
                write_kwargs = {k: v for k, v in kwargs.items() if k != 'output_path'}
                self._write_output(df, transformed_data, output_path, **write_kwargs)
                logger.info(f"{self.__class__.__name__}: Resultado escrito no S3 e catálogo atualizado")
            else:
                logger.warning(f"{self.__class__.__name__}: Nenhum destino de saída especificado. Dados não serão salvos.")
            
            # Retornar resultado
            result = {
                'status': 'success',
                'record_count': record_count,
                'transformed_data': transformed_data,
                'processor_type': self.__class__.__name__
            }
            
            logger.info(f"{self.__class__.__name__}: Processamento concluído: {result}")
            return result
            
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Erro no processamento: {e}", exc_info=True)
            # NÃO re-raise aqui - deixa o JourneyController lidar com retry
            # Mas loga o erro para rastreamento
            raise
    
    @abstractmethod
    def _read_data(self, **kwargs) -> DataFrame:
        """
        Hook method: Ler dados da fonte.
        
        Deve ser implementado por subclasses para definir como ler dados.
        
        Args:
            **kwargs: Parâmetros específicos
        
        Returns:
            DataFrame Spark com os dados lidos
        """
        pass
    
    @abstractmethod
    def _transform_data(self, df: DataFrame, **kwargs) -> Dict:
        """
        Hook method: Transformar dados conforme lógica de negócio.
        
        Deve ser implementado por subclasses para definir a transformação específica.
        
        Args:
            df: DataFrame Spark com dados
            **kwargs: Parâmetros específicos
        
        Returns:
            Dicionário com dados transformados
        """
        pass
    
    def _save_congregado(self, transformed_data: Dict, **kwargs) -> Dict:
        """
        DEPRECATED: Este método não é mais usado.
        
        Os dados são salvos diretamente no S3 e Glue Catalog via _write_output.
        O controle de jornada é feito via JourneyController no DynamoDB.
        
        Mantido apenas para compatibilidade com código legado.
        """
        logger.warning("_save_congregado está deprecated. Dados são salvos via _write_output no S3/Glue Catalog.")
        return {'id': None, 'status': 'deprecated'}
    
    def _write_output(self, df: DataFrame, transformed_data: Dict, output_path: str, **kwargs):
        """
        Hook method: Escrever resultado.
        
        Pode ser sobrescrito por subclasses se necessário comportamento diferente.
        
        Args:
            df: DataFrame original
            transformed_data: Dados transformados
            output_path: Caminho de saída
            **kwargs: Parâmetros específicos
        """
        self.glue_handler.write_to_s3(
            df=df,
            path=output_path,
            format=self.config.default_output_format
        )
    
    def _should_write_output(self, **kwargs) -> bool:
        """
        Hook method: Determina se deve escrever output.
        
        Pode ser sobrescrito por subclasses para lógica específica.
        
        Args:
            **kwargs: Parâmetros específicos
        
        Returns:
            True se deve escrever output, False caso contrário
        """
        return True
    
    @abstractmethod
    def _get_congregado_key(self, **kwargs) -> str:
        """
        Hook method: Gera chave primária para congregado.
        
        Deve ser implementado por subclasses.
        
        Args:
            **kwargs: Parâmetros específicos
        
        Returns:
            Chave primária para o congregado
        """
        pass
    
    def _get_congregado_metadata(self, **kwargs) -> Dict:
        """
        Hook method: Gera metadados para congregado.
        
        Pode ser sobrescrito por subclasses para metadados específicos.
        
        Args:
            **kwargs: Parâmetros específicos
        
        Returns:
            Dicionário com metadados
        """
        return { 'processor_type': self.__class__.__name__ }
    
    def get_processor_name(self) -> str:
        """Retorna o nome do processador (Nome da Classe)."""
        return self.__class__.__name__
