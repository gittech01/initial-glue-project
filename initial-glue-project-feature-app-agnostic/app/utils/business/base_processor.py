"""
Base Processor - Template Method Pattern para regras de negócio.

Define a interface comum e o template de execução para todas as regras de negócio,
garantindo consistência e permitindo extensão através de herança.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any
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
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ):
        """
        Inicializa o processador base.
        
        Args:
            glue_handler: Handler para operações Glue
            journey_controller: Controller de jornada para idempotência
            dynamodb_handler: Handler para salvar congregado
            config: Configurações da aplicação
        """
        self.glue_handler = glue_handler
        self.journey_controller = journey_controller
        self.dynamodb_handler = dynamodb_handler
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
        3. Salvar congregado
        4. Escrever resultado (opcional)
        
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
            
            # ETAPA 3: Salvar congregado (idempotente)
            logger.info(f"{self.__class__.__name__}: Etapa 3 - Salvando congregado")
            congregado_result = self._save_congregado(transformed_data, **kwargs)
            logger.info(f"{self.__class__.__name__}: Congregado salvo: {congregado_result}")
            
            # ETAPA 4: Escrever resultado (hook method - opcional)
            output_path = kwargs.get('output_path')
            if output_path and self._should_write_output(**kwargs):
                logger.info(f"{self.__class__.__name__}: Etapa 4 - Escrevendo resultado")
                # Remover output_path de kwargs para evitar duplicação
                write_kwargs = {k: v for k, v in kwargs.items() if k != 'output_path'}
                self._write_output(df, transformed_data, output_path, **write_kwargs)
                logger.info(f"{self.__class__.__name__}: Resultado escrito")
            
            # Retornar resultado
            result = {
                'status': 'success',
                'record_count': record_count,
                'transformed_data': transformed_data,
                'congregado_id': congregado_result.get('id'),
                'output_path': output_path,
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
        Salva congregado no DynamoDB de forma idempotente.
        
        Pode ser sobrescrito por subclasses se necessário comportamento diferente.
        
        Args:
            transformed_data: Dados transformados
            **kwargs: Parâmetros específicos
        
        Returns:
            Resultado da operação de salvamento
        """
        primary_key = self._get_congregado_key(**kwargs)
        metadata = self._get_congregado_metadata(**kwargs)
        
        return self.dynamodb_handler.save_congregado(
            congregado_data=transformed_data,
            primary_key=primary_key,
            metadata=metadata
        )
    
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
        return {
            'processor_type': self.__class__.__name__
        }
    
    def get_processor_name(self) -> str:
        """
        Retorna o nome do processador.
        
        Returns:
            Nome do processador
        """
        return self.__class__.__name__
