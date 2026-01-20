"""
Orchestrator - Orquestra execução de múltiplas regras de negócio.

Implementa Circuit Breaker e tratamento de erros resiliente,
garantindo que falhas em uma regra não interrompam outras.
"""
import logging
from typing import Dict, List, Optional, Any, Callable
from enum import Enum

from utils.journey_controller import JourneyController, JourneyStatus
from utils.core.base_processor import BaseBusinessProcessor


logger = logging.getLogger(__name__)


class ExecutionResult(Enum):
    """Resultado da execução de uma regra de negócio."""
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"  # Já executado (idempotência)


class BusinessRuleOrchestrator:
    """
    Orquestrador de regras de negócio.
    
    Implementa padrões:
    - Circuit Breaker: Previne execuções repetidas após falhas
    - Fail-Safe: Falhas não interrompem outras execuções
    - Strategy: Executa diferentes estratégias de processamento
    
    Design Patterns aplicados:
    - Orchestrator: Coordena múltiplas operações
    - Circuit Breaker: Protege contra falhas em cascata
    - Fail-Safe: Continua execução mesmo com falhas parciais
    """
    
    def __init__(
        self,
        journey_controller: JourneyController,
        continue_on_error: bool = True,
        max_concurrent_failures: int = 3
    ):
        """
        Inicializa o orquestrador.
        
        Args:
            journey_controller: Controller de jornada
            continue_on_error: Se True, continua execução mesmo com erros
            max_concurrent_failures: Número máximo de falhas consecutivas antes de parar
        """
        self.journey_controller = journey_controller
        self.continue_on_error = continue_on_error
        self.max_concurrent_failures = max_concurrent_failures
        self.consecutive_failures = 0
        logger.info("BusinessRuleOrchestrator inicializado")
    
    def execute_rule(
        self,
        processor: BaseBusinessProcessor,
        idempotency_key: str,
        metadata: Optional[Dict] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Executa uma regra de negócio de forma resiliente.
        
        NÃO interrompe o fluxo em caso de falha se continue_on_error=True.
        
        Args:
            processor: Processador de negócio a executar
            idempotency_key: Chave de idempotência
            metadata: Metadados adicionais
            **kwargs: Parâmetros para o processador
        
        Returns:
            Dicionário com resultado da execução
        """
        processor_name = processor.get_processor_name()
        logger.info(f"Executando regra de negócio: {processor_name}")
        
        try:
            # Executar via JourneyController (garante idempotência e retry)
            result = self.journey_controller.execute_with_journey(
                processor.process,
                idempotency_key=idempotency_key,
                metadata={
                    **(metadata or {}),
                    'processor_type': processor_name
                },
                **kwargs
            )
            
            # Reset contador de falhas em caso de sucesso
            self.consecutive_failures = 0
            
            # Retornar resultado com status em minúsculas para compatibilidade
            return {
                'status': 'success',  # Usar minúsculas para compatibilidade
                'execution_status': ExecutionResult.SUCCESS.value,  # Manter enum também
                'result': result,
                'processor': processor_name,
                'idempotency_key': idempotency_key
            }
            
        except Exception as e:
            self.consecutive_failures += 1
            error_msg = str(e)
            
            logger.error(
                f"Erro ao executar regra {processor_name} "
                f"(falhas consecutivas: {self.consecutive_failures}): {error_msg}",
                exc_info=True
            )
            
            # Se continue_on_error=False ou muitas falhas consecutivas, re-raise
            if not self.continue_on_error or self.consecutive_failures >= self.max_concurrent_failures:
                logger.error(
                    f"Interrompendo execução após {self.consecutive_failures} falhas consecutivas"
                )
                raise
            
            # Retornar resultado de erro sem interromper fluxo
            return {
                'status': 'failed',  # Usar minúsculas para compatibilidade
                'execution_status': ExecutionResult.FAILED.value,  # Manter enum também
                'error': error_msg,
                'processor': processor_name,
                'idempotency_key': idempotency_key,
                'consecutive_failures': self.consecutive_failures
            }
    
    def execute_multiple_rules(
        self,
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Executa múltiplas regras de negócio de forma isolada.
        
        Cada regra é executada independentemente. Falhas em uma não afetam outras.
        
        Args:
            rules: Lista de dicionários com:
                - processor: BaseBusinessProcessor
                - idempotency_key: str
                - metadata: Dict (opcional)
                - **kwargs: Parâmetros específicos
        
        Returns:
            Dicionário com resultados de todas as execuções
        """
        logger.info(f"Executando {len(rules)} regras de negócio")
        results = {}
        successful = 0
        failed = 0
        skipped = 0
        
        for i, rule_config in enumerate(rules, 1):
            processor = rule_config.get('processor')
            idempotency_key = rule_config.get('idempotency_key')
            metadata = rule_config.get('metadata')
            kwargs = {k: v for k, v in rule_config.items() 
                     if k not in ['processor', 'idempotency_key', 'metadata']}
            
            if not processor or not idempotency_key:
                logger.warning(f"Regra {i} ignorada: processor ou idempotency_key ausente")
                continue
            
            processor_name = processor.get_processor_name()
            logger.info(f"Executando regra {i}/{len(rules)}: {processor_name}")
            
            try:
                result = self.execute_rule(
                    processor=processor,
                    idempotency_key=idempotency_key,
                    metadata=metadata,
                    **kwargs
                )
                
                results[processor_name] = result
                
                # Comparar status em minúsculas (execute_rule retorna minúsculas)
                status = result.get('status', '').lower()
                if status == 'success':
                    successful += 1
                elif status == 'skipped':
                    skipped += 1
                else:
                    failed += 1
                    
            except Exception as e:
                # Se continue_on_error=False, esta exceção será propagada
                # Mas se continue_on_error=True, execute_rule já tratou
                failed += 1
                results[processor_name] = {
                    'status': ExecutionResult.FAILED.value,
                    'error': str(e),
                    'processor': processor_name
                }
                logger.error(f"Erro fatal na regra {processor_name}: {e}")
        
        summary = {
            'total': len(rules),
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'results': results
        }
        
        logger.info(f"Execução concluída: {summary}")
        return summary
    
    def reset_failure_counter(self):
        """Reseta o contador de falhas consecutivas."""
        self.consecutive_failures = 0
        logger.info("Contador de falhas resetado")
