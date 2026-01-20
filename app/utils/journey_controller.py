"""
Classe para controle de jornada da aplicação com suporte a idempotência e resiliência a falhas.
Gerencia estados de execução, rastreamento de etapas e recuperação de processos interrompidos.
"""
import json
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, Callable
from enum import Enum
import logging
from functools import wraps

try:
    import boto3
    from botocore.exceptions import ClientError
    from boto3.dynamodb.conditions import Key
except ImportError:
    boto3 = None
    ClientError = Exception
    Key = None
    logging.warning("boto3 não encontrado. Funcionalidades do DynamoDB serão limitadas.")


class JourneyStatus(Enum):
    """Status possíveis de uma jornada."""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


class JourneyController:
    """
    Controlador de jornada que gerencia execuções de processos de forma idempotente.
    
    Características:
    - Idempotência: Execuções duplicadas não causam efeitos colaterais
    - Resiliência: Recupera de falhas e permite retry
    - Rastreamento: Mantém histórico de estados e etapas
    - Isolamento: Cada jornada é independente
    """
    
    def __init__(
        self,
        table_name: str,
        region_name: str = "us-east-1",
        max_retries: int = 3,
        retry_delay: int = 5,
        dynamodb_client=None
    ):
        """
        Inicializa o controlador de jornada.
        
        Args:
            table_name: Nome da tabela DynamoDB para armazenar estados
            region_name: Região AWS (padrão: us-east-1)
            max_retries: Número máximo de tentativas em caso de falha
            retry_delay: Delay inicial entre tentativas (em segundos)
            dynamodb_client: Cliente DynamoDB opcional (útil para testes)
        """
        self.table_name = table_name
        self.region_name = region_name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)
        
        # Inicializar cliente DynamoDB
        if dynamodb_client:
            self.dynamodb = dynamodb_client
        elif boto3:
            try:
                self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
            except Exception as e:
                # Se falhar (ex: NoCredentialsError), usar modo em memória
                self.logger.warning(f"Erro ao inicializar DynamoDB ({type(e).__name__}): {e}. Usando armazenamento em memória.")
                self.dynamodb = None
                self._in_memory_store = {}
        else:
            self.dynamodb = None
            self.logger.warning("DynamoDB não disponível. Usando armazenamento em memória.")
            self._in_memory_store = {}
        
        self._ensure_table_exists()
    
    def _ensure_table_exists(self):
        """Garante que a tabela DynamoDB existe. Se não existir, usa armazenamento em memória."""
        if not self.dynamodb:
            # Garantir que _in_memory_store existe se não tiver DynamoDB
            if not hasattr(self, '_in_memory_store'):
                self._in_memory_store = {}
            self.table = None
            return
        
        try:
            self.table = self.dynamodb.Table(self.table_name)
            # Verificar se a tabela existe fazendo uma operação simples
            self.table.meta.client.describe_table(TableName=self.table_name)
            self.logger.info(f"Tabela DynamoDB '{self.table_name}' encontrada e pronta para uso.")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'ResourceNotFoundException':
                self.logger.warning(
                    f"Tabela DynamoDB '{self.table_name}' não encontrada. "
                    "Usando armazenamento em memória. "
                    f"Para usar DynamoDB, crie a tabela '{self.table_name}' com chave primária 'journey_id' (String)."
                )
                self.dynamodb = None
                self.table = None
                if not hasattr(self, '_in_memory_store'):
                    self._in_memory_store = {}
            else:
                # Outro erro do DynamoDB (ex: AccessDeniedException)
                self.logger.warning(
                    f"Erro ao acessar DynamoDB ({error_code}): {e}. "
                    "Usando armazenamento em memória."
                )
                self.dynamodb = None
                self.table = None
                if not hasattr(self, '_in_memory_store'):
                    self._in_memory_store = {}
        except Exception as e:
            # Capturar qualquer outro erro (ex: NoCredentialsError, AttributeError)
            self.logger.warning(
                f"Erro ao acessar DynamoDB ({type(e).__name__}): {e}. "
                "Usando armazenamento em memória."
            )
            self.dynamodb = None
            self.table = None
            if not hasattr(self, '_in_memory_store'):
                self._in_memory_store = {}
    
    def _get_item(self, journey_id: str) -> Optional[Dict]:
        """Recupera um item do DynamoDB ou do armazenamento em memória."""
        if not self.dynamodb or not self.table:
            # Usar armazenamento em memória
            if not hasattr(self, '_in_memory_store'):
                self._in_memory_store = {}
            return self._in_memory_store.get(journey_id)
        
        try:
            response = self.table.get_item(Key={'journey_id': journey_id})
            return response.get('Item')
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'ResourceNotFoundException':
                # Tabela não existe, fallback para memória
                self.logger.warning(
                    f"Tabela '{self.table_name}' não encontrada durante GetItem. "
                    "Usando armazenamento em memória."
                )
                self.dynamodb = None
                self.table = None
                if not hasattr(self, '_in_memory_store'):
                    self._in_memory_store = {}
                return self._in_memory_store.get(journey_id)
            else:
                self.logger.error(f"Erro ao recuperar jornada {journey_id}: {e}")
                return None
    
    def _put_item(self, item: Dict):
        """Salva um item no DynamoDB ou no armazenamento em memória."""
        if not self.dynamodb or not self.table:
            # Usar armazenamento em memória
            if not hasattr(self, '_in_memory_store'):
                self._in_memory_store = {}
            self._in_memory_store[item['journey_id']] = item
            return
        
        try:
            self.table.put_item(Item=item)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'ResourceNotFoundException':
                # Tabela não existe, fallback para memória
                self.logger.warning(
                    f"Tabela '{self.table_name}' não encontrada durante PutItem. "
                    "Usando armazenamento em memória."
                )
                self.dynamodb = None
                self.table = None
                if not hasattr(self, '_in_memory_store'):
                    self._in_memory_store = {}
                self._in_memory_store[item['journey_id']] = item
            else:
                self.logger.error(f"Erro ao salvar jornada {item.get('journey_id')}: {e}")
                raise
    
    def _update_item(self, journey_id: str, update_expression: str, expression_values: Dict, expression_names: Dict = None):
        """Atualiza um item no DynamoDB ou no armazenamento em memória."""
        if not self.dynamodb or not self.table:
            # Usar armazenamento em memória
            if not hasattr(self, '_in_memory_store'):
                self._in_memory_store = {}
            item = self._in_memory_store.get(journey_id, {})
            if not item:
                return
            
            # Processar SET expressions
            if 'SET' in update_expression:
                # Extrair campos SET
                set_part = update_expression.split('SET')[1].split(',')[0].strip()
                if 'ADD' in set_part:
                    set_part = set_part.split('ADD')[0].strip()
                
                # Atualizar status
                if ':status' in expression_values:
                    item['status'] = expression_values[':status']
                if ':updated_at' in expression_values:
                    item['updated_at'] = expression_values[':updated_at']
                if ':error' in expression_values:
                    item['error_message'] = expression_values[':error']
                if ':result' in expression_values:
                    item['result'] = expression_values[':result']
            
            # Processar ADD expressions (para retry_count)
            if 'ADD' in update_expression and ':inc' in expression_values:
                item['retry_count'] = item.get('retry_count', 0) + expression_values[':inc']
            
            self._in_memory_store[journey_id] = item
            return
        
        try:
            kwargs = {
                'Key': {'journey_id': journey_id},
                'UpdateExpression': update_expression,
                'ExpressionAttributeValues': expression_values
            }
            if expression_names:
                kwargs['ExpressionAttributeNames'] = expression_names
            self.table.update_item(**kwargs)
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == 'ResourceNotFoundException':
                # Tabela não existe, fallback para memória
                self.logger.warning(
                    f"Tabela '{self.table_name}' não encontrada durante UpdateItem. "
                    "Usando armazenamento em memória."
                )
                self.dynamodb = None
                self.table = None
                if not hasattr(self, '_in_memory_store'):
                    self._in_memory_store = {}
                # Reprocessar update em memória
                item = self._in_memory_store.get(journey_id, {})
                if item:
                    if 'SET' in update_expression:
                        if ':status' in expression_values:
                            item['status'] = expression_values[':status']
                        if ':updated_at' in expression_values:
                            item['updated_at'] = expression_values[':updated_at']
                        if ':error' in expression_values:
                            item['error_message'] = expression_values[':error']
                        if ':result' in expression_values:
                            item['result'] = expression_values[':result']
                    if 'ADD' in update_expression and ':inc' in expression_values:
                        item['retry_count'] = item.get('retry_count', 0) + expression_values[':inc']
                    self._in_memory_store[journey_id] = item
            else:
                self.logger.error(f"Erro ao atualizar jornada {journey_id}: {e}")
                raise
    
    def start_journey(
        self,
        journey_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
        idempotency_key: Optional[str] = None
    ) -> str:
        """
        Inicia uma nova jornada ou recupera uma existente (idempotência).
        
        Args:
            journey_id: ID único da jornada (gerado automaticamente se None)
            metadata: Metadados adicionais da jornada
            idempotency_key: Chave de idempotência (se fornecida, verifica jornada existente)
        
        Returns:
            ID da jornada (novo ou existente)
        """
        # Se idempotency_key for fornecida, ela tem prioridade para garantir idempotência
        # Buscar jornada existente por idempotency_key primeiro
        if idempotency_key:
            # Buscar jornadas existentes que tenham essa idempotency_key
            # Em modo em memória, precisamos fazer uma busca linear
            if not self.dynamodb:
                # Buscar em todas as jornadas em memória
                for stored_journey_id, stored_journey in self._in_memory_store.items():
                    if stored_journey.get('idempotency_key') == idempotency_key:
                        # Jornada existente encontrada com essa idempotency_key
                        self.logger.info(
                            f"Jornada existente encontrada com idempotency_key: {idempotency_key} "
                            f"(journey_id: {stored_journey_id})"
                        )
                        return stored_journey_id
            else:
                # Em DynamoDB, seria necessário GSI ou scan para buscar por idempotency_key
                # Por enquanto, se journey_id foi fornecido, tentar buscar por ele primeiro
                # Mas se idempotency_key for diferente, precisamos fazer scan
                # Por simplicidade e para garantir idempotência, se idempotency_key foi fornecida,
                # usamos ela como journey_id (a menos que já exista uma jornada com journey_id diferente)
                if journey_id:
                    # Verificar se a jornada com esse journey_id tem a mesma idempotency_key
                    existing = self._get_item(journey_id)
                    if existing and existing.get('idempotency_key') == idempotency_key:
                        # Mesma jornada, retornar
                        self.logger.info(
                            f"Jornada existente encontrada: journey_id={journey_id}, "
                            f"idempotency_key={idempotency_key}"
                        )
                        return journey_id
                    elif existing and existing.get('idempotency_key') != idempotency_key:
                        # Conflito: journey_id existe mas com idempotency_key diferente
                        # Para garantir idempotência, devemos buscar por idempotency_key
                        # Como não temos GSI, vamos usar idempotency_key como journey_id
                        self.logger.warning(
                            f"Conflito detectado: journey_id={journey_id} existe mas com "
                            f"idempotency_key diferente. Usando idempotency_key={idempotency_key} "
                            "como journey_id para garantir idempotência."
                        )
                        journey_id = idempotency_key
                else:
                    # Se journey_id não foi fornecido, usar idempotency_key como journey_id
                    journey_id = idempotency_key
                
                # Verificar se existe jornada com esse journey_id (que agora é igual a idempotency_key)
                existing = self._get_item(journey_id)
                if existing and existing.get('idempotency_key') == idempotency_key:
                    self.logger.info(
                        f"Jornada existente encontrada com idempotency_key: {idempotency_key} "
                        f"(journey_id: {journey_id})"
                    )
                    return journey_id
        
        # Se journey_id ainda não foi definido, gerar novo
        if not journey_id:
            journey_id = str(uuid.uuid4())
        
        # Verificar se a jornada já existe (verificação final antes de criar)
        existing = self._get_item(journey_id)
        if existing:
            # Se já existe, verificar se a idempotency_key corresponde (se fornecida)
            if idempotency_key and existing.get('idempotency_key') != idempotency_key:
                # Conflito: jornada existe mas com idempotency_key diferente
                # Isso não deveria acontecer com a lógica acima, mas é uma verificação de segurança
                self.logger.error(
                    f"Conflito crítico: jornada {journey_id} existe mas com idempotency_key diferente. "
                    f"Esperado: {idempotency_key}, Encontrado: {existing.get('idempotency_key')}"
                )
                # Para garantir idempotência, retornar a jornada existente mesmo com idempotency_key diferente
                # Isso previne criação de jornadas duplicadas
            
            status = existing.get('status')
            if status == JourneyStatus.COMPLETED.value:
                self.logger.info(f"Jornada {journey_id} já foi completada. Retornando ID existente.")
                return journey_id
            elif status == JourneyStatus.IN_PROGRESS.value:
                self.logger.warning(f"Jornada {journey_id} já está em progresso. Retomando execução.")
                return journey_id
        
        # Criar nova jornada
        journey_data = {
            'journey_id': journey_id,
            'status': JourneyStatus.PENDING.value,
            'created_at': datetime.utcnow().isoformat(),
            'updated_at': datetime.utcnow().isoformat(),
            'metadata': metadata or {},
            'steps': [],
            'retry_count': 0,
            'idempotency_key': idempotency_key or journey_id
        }
        
        self._put_item(journey_data)
        self.logger.info(f"Jornada {journey_id} iniciada.")
        return journey_id
    
    def update_status(self, journey_id: str, status: JourneyStatus, error: Optional[str] = None):
        """
        Atualiza o status de uma jornada.
        
        Args:
            journey_id: ID da jornada
            status: Novo status
            error: Mensagem de erro (se houver)
        """
        update_expr = "SET #status = :status, updated_at = :updated_at"
        expr_values = {
            ':status': status.value,
            ':updated_at': datetime.utcnow().isoformat()
        }
        expr_names = {
            '#status': 'status'
        }
        
        if error:
            update_expr += ", error_message = :error"
            expr_values[':error'] = error
        
        if status == JourneyStatus.RETRYING:
            update_expr += " ADD retry_count :inc"
            expr_values[':inc'] = 1
        
        self._update_item(
            journey_id,
            update_expr,
            expr_values,
            expr_names
        )
        self.logger.info(f"Jornada {journey_id} atualizada para status: {status.value}")
    
    def add_step(self, journey_id: str, step_name: str, step_data: Optional[Dict] = None):
        """
        Adiciona uma etapa à jornada.
        
        Args:
            journey_id: ID da jornada
            step_name: Nome da etapa
            step_data: Dados adicionais da etapa
        """
        journey = self._get_item(journey_id)
        if not journey:
            raise ValueError(f"Jornada {journey_id} não encontrada.")
        
        step = {
            'name': step_name,
            'timestamp': datetime.utcnow().isoformat(),
            'data': step_data or {}
        }
        
        steps = journey.get('steps', [])
        steps.append(step)
        
        update_expr = "SET steps = :steps, updated_at = :updated_at"
        expr_values = {
            ':steps': steps,
            ':updated_at': datetime.utcnow().isoformat()
        }
        
        self._update_item(journey_id, update_expr, expr_values, None)
        self.logger.debug(f"Etapa '{step_name}' adicionada à jornada {journey_id}")
    
    def get_journey(self, journey_id: str) -> Optional[Dict]:
        """Recupera informações completas de uma jornada."""
        return self._get_item(journey_id)
    
    def execute_with_journey(
        self,
        func: Callable,
        journey_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        metadata: Optional[Dict] = None,
        *args,
        **kwargs
    ) -> Any:
        """
        Executa uma função dentro de uma jornada controlada com retry e idempotência.
        
        Args:
            func: Função a ser executada
            journey_id: ID da jornada (opcional)
            idempotency_key: Chave de idempotência
            metadata: Metadados da jornada
            *args, **kwargs: Argumentos para a função
        
        Returns:
            Resultado da função
        """
        journey_id = self.start_journey(journey_id, metadata, idempotency_key)
        
        journey = self._get_item(journey_id)
        if journey and journey.get('status') == JourneyStatus.COMPLETED.value:
            self.logger.info(f"Jornada {journey_id} já completada. Retornando resultado armazenado.")
            result = journey.get('result')
            # Se resultado foi salvo como JSON string, deserializar
            if isinstance(result, str):
                try:
                    result = json.loads(result)
                except (json.JSONDecodeError, TypeError):
                    pass
            return result
        
        self.update_status(journey_id, JourneyStatus.IN_PROGRESS)
        self.add_step(journey_id, "execution_started")
        
        last_error = None
        for attempt in range(self.max_retries):
            try:
                if attempt > 0:
                    self.update_status(journey_id, JourneyStatus.RETRYING)
                    delay = self.retry_delay * (2 ** (attempt - 1))  # Exponential backoff
                    self.logger.info(f"Tentativa {attempt + 1}/{self.max_retries} após {delay}s")
                    time.sleep(delay)
                
                result = func(*args, **kwargs)
                
                # Salvar resultado
                # Para tipos simples, salvar diretamente; para complexos, serializar como JSON
                result_to_store = result
                if not isinstance(result, (str, int, float, bool, type(None))):
                    try:
                        result_to_store = json.dumps(result)
                    except (TypeError, ValueError):
                        result_to_store = str(result)
                
                update_expr = "SET #status = :status, result = :result, updated_at = :updated_at"
                expr_values = {
                    ':status': JourneyStatus.COMPLETED.value,
                    ':result': result_to_store,
                    ':updated_at': datetime.utcnow().isoformat()
                }
                expr_names = {'#status': 'status'}
                self._update_item(journey_id, update_expr, expr_values, expr_names)
                self.add_step(journey_id, "execution_completed", {'result': str(result)})
                
                self.logger.info(f"Jornada {journey_id} completada com sucesso.")
                return result
                
            except Exception as e:
                last_error = str(e)
                self.logger.error(f"Erro na tentativa {attempt + 1} da jornada {journey_id}: {e}")
                self.add_step(journey_id, f"execution_error_attempt_{attempt + 1}", {'error': last_error})
        
        # Todas as tentativas falharam
        self.update_status(journey_id, JourneyStatus.FAILED, last_error)
        raise Exception(f"Jornada {journey_id} falhou após {self.max_retries} tentativas: {last_error}")
    
    def cleanup_old_journeys(self, days_old: int = 30):
        """
        Remove jornadas antigas (mais de X dias) do armazenamento.
        
        Args:
            days_old: Idade mínima em dias para remoção
        """
        cutoff_date = (datetime.utcnow() - timedelta(days=days_old)).isoformat()
        
        if not self.dynamodb:
            # Limpeza em memória
            to_remove = [
                journey_id for journey_id, journey in self._in_memory_store.items()
                if journey.get('created_at', '') < cutoff_date
            ]
            for journey_id in to_remove:
                del self._in_memory_store[journey_id]
            self.logger.info(f"Removidas {len(to_remove)} jornadas antigas do armazenamento em memória.")
            return
        
        # Em produção, seria necessário usar scan ou query com filtro
        # Por simplicidade, apenas logamos
        self.logger.info(f"Limpeza de jornadas antigas (>{days_old} dias) deve ser feita via script separado ou DynamoDB TTL.")


def journey_controlled(
    journey_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    metadata: Optional[Dict] = None
):
    """
    Decorator para executar funções dentro de uma jornada controlada.
    
    Exemplo:
        @journey_controlled(idempotency_key="process_data_2024-01-01")
        def process_data():
            # código aqui
            return result
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Obter controller do contexto ou criar um padrão
            controller = kwargs.pop('journey_controller', None)
            if not controller:
                raise ValueError("journey_controller deve ser fornecido via kwargs ou contexto")
            
            return controller.execute_with_journey(
                func,
                journey_id=journey_id,
                idempotency_key=idempotency_key,
                metadata=metadata,
                *args,
                **kwargs
            )
        return wrapper
    return decorator
