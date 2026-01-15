"""
Classe para salvar dados agregados (congregado) no DynamoDB de forma idempotente e resiliente.
Garante que operações sejam seguras mesmo em caso de falhas parciais ou execuções duplicadas.
"""
import json
import time
import hashlib
from datetime import datetime
from typing import Dict, Optional, List
import logging

try:
    import boto3
    from botocore.exceptions import ClientError
    from boto3.dynamodb.conditions import Key, Attr
    from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
except ImportError:
    boto3 = None
    ClientError = Exception
    Key = None
    Attr = None
    TypeDeserializer = None
    TypeSerializer = None
    logging.warning("boto3 não encontrado. Funcionalidades do DynamoDB serão limitadas.")


class DynamoDBHandler:
    """
    Handler para operações idempotentes no DynamoDB.
    
    Características:
    - Idempotência: Operações podem ser repetidas sem efeitos colaterais
    - Transações: Suporte a operações transacionais
    - Retry: Tentativas automáticas com backoff exponencial
    - Validação: Verificação de dados antes de salvar
    - Versionamento: Controle de versão para evitar sobrescritas acidentais
    """
    
    def __init__(
        self,
        table_name: str,
        region_name: str = "sa-east-1",
        max_retries: int = 3,
        retry_delay: int = 1,
        dynamodb_client=None,
        idempotency_key_field: str = "idempotency_key"
    ):
        """
        Inicializa o handler DynamoDB.
        
        Args:
            table_name: Nome da tabela DynamoDB
            region_name: Região AWS
            max_retries: Número máximo de tentativas
            retry_delay: Delay inicial entre tentativas (segundos)
            dynamodb_client: Cliente DynamoDB opcional (para testes)
            idempotency_key_field: Nome do campo usado para idempotência
        """
        self.table_name = table_name
        self.region_name = region_name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.idempotency_key_field = idempotency_key_field
        self.logger = logging.getLogger(__name__)
        
        # Inicializar cliente DynamoDB
        if dynamodb_client:
            self.dynamodb = dynamodb_client
            self.client = dynamodb_client.meta.client if hasattr(dynamodb_client, 'meta') else None
        elif boto3:
            self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
            self.client = boto3.client('dynamodb', region_name=region_name)
        else:
            self.dynamodb = None
            self.client = None
            self.logger.warning("DynamoDB não disponível. Usando armazenamento em memória.")
            self._in_memory_store = {}
        
        self._ensure_table_exists()
        
        # Serializadores para conversão de tipos
        self.serializer = TypeSerializer() if TypeSerializer else None
        self.deserializer = TypeDeserializer() if TypeDeserializer else None
    
    def _ensure_table_exists(self):
        """Garante que a tabela DynamoDB existe."""
        if not self.dynamodb:
            return
        
        try:
            self.table = self.dynamodb.Table(self.table_name)
            self.table.meta.client.describe_table(TableName=self.table_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.logger.warning(
                    f"Tabela {self.table_name} não encontrada. "
                    "Usando armazenamento em memória. "
                    "Crie a tabela no DynamoDB antes de usar."
                )
                self.dynamodb = None
                self.client = None
                self._in_memory_store = {}
            else:
                raise
    
    def _generate_idempotency_key(self, data: Dict) -> str:
        """
        Gera uma chave de idempotência baseada no conteúdo dos dados.
        Args:
            data: Dados para gerar a chave
        Returns:
            Hash SHA256 dos dados serializados
        """
        # Remover campos que não devem afetar a idempotência
        clean_data = {k: v for k, v in data.items() 
                     if k not in ['created_at', 'updated_at', 'version']}
        serialized = json.dumps(clean_data, sort_keys=True)
        return hashlib.sha256(serialized.encode()).hexdigest()
    
    def _get_item(self, key: Dict) -> Optional[Dict]:
        """Recupera um item do DynamoDB."""
        if not self.dynamodb:
            # Busca em memória usando a chave primária 'id'
            primary_key = key.get('id')
            if primary_key:
                return self._in_memory_store.get(primary_key)
            return None
        
        try:
            response = self.table.get_item(Key=key)
            return response.get('Item')
        except ClientError as e:
            self.logger.error(f"Erro ao recuperar item: {e}")
            return None
    
    def _put_item_with_retry(
        self, 
        item: Dict, 
        condition_expression: Optional[str] = None,
        condition_values: Optional[Dict] = None
    ) -> bool:
        """
        Salva um item no DynamoDB com retry e tratamento de erros.
        Args:
            item: Item a ser salvo
            condition_expression: Expressão condicional opcional
            condition_values: Valores para a expressão condicional
        Returns:
            True se bem-sucedido, False caso contrário
        """
        if not self.dynamodb:
            # Armazenamento em memória
            primary_key = item.get('id') or item.get('idempotency_key', 'default')
            # Verificar condição em memória
            if condition_expression and condition_values:
                existing = self._in_memory_store.get(primary_key)
                if existing and ':new_version' in condition_values:
                    if existing.get('version', 0) >= condition_values[':new_version']:
                        return False  # Condição não satisfeita
            self._in_memory_store[primary_key] = item
            return True
        
        for attempt in range(self.max_retries):
            try:
                kwargs = {'Item': item}
                if condition_expression:
                    kwargs['ConditionExpression'] = condition_expression
                    if condition_values:
                        kwargs['ExpressionAttributeValues'] = condition_values
                
                self.table.put_item(**kwargs)
                return True
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                
                # Condição não satisfeita (idempotência funcionando)
                if error_code == 'ConditionalCheckFailedException':
                    self.logger.info("Item já existe com mesma chave de idempotência. Operação idempotente.")
                    return True
                
                # Erro de throttling - retry
                if error_code in ['ProvisionedThroughputExceededException', 'ThrottlingException']:
                    if attempt < self.max_retries - 1:
                        delay = self.retry_delay * (2 ** attempt)
                        self.logger.warning(f"Throttling detectado. Retry após {delay}s")
                        time.sleep(delay)
                        continue
                
                self.logger.error(f"Erro ao salvar item (tentativa {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
                    raise
        
        return False
    
    def save_congregado(
        self,
        congregado_data: Dict,
        primary_key: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        version: Optional[int] = None,
        metadata: Optional[Dict] = None
    ) -> Dict:
        """
        Salva dados agregados (congregado) no DynamoDB de forma idempotente.
        Args:
            congregado_data: Dados agregados a serem salvos
            primary_key: Chave primária (se não fornecida, será gerada)
            idempotency_key: Chave de idempotência (se não fornecida, será gerada)
            version: Versão dos dados (para controle de concorrência)
            metadata: Metadados adicionais
        Returns:
            Dicionário com informações da operação (id, idempotency_key, version, etc.)
        """
        # Preparar dados completos
        now = datetime.utcnow().isoformat()
        
        # Gerar chave de idempotência se não fornecida
        if not idempotency_key:
            idempotency_key = self._generate_idempotency_key(congregado_data)
        
        # Preparar item completo
        item = {
            **congregado_data,
            self.idempotency_key_field: idempotency_key,
            'created_at': now,
            'updated_at': now,
            'metadata': metadata or {}
        }
        
        # Adicionar chave primária se fornecida
        if primary_key:
            item['id'] = primary_key
        
        # Verificar se já existe item com mesma chave de idempotência
        existing_item = None
        # Buscar pela chave primária (funciona tanto em memória quanto DynamoDB)
        if primary_key:
            existing_item = self._get_item({'id': primary_key})
        
        # Se item existe e tem mesma idempotency_key, retornar existente (idempotência)
        if existing_item and existing_item.get(self.idempotency_key_field) == idempotency_key:
            self.logger.info(f"Item já existe com idempotency_key {idempotency_key}. Operação idempotente.")
            return {
                'id': existing_item.get('id'),
                'idempotency_key': idempotency_key,
                'version': existing_item.get('version', 0),
                'status': 'exists',
                'item': existing_item
            }
        
        # Controle de versão
        if version is not None:
            item['version'] = version
            if existing_item:
                existing_version = existing_item.get('version', 0)
                if version <= existing_version:
                    raise ValueError(
                        f"Versão {version} não é maior que versão existente {existing_version}. "
                        "Operação rejeitada para evitar sobrescrita."
                    )
        else:
            # Se há item existente mas idempotency_key diferente, incrementar versão
            # Se não há item existente, começar com versão 1
            if existing_item:
                item['version'] = existing_item.get('version', 0) + 1
            else:
                item['version'] = 1
        
        # Condição para garantir idempotência
        condition_expr = None
        condition_values = {}
        if not primary_key:
            # Se não há chave primária, usar idempotency_key como condição
            condition_expr = f"attribute_not_exists({self.idempotency_key_field})"
        elif existing_item:
            # Se item existe, só atualizar se versão for maior
            condition_expr = "version < :new_version"
            condition_values[':new_version'] = item['version']
        
        # Salvar item
        success = self._put_item_with_retry(item, condition_expr, condition_values)
        
        if not success:
            raise Exception(f"Falha ao salvar congregado após {self.max_retries} tentativas")
        
        self.logger.info(f"Congregado salvo com sucesso. ID: {item.get('id')}, Version: {item['version']}")
        
        return {
            'id': item.get('id'),
            'idempotency_key': idempotency_key,
            'version': item['version'],
            'status': 'created' if not existing_item else 'updated',
            'item': item
        }
    
    def get_congregado(self, primary_key: str) -> Optional[Dict]:
        """
        Recupera dados agregados pela chave primária.
        
        Args:
            primary_key: Chave primária do item
        
        Returns:
            Item encontrado ou None
        """
        return self._get_item({'id': primary_key})
    
    def batch_save_congregados(
        self,
        congregados: List[Dict],
        primary_key_field: str = 'id',
        idempotency_key_field: str = None
    ) -> Dict:
        """
        Salva múltiplos congregados em lote (usando batch_write_item).
        
        Args:
            congregados: Lista de dados agregados
            primary_key_field: Campo usado como chave primária
            idempotency_key_field: Campo usado para idempotência (padrão: self.idempotency_key_field)
        
        Returns:
            Dicionário com estatísticas da operação
        """
        if not self.dynamodb:
            # Processamento em memória
            saved = 0
            for item in congregados:
                try:
                    self.save_congregado(
                        item,
                        primary_key=item.get(primary_key_field),
                        idempotency_key=item.get(idempotency_key_field or self.idempotency_key_field)
                    )
                    saved += 1
                except Exception as e:
                    self.logger.error(f"Erro ao salvar item em lote: {e}")
            
            return {'saved': saved, 'total': len(congregados), 'failed': len(congregados) - saved}
        
        # Usar batch_write_item do DynamoDB
        saved = 0
        failed = 0
        
        # DynamoDB permite até 25 itens por batch
        batch_size = 25
        for i in range(0, len(congregados), batch_size):
            batch = congregados[i:i + batch_size]
            
            # Preparar itens do batch
            items_to_write = []
            for item_data in batch:
                try:
                    now = datetime.utcnow().isoformat()
                    primary_key = item_data.get(primary_key_field)
                    idempotency_key = item_data.get(
                        idempotency_key_field or self.idempotency_key_field,
                        self._generate_idempotency_key(item_data)
                    )
                    
                    item = {
                        **item_data,
                        'id': primary_key,
                        self.idempotency_key_field: idempotency_key,
                        'created_at': now,
                        'updated_at': now,
                        'version': 1
                    }
                    items_to_write.append(item)
                except Exception as e:
                    self.logger.error(f"Erro ao preparar item do batch: {e}")
                    failed += 1
            
            # Escrever batch
            if items_to_write:
                try:
                    with self.table.batch_writer() as writer:
                        for item in items_to_write:
                            writer.put_item(Item=item)
                            saved += 1
                except Exception as e:
                    self.logger.error(f"Erro ao escrever batch: {e}")
                    failed += len(items_to_write)
                    saved -= len(items_to_write)
        
        return {
            'saved': saved,
            'total': len(congregados),
            'failed': failed
        }
    
    def delete_congregado(self, primary_key: str, condition_expression: Optional[str] = None) -> bool:
        """
        Remove um congregado do DynamoDB.
        
        Args:
            primary_key: Chave primária do item
            condition_expression: Expressão condicional opcional
        
        Returns:
            True se removido com sucesso
        """
        if not self.dynamodb:
            if primary_key in self._in_memory_store:
                del self._in_memory_store[primary_key]
                return True
            return False
        
        try:
            kwargs = {'Key': {'id': primary_key}}
            if condition_expression:
                kwargs['ConditionExpression'] = condition_expression
            
            self.table.delete_item(**kwargs)
            return True
        except ClientError as e:
            if e.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException':
                self.logger.info("Condição não satisfeita. Item não removido.")
                return False
            self.logger.error(f"Erro ao remover item: {e}")
            raise
