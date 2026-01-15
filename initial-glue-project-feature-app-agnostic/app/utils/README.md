# Utils - Controle de Jornada e DynamoDB Handler

Este módulo contém classes para controle de jornada da aplicação e persistência idempotente no DynamoDB.

## Classes

### JourneyController

Classe para controle de jornada da aplicação com suporte a idempotência e resiliência a falhas.

**Características:**
- ✅ **Idempotência**: Execuções duplicadas não causam efeitos colaterais
- ✅ **Resiliência**: Recupera de falhas e permite retry automático
- ✅ **Rastreamento**: Mantém histórico de estados e etapas
- ✅ **Isolamento**: Cada jornada é independente

**Exemplo de uso:**

```python
from utils import JourneyController, JourneyStatus

# Inicializar controller
controller = JourneyController(
    table_name="journey_control",
    region_name="us-east-1",
    max_retries=3,
    retry_delay=2
)

# Iniciar jornada com idempotência
journey_id = controller.start_journey(
    idempotency_key="process_data_2024-01-01",
    metadata={"source": "glue_job"}
)

# Executar função com controle de jornada
def process_data():
    # Seu código aqui
    return {"result": "success"}

result = controller.execute_with_journey(
    process_data,
    journey_id=journey_id,
    idempotency_key="process_data_2024-01-01"
)
```

### DynamoDBHandler

Classe para salvar dados agregados (congregado) no DynamoDB de forma idempotente.

**Características:**
- ✅ **Idempotência**: Operações podem ser repetidas sem efeitos colaterais
- ✅ **Controle de versão**: Evita sobrescritas acidentais
- ✅ **Retry automático**: Tentativas com backoff exponencial
- ✅ **Batch operations**: Suporte a operações em lote

**Exemplo de uso:**

```python
from utils import DynamoDBHandler

# Inicializar handler
handler = DynamoDBHandler(
    table_name="congregado_data",
    region_name="us-east-1"
)

# Salvar congregado (idempotente)
congregado_data = {
    "total_vendas": 150000.50,
    "total_clientes": 500,
    "periodo": "2024-01"
}

result = handler.save_congregado(
    congregado_data=congregado_data,
    primary_key="vendas_2024-01",
    metadata={"source": "glue_job"}
)

# Recuperar congregado
retrieved = handler.get_congregado("vendas_2024-01")
```

## Integração

As classes podem ser usadas juntas para um controle completo:

```python
from utils import JourneyController, DynamoDBHandler

journey_controller = JourneyController(table_name="journey_control")
dynamodb_handler = DynamoDBHandler(table_name="congregado_data")

def process_and_save():
    # Processar dados
    processed_data = {"total": 1000}
    
    # Salvar no DynamoDB
    result = dynamodb_handler.save_congregado(
        congregado_data=processed_data,
        primary_key="data_2024-01"
    )
    return result

# Executar com controle de jornada
journey_id = journey_controller.start_journey(
    idempotency_key="process_and_save_2024-01"
)

result = journey_controller.execute_with_journey(
    process_and_save,
    journey_id=journey_id,
    idempotency_key="process_and_save_2024-01"
)
```

## Configuração do DynamoDB

### Tabela para JourneyController

Crie uma tabela com:
- **Chave primária**: `journey_id` (String)
- **Atributos sugeridos**: `status`, `created_at`, `updated_at`, `steps`, `retry_count`

### Tabela para DynamoDBHandler

Crie uma tabela com:
- **Chave primária**: `id` (String)
- **Índice GSI sugerido**: `idempotency_key` (String) - para buscas rápidas por idempotência
- **Atributos sugeridos**: `version`, `created_at`, `updated_at`, `metadata`

## Idempotência

Ambas as classes garantem idempotência:

1. **JourneyController**: Usa `idempotency_key` para identificar execuções duplicadas
2. **DynamoDBHandler**: Gera hash SHA256 dos dados para criar chave de idempotência única

Operações repetidas com os mesmos parâmetros não causam efeitos colaterais.

## Resiliência a Falhas

- **Retry automático**: Configurável com `max_retries` e `retry_delay`
- **Backoff exponencial**: Delay aumenta exponencialmente entre tentativas
- **Estado persistente**: Estados são salvos no DynamoDB, permitindo recuperação
- **Fallback em memória**: Funciona mesmo sem DynamoDB (útil para testes)

## Requisitos

- Python 3.7+
- boto3 (instalado via `requirements-dev.txt`)
- Tabelas DynamoDB configuradas (ou uso em memória para testes)

## Testes

As classes podem ser testadas sem DynamoDB real usando o modo em memória (fallback automático).
