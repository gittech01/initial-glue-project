# Exemplo de Uso Completo - Sistema Agnóstico

## 🎯 Cenários de Uso

### **Cenário 1: Processamento Básico de Dados**

```bash
# Executar job com data_processor
glue-job \
  --JOB_NAME process_data_job \
  --processor_type data_processor \
  --database vendas \
  --table_name faturamento \
  --output_path s3://bucket/processed/faturamento
```

**O que acontece:**

1. `main.py` recebe `processor_type=data_processor`
2. `ProcessorFactory.create()` cria `DataProcessor`
3. `BusinessRuleOrchestrator.execute_rule()` executa
4. `BaseBusinessProcessor.process()` (template method) roda:
   - Lê dados do catálogo
   - Agrega dados
   - Salva congregado no DynamoDB
   - Escreve no S3

---

### **Cenário 2: Análise de Vendas**

```bash
# Executar job com sales_analyzer
glue-job \
  --JOB_NAME analyze_sales_job \
  --processor_type sales_analyzer \
  --database vendas \
  --table_name vendas \
  --periodo 2024-01 \
  --output_path s3://bucket/analises/2024-01
```

**O que acontece:**

1. `main.py` recebe `processor_type=sales_analyzer`
2. `ProcessorFactory.create()` cria `SalesAnalyzer`
3. Mesmo fluxo, mas com lógica específica:
   - Filtra por período
   - Calcula totais e médias
   - Agrupa por categoria
   - Salva análise no DynamoDB

---

### **Cenário 3: Múltiplas Regras em Paralelo**

```python
from utils.business import ProcessorFactory, BusinessRuleOrchestrator
from utils.config import AppConfig
from utils.handlers import GlueDataHandler
from utils import JourneyController, DynamoDBHandler

# Setup
config = AppConfig()
glue_handler = GlueDataHandler(glue_context)
journey_controller = JourneyController(...)
dynamodb_handler = DynamoDBHandler(...)

orchestrator = BusinessRuleOrchestrator(
    journey_controller=journey_controller,
    continue_on_error=True  # ← Não interrompe em caso de falha
)

# Criar múltiplos processadores
processor1 = ProcessorFactory.create('data_processor', ...)
processor2 = ProcessorFactory.create('sales_analyzer', ...)
processor3 = ProcessorFactory.create('inventory_processor', ...)

# Executar todos (isolados)
rules = [
    {
        'processor': processor1,
        'idempotency_key': 'data_2024-01',
        'database': 'vendas',
        'table_name': 'faturamento'
    },
    {
        'processor': processor2,
        'idempotency_key': 'sales_2024-01',
        'database': 'vendas',
        'table_name': 'vendas',
        'periodo': '2024-01'
    },
    {
        'processor': processor3,
        'idempotency_key': 'inventory_2024-01',
        'database': 'estoque',
        'table_name': 'produtos',
        'categoria': 'eletronicos'
    }
]

# Executar todos (falhas não interrompem outras)
results = orchestrator.execute_multiple_rules(rules)

# Resultado:
# {
#   'total': 3,
#   'successful': 2,
#   'failed': 1,
#   'skipped': 0,
#   'results': {
#     'DataProcessor': {'status': 'SUCCESS', ...},
#     'SalesAnalyzer': {'status': 'SUCCESS', ...},
#     'InventoryProcessor': {'status': 'FAILED', 'error': '...'}
#   }
# }
```

**Benefício:** Se `InventoryProcessor` falhar, `DataProcessor` e `SalesAnalyzer` continuam executando.

---

### **Cenário 4: Adicionar Nova Regra (Sem Modificar Código)**

#### **Passo 1: Criar Classe**

```python
# utils/business/customer_processor.py
from utils.business.base_processor import BaseBusinessProcessor

class CustomerProcessor(BaseBusinessProcessor):
    def _read_data(self, **kwargs):
        return self.glue_handler.read_from_catalog(
            database=kwargs['database'],
            table_name=kwargs['table_name']
        )

    def _transform_data(self, df, **kwargs):
        # Lógica específica de clientes
        return {
            'total_customers': df.count(),
            'active_customers': df.filter(F.col('status') == 'active').count(),
            ...
        }

    def _get_congregado_key(self, **kwargs):
        return f"customers_{kwargs['database']}_{kwargs['table_name']}"
```

#### **Passo 2: Registrar**

```python
# Em processor_factory.py ou no __init__.py
from utils.business.customer_processor import CustomerProcessor
ProcessorFactory.register('customer_processor', CustomerProcessor)
```

#### **Passo 3: Usar (Sem Modificar main.py!)**

```bash
glue-job \
  --processor_type customer_processor \
  --database clientes \
  --table_name clientes \
  --output_path s3://bucket/customers
```

**Pronto!** Sistema funciona sem modificar código existente.

---

## 🔄 Fluxo Resiliente (Não Interrompe)

### **Exemplo: Múltiplas Execuções com Falhas**

```python
orchestrator = BusinessRuleOrchestrator(
    continue_on_error=True,  # ← Chave para não interromper
    max_concurrent_failures=3
)

# Executar 5 regras
results = orchestrator.execute_multiple_rules([
    {'processor': p1, 'idempotency_key': 'key1', ...},  # ✅ Sucesso
    {'processor': p2, 'idempotency_key': 'key2', ...},  # ❌ Falha
    {'processor': p3, 'idempotency_key': 'key3', ...},  # ✅ Sucesso
    {'processor': p4, 'idempotency_key': 'key4', ...},  # ❌ Falha
    {'processor': p5, 'idempotency_key': 'key5', ...},  # ✅ Sucesso
])

# Resultado:
# {
#   'total': 5,
#   'successful': 3,  # ← 3 executaram com sucesso
#   'failed': 2,       # ← 2 falharam mas não interromperam
#   'results': {
#     'Processor1': {'status': 'SUCCESS', ...},
#     'Processor2': {'status': 'FAILED', 'error': '...'},  # ← Falhou mas não parou
#     'Processor3': {'status': 'SUCCESS', ...},
#     'Processor4': {'status': 'FAILED', 'error': '...'},  # ← Falhou mas não parou
#     'Processor5': {'status': 'SUCCESS', ...}
#   }
# }
```

**Benefício:** Sistema continua funcionando mesmo com falhas parciais.

---

## 📊 Comparação: Antes vs Depois

### **Antes (Acoplado):**

```python
# main.py
if args['tipo'] == 'data':
    processor = DataProcessor(...)
elif args['tipo'] == 'sales':
    processor = SalesAnalyzer(...)
else:
    raise ValueError("Tipo desconhecido")

# ❌ Problemas:
# - Precisa modificar main.py para cada nova regra
# - Código acoplado
# - Difícil testar
```

### **Depois (Agnóstico):**

```python
# main.py
processor = ProcessorFactory.create(
    processor_type=args.get('processor_type'),
    ...
)

# ✅ Benefícios:
# - Não precisa modificar main.py
# - Código desacoplado
# - Fácil testar
# - Fácil adicionar novas regras
```

---

## 🎯 Design Patterns em Ação

### **1. Factory Pattern**

```python
# Cria sem conhecer classe concreta
processor = ProcessorFactory.create('sales_analyzer', ...)
# ↑ Não sabe que é SalesAnalyzer, apenas o tipo
```

### **2. Template Method**

```python
# Todas as regras seguem mesmo template
class MinhaRegra(BaseBusinessProcessor):
    def _read_data(...):      # Hook
    def _transform_data(...): # Hook
    # process() já está implementado na base
```

### **3. Strategy Pattern**

```python
# Diferentes estratégias intercambiáveis
strategies = [
    ProcessorFactory.create('data_processor', ...),
    ProcessorFactory.create('sales_analyzer', ...),
    ProcessorFactory.create('inventory_processor', ...)
]
# Todas implementam mesma interface
```

### **4. Orchestrator Pattern**

```python
# Coordena múltiplas execuções
orchestrator.execute_multiple_rules(rules)
# Gerencia isolamento e resiliência
```

---

## ✅ Checklist de Uso

- [x] Sistema agnóstico para múltiplas regras
- [x] Factory Pattern para criação
- [x] Template Method para consistência
- [x] Orchestrator para resiliência
- [x] Fluxo não interrompe em caso de falha
- [x] SalesAnalyzer implementado
- [x] Exemplo de nova regra (InventoryProcessor)
- [x] Documentação completa

---

**🎉 Sistema pronto para produção com design patterns e arquitetura agnóstica!**
