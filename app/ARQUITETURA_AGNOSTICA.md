# Arquitetura Agnóstica - Múltiplas Regras de Negócio

## 🎯 Objetivo

Sistema completamente agnóstico que permite adicionar novas regras de negócio **sem modificar código existente**.

---

## 🏗️ Arquitetura Implementada

### **1. Base Abstrata (Template Method)**

**Arquivo:** `utils/business/base_processor.py`

```python
class BaseBusinessProcessor(ABC):
    def process(self, **kwargs):
        # Template: define fluxo padrão
        df = self._read_data(**kwargs)        # Hook
        data = self._transform_data(df, **kwargs)  # Hook
        self._save_congregado(data, **kwargs)
        self._write_output(df, data, **kwargs)  # Hook
```

**Benefício:** Todas as regras seguem o mesmo padrão, garantindo consistência.

---

### **2. Factory Pattern (Criação Agnóstica)**

**Arquivo:** `utils/business/processor_factory.py`

```python
# Registrar nova regra
ProcessorFactory.register('minha_regra', MinhaRegra)

# Criar instância (agnóstico)
processor = ProcessorFactory.create(
    processor_type='minha_regra',  # ← Configurável!
    glue_handler=handler,
    ...
)
```

**Benefício:** Código não conhece classes concretas, apenas o tipo.

---

### **3. Orchestrator (Execução Resiliente)**

**Arquivo:** `utils/business/orchestrator.py`

```python
orchestrator = BusinessRuleOrchestrator(
    journey_controller=controller,
    continue_on_error=True  # ← NÃO interrompe em caso de falha
)

result = orchestrator.execute_rule(
    processor=processor,
    idempotency_key='unique',
    **kwargs
)
# Se falhar e continue_on_error=True, retorna erro mas não interrompe
```

**Benefício:** Falhas não interrompem outras execuções.

---

### **4. Main Agnóstico**

**Arquivo:** `src/main.py`

```python
# Recebe processor_type como parâmetro
processor_type = args.get('processor_type', 'data_processor')

# Cria via Factory (agnóstico)
processor = ProcessorFactory.create(
    processor_type=processor_type,  # ← Qualquer tipo registrado
    ...
)

# Executa via Orchestrator (resiliente)
result = orchestrator.execute_rule(
    processor=processor,
    idempotency_key=...,
    **kwargs
)
```

**Benefício:** Main não conhece regras específicas, apenas orquestra.

---

## 📋 Como Adicionar Nova Regra de Negócio

### **Passo 1: Criar Classe**

```python
# utils/business/minha_regra.py
from utils.business.base_processor import BaseBusinessProcessor

class MinhaRegra(BaseBusinessProcessor):
    def _read_data(self, **kwargs):
        # Sua lógica de leitura
        return self.glue_handler.read_from_catalog(...)

    def _transform_data(self, df, **kwargs):
        # Sua lógica de transformação
        return {'meus': 'dados'}

    def _get_congregado_key(self, **kwargs):
        return f"minha_regra_{kwargs.get('id')}"
```

### **Passo 2: Registrar no Factory**

```python
# Em processor_factory.py ou no __init__.py
from utils.business.minha_regra import MinhaRegra
ProcessorFactory.register('minha_regra', MinhaRegra)
```

### **Passo 3: Usar (Sem Modificar main.py!)**

```bash
# Executar job
glue-job \
  --processor_type minha_regra \
  --database db \
  --table_name table \
  --meu_parametro_customizado valor
```

**Pronto!** Sem modificar código existente.

---

## 🔄 Fluxo Completo (Agnóstico)

```
1. main.py recebe --processor_type
   ↓
2. ProcessorFactory.create(processor_type)
   ├─> Busca no registry
   ├─> Cria instância (agnóstico)
   └─> Retorna processador
   ↓
3. BusinessRuleOrchestrator.execute_rule()
   ├─> Executa via JourneyController
   ├─> Se falhar e continue_on_error=True:
   │   └─> Retorna erro, NÃO interrompe
   └─> Se falhar e continue_on_error=False:
       └─> Propaga erro
   ↓
4. BaseBusinessProcessor.process() (Template)
   ├─> _read_data() (hook - implementado pela regra)
   ├─> _transform_data() (hook - implementado pela regra)
   ├─> _save_congregado() (comum)
   └─> _write_output() (hook - opcional)
```

---

## ✅ Regras de Negócio Disponíveis

### **1. DataProcessor** (`data_processor` ou `data`)

**Uso:**

```bash
glue-job \
  --processor_type data_processor \
  --database vendas \
  --table_name faturamento \
  --output_path s3://bucket/output
```

**Funcionalidade:** Processamento genérico de dados com agregação.

---

### **2. SalesAnalyzer** (`sales_analyzer` ou `sales`)

**Uso:**

```bash
glue-job \
  --processor_type sales_analyzer \
  --database vendas \
  --table_name vendas \
  --periodo 2024-01 \
  --output_path s3://bucket/analises
```

**Funcionalidade:** Análise de vendas por período com:

- Total de vendas
- Média de vendas
- Agrupamento por categoria
- Filtro por período

---

## 🛡️ Resiliência Implementada

### **Problema Anterior:**

- ❌ Falha em uma regra interrompia todo o fluxo
- ❌ Não havia recuperação automática
- ❌ Erros não eram isolados

### **Solução Implementada:**

- ✅ `BusinessRuleOrchestrator` com `continue_on_error=True`
- ✅ Falhas retornam resultado de erro, mas não interrompem
- ✅ Circuit Breaker protege contra falhas em cascata
- ✅ Cada regra é executada isoladamente

### **Exemplo:**

```python
# Executar múltiplas regras
rules = [
    {'processor': processor1, 'idempotency_key': 'key1', ...},
    {'processor': processor2, 'idempotency_key': 'key2', ...},
    {'processor': processor3, 'idempotency_key': 'key3', ...}
]

results = orchestrator.execute_multiple_rules(rules)
# Se processor2 falhar, processor1 e processor3 continuam executando
```

---

## 📊 Comparação: Antes vs Depois

| Aspecto              | Antes             | Depois                          |
| -------------------- | ----------------- | ------------------------------- |
| **Adicionar regra**  | Modificar main.py | Apenas criar classe e registrar |
| **Falha interrompe** | Sim               | Não (se continue_on_error=True) |
| **Código acoplado**  | Sim               | Não (Factory Pattern)           |
| **Testabilidade**    | Difícil           | Fácil (DI + Mocks)              |
| **Extensibilidade**  | Baixa             | Alta (Template Method)          |

---

## 🎯 Design Patterns Aplicados

1. **Template Method** → BaseBusinessProcessor
2. **Factory** → ProcessorFactory
3. **Strategy** → Cada regra de negócio
4. **Orchestrator** → BusinessRuleOrchestrator
5. **Circuit Breaker** → No orchestrator
6. **Dependency Injection** → Todas as classes

---

## 📝 Exemplo Completo: Nova Regra

```python
# 1. Criar classe
class InventoryProcessor(BaseBusinessProcessor):
    def _read_data(self, **kwargs):
        return self.glue_handler.read_from_catalog(
            database=kwargs['database'],
            table_name=kwargs['table_name']
        )

    def _transform_data(self, df, **kwargs):
        # Lógica específica de inventário
        return {'total_items': df.count(), ...}

    def _get_congregado_key(self, **kwargs):
        return f"inventory_{kwargs['database']}_{kwargs['table_name']}"

# 2. Registrar
ProcessorFactory.register('inventory', InventoryProcessor)

# 3. Usar (sem modificar main.py!)
# --processor_type inventory --database estoque --table_name produtos
```

---

## ✅ Checklist de Implementação

- [x] BaseBusinessProcessor criado (Template Method)
- [x] ProcessorFactory criado (Factory Pattern)
- [x] BusinessRuleOrchestrator criado (Orchestrator + Circuit Breaker)
- [x] DataProcessor refatorado para herdar de BaseBusinessProcessor
- [x] SalesAnalyzer refatorado e implementado corretamente
- [x] Main.py agnóstico (usa Factory)
- [x] Fluxo não interrompe em caso de falha (continue_on_error)
- [x] Testes criados para novos componentes
- [x] Documentação completa

---

**🎉 Sistema agora é 100% agnóstico e resiliente!**
