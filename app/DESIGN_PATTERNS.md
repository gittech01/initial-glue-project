# Design Patterns Aplicados - Engenharia de Dados

Este documento descreve os design patterns aplicados no projeto, seguindo melhores práticas de engenharia de dados em Python.

## 📐 Design Patterns Implementados

### 1. **Template Method Pattern** ✅

**Onde:** `utils/business/base_processor.py`

**Propósito:** Define o esqueleto do algoritmo de processamento, delegando etapas específicas para subclasses.

**Implementação:**

```python
class BaseBusinessProcessor(ABC):
    def process(self, **kwargs):
        # Template: define o fluxo padrão
        df = self._read_data(**kwargs)      # Hook method
        transformed = self._transform_data(df, **kwargs)  # Hook method
        self._save_congregado(transformed, **kwargs)
        self._write_output(df, transformed, **kwargs)  # Hook method
```

**Benefícios:**

- ✅ Consistência entre todas as regras de negócio
- ✅ Reutilização de código comum
- ✅ Facilita adicionar novas regras (apenas implementar hooks)
- ✅ Garante que todas seguem o mesmo fluxo

**Uso:**

```python
class DataProcessor(BaseBusinessProcessor):
    def _read_data(self, **kwargs):  # Implementa hook
        return self.glue_handler.read_from_catalog(...)

    def _transform_data(self, df, **kwargs):  # Implementa hook
        return self._aggregate_data(df)
```

---

### 2. **Factory Pattern** ✅

**Onde:** `utils/business/processor_factory.py`

**Propósito:** Centraliza a criação de processadores, permitindo adicionar novos tipos sem modificar código existente.

**Implementação:**

```python
class ProcessorFactory:
    _processors = {}  # Registry

    @classmethod
    def register(cls, name, processor_class):
        cls._processors[name] = processor_class

    @classmethod
    def create(cls, processor_type, **dependencies):
        return cls._processors[processor_type](**dependencies)
```

**Benefícios:**

- ✅ Desacoplamento: código não conhece classes concretas
- ✅ Extensibilidade: adicionar novos tipos sem modificar código
- ✅ Testabilidade: fácil mockar factory
- ✅ Registry pattern: auto-registro de processadores

**Uso:**

```python
# Registrar processador
ProcessorFactory.register('sales_analyzer', SalesAnalyzer)

# Criar instância
processor = ProcessorFactory.create(
    processor_type='sales_analyzer',
    glue_handler=handler,
    ...
)
```

---

### 3. **Strategy Pattern** ✅

**Onde:** `utils/business/` (todas as regras de negócio)

**Propósito:** Cada regra de negócio é uma estratégia diferente de processamento.

**Implementação:**

```python
# Estratégia 1
class DataProcessor(BaseBusinessProcessor):
    def _transform_data(self, df, **kwargs):
        return self._aggregate_data(df)

# Estratégia 2
class SalesAnalyzer(BaseBusinessProcessor):
    def _transform_data(self, df, **kwargs):
        return self._analisar_dados(df, kwargs['periodo'])
```

**Benefícios:**

- ✅ Intercambiabilidade: trocar estratégias em runtime
- ✅ Isolamento: cada estratégia é independente
- ✅ Extensibilidade: adicionar novas estratégias facilmente

---

### 4. **Dependency Injection** ✅

**Onde:** Todo o projeto

**Propósito:** Dependências são injetadas, não criadas internamente.

**Implementação:**

```python
class BaseBusinessProcessor:
    def __init__(
        self,
        glue_handler: GlueDataHandler,      # Injetado
        journey_controller: JourneyController,  # Injetado
        dynamodb_handler: DynamoDBHandler,  # Injetado
        config: AppConfig                   # Injetado
    ):
        self.glue_handler = glue_handler
        # ...
```

**Benefícios:**

- ✅ Testabilidade: fácil injetar mocks
- ✅ Flexibilidade: trocar implementações facilmente
- ✅ Desacoplamento: classes não dependem de implementações concretas

---

### 5. **Orchestrator Pattern** ✅

**Onde:** `utils/business/orchestrator.py`

**Propósito:** Coordena execução de múltiplas regras de negócio de forma resiliente.

**Implementação:**

```python
class BusinessRuleOrchestrator:
    def execute_rule(self, processor, idempotency_key, **kwargs):
        try:
            result = self.journey_controller.execute_with_journey(...)
            return {'status': 'SUCCESS', 'result': result}
        except Exception as e:
            if self.continue_on_error:
                return {'status': 'FAILED', 'error': str(e)}
            raise
```

**Benefícios:**

- ✅ Resiliência: falhas não interrompem outras execuções
- ✅ Coordenação: gerencia múltiplas regras
- ✅ Circuit Breaker: protege contra falhas em cascata

---

### 6. **Circuit Breaker Pattern** ✅

**Onde:** `utils/business/orchestrator.py`

**Propósito:** Previne execuções repetidas após muitas falhas consecutivas.

**Implementação:**

```python
class BusinessRuleOrchestrator:
    def __init__(self, max_concurrent_failures=3):
        self.consecutive_failures = 0
        self.max_concurrent_failures = max_concurrent_failures

    def execute_rule(self, ...):
        if self.consecutive_failures >= self.max_concurrent_failures:
            raise Exception("Circuit breaker aberto")
```

**Benefícios:**

- ✅ Proteção: evita sobrecarga em caso de falhas sistemáticas
- ✅ Recuperação: permite reset após período

---

### 7. **Repository Pattern** ✅

**Onde:** `utils/handlers/glue_handler.py`, `utils/dynamodb_handler.py`

**Propósito:** Abstrai acesso a dados, isolando lógica de negócio de detalhes de persistência.

**Implementação:**

```python
class GlueDataHandler:
    def read_from_catalog(self, database, table_name):
        # Abstrai detalhes do Glue

class DynamoDBHandler:
    def save_congregado(self, data, primary_key):
        # Abstrai detalhes do DynamoDB
```

**Benefícios:**

- ✅ Abstração: negócio não conhece detalhes de I/O
- ✅ Testabilidade: fácil mockar repositórios
- ✅ Flexibilidade: trocar implementação sem afetar negócio

---

### 8. **Singleton Pattern** (Implícito) ✅

**Onde:** `utils/config/settings.py`

**Propósito:** Configurações são carregadas uma vez e reutilizadas.

**Implementação:**

```python
config = AppConfig()  # Pode ser instanciado múltiplas vezes, mas carrega mesmo estado
```

---

## 🏗️ Arquitetura Geral

```
┌─────────────────────────────────────────────────────────┐
│  Entry Point: main.py                                  │
│  - Factory Pattern: Cria processadores                 │
│  - Orchestrator: Coordena execuções                     │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  ProcessorFactory (Factory Pattern)                     │
│  - Registry de processadores                            │
│  - Cria instâncias agnósticas                           │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  BaseBusinessProcessor (Template Method)                 │
│  - Define template de processamento                     │
│  - Hook methods para subclasses                         │
└─────────────────────────────────────────────────────────┘
        ↓                    ↓
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ DataProcessor│    │SalesAnalyzer │    │  NovaRegra   │
│ (Strategy)   │    │ (Strategy)   │    │ (Strategy)   │
└──────────────┘    └──────────────┘    └──────────────┘
        ↓                    ↓                    ↓
┌─────────────────────────────────────────────────────────┐
│  BusinessRuleOrchestrator                               │
│  - Circuit Breaker                                      │
│  - Fail-Safe (não interrompe fluxo)                    │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│  JourneyController                                      │
│  - Idempotência                                         │
│  - Retry automático                                     │
└─────────────────────────────────────────────────────────┘
```

---

## 🎯 Benefícios dos Patterns Aplicados

### **Extensibilidade**

- ✅ Adicionar nova regra de negócio: apenas criar classe e registrar
- ✅ Não precisa modificar código existente
- ✅ Factory gerencia criação automaticamente

### **Manutenibilidade**

- ✅ Código organizado e separado por responsabilidade
- ✅ Fácil localizar e modificar funcionalidades
- ✅ Testes isolados por componente

### **Resiliência**

- ✅ Falhas não interrompem outras execuções
- ✅ Retry automático com backoff exponencial
- ✅ Circuit breaker protege contra falhas em cascata

### **Testabilidade**

- ✅ Dependency Injection facilita mocks
- ✅ Cada componente pode ser testado isoladamente
- ✅ Factory pode ser mockado facilmente

### **Agnóstico**

- ✅ Sistema não conhece regras de negócio específicas
- ✅ Novas regras adicionadas sem modificar código core
- ✅ Configuração via parâmetros (processor_type)

---

## 📝 Como Adicionar Nova Regra de Negócio

### Passo 1: Criar classe herdando de BaseBusinessProcessor

```python
from utils.business.base_processor import BaseBusinessProcessor

class MinhaNovaRegra(BaseBusinessProcessor):
    def _read_data(self, **kwargs):
        # Implementar leitura
        pass

    def _transform_data(self, df, **kwargs):
        # Implementar transformação
        pass

    def _get_congregado_key(self, **kwargs):
        # Gerar chave única
        pass
```

### Passo 2: Registrar no Factory

```python
# Em processor_factory.py ou no __init__.py do módulo
from utils.business.minha_regra import MinhaNovaRegra
ProcessorFactory.register('minha_regra', MinhaNovaRegra)
```

### Passo 3: Usar no main.py

```python
# main.py já é agnóstico, apenas passar processor_type
# --processor_type minha_regra
```

**Pronto!** Sem modificar código existente.

---

## 🔄 Fluxo de Execução Resiliente

```
1. main.py recebe processor_type
   ↓
2. ProcessorFactory.create() cria processador
   ↓
3. BusinessRuleOrchestrator.execute_rule()
   ↓
4. JourneyController.execute_with_journey()
   ├─> Verifica idempotência
   ├─> Se já executado: retorna resultado
   └─> Se não: executa com retry
   ↓
5. BaseBusinessProcessor.process() (Template Method)
   ├─> _read_data() (hook)
   ├─> _transform_data() (hook)
   ├─> _save_congregado()
   └─> _write_output() (hook)
   ↓
6. Se erro:
   ├─> continue_on_error=True: retorna erro, não interrompe
   └─> continue_on_error=False: propaga erro
```

---

## ✅ Checklist de Patterns

- [x] Template Method: BaseBusinessProcessor
- [x] Factory: ProcessorFactory
- [x] Strategy: Cada regra de negócio
- [x] Dependency Injection: Todas as classes
- [x] Orchestrator: BusinessRuleOrchestrator
- [x] Circuit Breaker: No orchestrator
- [x] Repository: Handlers de I/O
- [x] Fail-Safe: Orchestrator não interrompe fluxo

---

## 📚 Referências

- **Gang of Four**: Design Patterns: Elements of Reusable Object-Oriented Software
- **Python Best Practices**: PEP 8, Clean Code principles
- **Data Engineering Patterns**: ETL/ELT patterns, idempotency patterns
