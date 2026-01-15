# Estrutura do Projeto - Documentação

## Organização Final

### 📁 src/

**Apenas o entry point da aplicação**

- `main.py`: Ponto único de entrada, orquestra toda a execução

### 📁 utils/

**Funcionalidades comuns organizadas em subpastas**

#### 📁 config/

- `settings.py`: Configurações centralizadas (variáveis de ambiente, valores padrão)

#### 📁 handlers/

- `glue_handler.py`: Handler para operações Glue/S3 (movido de src/)

#### 📁 business/

- `data_processor.py`: **Regra de negócio principal**
  - Demonstra como chamar múltiplas vezes sem impacto entre execuções
  - Cada chamada é isolada e idempotente
  - Exemplo de uso em `README.md`

#### Arquivos raiz de utils/

- `journey_controller.py`: Controle de jornada com idempotência
- `dynamodb_handler.py`: Handler DynamoDB para salvar congregado

### 📁 tests/

**Cobertura completa (98% TDD)**

#### 📁 unit/

- `test_main.py`: Testes do entry point
- `test_config.py`: Testes de configuração
- `test_glue_handler.py`: Testes do handler Glue
- `test_data_processor.py`: Testes da regra de negócio
- `test_journey_controller.py`: Testes do controller de jornada
- `test_dynamodb_handler.py`: Testes do handler DynamoDB

#### 📁 integration/

- `test_integration_glue.py`: Testes de integração

## Características Implementadas

### ✅ Isolamento de Execuções

A classe `DataProcessor` em `utils/business/data_processor.py` demonstra:

- Múltiplas chamadas não interferem entre si
- Cada execução tem seu próprio contexto
- Idempotência garantida via `JourneyController`

### ✅ Idempotência

- Todas as operações podem ser repetidas sem efeitos colaterais
- Chaves de idempotência únicas
- Verificação de estado antes de executar

### ✅ Resiliência

- Retry automático com backoff exponencial
- Recuperação de falhas
- Estado persistente no DynamoDB

### ✅ Cobertura de Testes (98%)

- Testes unitários completos
- Testes de integração
- Mocks apropriados
- Casos de erro cobertos

## Como Executar

### Testes

```bash
# Todos os testes com cobertura
./run_tests.sh

# Ou diretamente
pytest --cov=src --cov=utils --cov-fail-under=98
```

### Aplicação

```bash
python -m src.main
```

## Exemplo de Uso - Múltiplas Execuções

```python
from utils import DataProcessor, GlueDataHandler, JourneyController, DynamoDBHandler, AppConfig

# Setup (feito uma vez)
processor = DataProcessor(...)

# Execução 1 - Isolada
result1 = processor.process_data("db1", "table1", "s3://out1")

# Execução 2 - Paralela, não impacta execução 1
result2 = processor.process_data("db2", "table2", "s3://out2")

# Execução 3 - Idempotente (mesmos parâmetros)
result3 = processor.process_data("db1", "table1", "s3://out1")
# result3 == result1 (sem reprocessar)
```

## Configuração

Ver `pytest.ini` para configuração de testes:

- Cobertura mínima: 98%
- Relatórios: term, HTML, XML
- Caminhos: src/, utils/

## Dependências

Ver `requirements-dev.txt`:

- pytest
- pytest-cov (cobertura)
- boto3
- pyspark
- pandas, numpy
