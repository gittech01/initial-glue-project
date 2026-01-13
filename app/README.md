# AWS Glue Project - Estrutura Reorganizada

Projeto AWS Glue com arquitetura organizada seguindo boas práticas de engenharia de software.

## Estrutura do Projeto

```
app/
├── src/
│   └── main.py                 # Entry point da aplicação
├── utils/
│   ├── config/                  # Configurações
│   │   ├── __init__.py
│   │   └── settings.py
│   ├── handlers/                # Handlers de I/O
│   │   ├── __init__.py
│   │   └── glue_handler.py
│   ├── business/                # Regras de negócio
│   │   ├── __init__.py
│   │   └── data_processor.py
│   ├── journey_controller.py    # Controle de jornada
│   └── dynamodb_handler.py      # Handler DynamoDB
└── tests/
    ├── unit/                    # Testes unitários
    │   ├── test_main.py
    │   ├── test_config.py
    │   ├── test_glue_handler.py
    │   ├── test_data_processor.py
    │   ├── test_journey_controller.py
    │   └── test_dynamodb_handler.py
    └── integration/             # Testes de integração
        └── test_integration_glue.py
```

## Características

### 1. Entry Point (`src/main.py`)

- Ponto único de entrada da aplicação
- Inicializa contexto Glue
- Orquestra execução dos processos
- Integra todas as funcionalidades

### 2. Configurações (`utils/config/`)

- Centralização de configurações
- Suporte a variáveis de ambiente
- Valores padrão configuráveis

### 3. Handlers (`utils/handlers/`)

- Abstração de operações de I/O
- `GlueDataHandler`: Operações Glue/S3

### 4. Regras de Negócio (`utils/business/`)

- `DataProcessor`: Processamento de dados
- **Isolamento**: Cada execução é independente
- **Idempotência**: Múltiplas chamadas não impactam outras execuções
- Demonstração de como chamar várias vezes sem interferência

### 5. Controle de Jornada (`utils/journey_controller.py`)

- Rastreamento de execuções
- Idempotência garantida
- Retry automático
- Recuperação de falhas

### 6. DynamoDB Handler (`utils/dynamodb_handler.py`)

- Persistência idempotente
- Controle de versão
- Operações em lote

## Executando a Aplicação

### Ambiente Local

**Linux/Mac:**

```bash
# Ativar virtualenv
source venv/bin/activate

# Instalar dependências
pip install -r requirements-dev.txt

# Executar aplicação
python -m src.main
```

**Windows:**

```cmd
REM Ativar virtualenv
venv\Scripts\activate

REM Instalar dependências
pip install -r requirements-dev.txt

REM Executar aplicação
python -m src.main
```

### AWS Glue Job

```bash
# Configurar variáveis de ambiente
export JOURNEY_TABLE_NAME=journey_control
export CONGREGADO_TABLE_NAME=congregado_data
export AWS_REGION=us-east-1

# Executar job Glue
glue-job --job-name my-job \
  --database my_database \
  --table_name my_table \
  --output_path s3://my-bucket/output
```

## Executando Testes

### Script Multiplataforma (Recomendado)

**Windows, Linux e macOS:**

```bash
# Funciona em todos os sistemas operacionais
python run_tests.py
```

### Scripts Específicos

**Linux/Mac:**

```bash
./run_tests.sh
```

**Windows:**

```cmd
run_tests.bat
```

### Comandos Diretos

```bash
# Com cobertura (deve atingir 98%)
pytest

# Com relatório HTML
pytest --cov-report=html
# Abrir htmlcov/index.html
```

### Testes Unitários

```bash
pytest tests/unit/
```

### Testes de Integração

```bash
pytest tests/integration/
```

### Verificar Cobertura

```bash
# Cobertura no terminal
pytest --cov=src --cov=utils --cov-report=term-missing

# Cobertura HTML
pytest --cov=src --cov=utils --cov-report=html
```

> **Nota:** Para informações detalhadas sobre compatibilidade Windows, veja [COMPATIBILIDADE_WINDOWS.md](COMPATIBILIDADE_WINDOWS.md)

## Exemplo de Uso - Múltiplas Execuções Isoladas

A classe `DataProcessor` demonstra como processar dados de forma isolada:

```python
from utils import DataProcessor, GlueDataHandler, JourneyController, DynamoDBHandler, AppConfig

# Inicializar componentes
config = AppConfig()
glue_handler = GlueDataHandler(glue_context)
journey_controller = JourneyController(...)
dynamodb_handler = DynamoDBHandler(...)

processor = DataProcessor(
    glue_handler=glue_handler,
    journey_controller=journey_controller,
    dynamodb_handler=dynamodb_handler,
    config=config
)

# Execução 1 - Não impacta outras
result1 = processor.process_data("db1", "table1", "s3://bucket/output1")

# Execução 2 - Paralela, isolada
result2 = processor.process_data("db2", "table2", "s3://bucket/output2")

# Execução 3 - Mesmos parâmetros, idempotente
result3 = processor.process_data("db1", "table1", "s3://bucket/output1")

# Processar múltiplas tabelas
tables = [("db1", "table1"), ("db2", "table2"), ("db3", "table3")]
results = processor.process_multiple_tables(tables, "s3://bucket/output")
```

## Cobertura de Testes

O projeto mantém **98% de cobertura de testes** (TDD):

- ✅ Testes unitários para todos os módulos
- ✅ Testes de integração
- ✅ Mocks e fixtures apropriados
- ✅ Testes de casos de erro
- ✅ Testes de idempotência
- ✅ Testes de isolamento

## Configuração

### Variáveis de Ambiente

```bash
# AWS
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# DynamoDB Tables
export JOURNEY_TABLE_NAME=journey_control
export CONGREGADO_TABLE_NAME=congregado_data

# Glue
export GLUE_DATABASE=my_database
export OUTPUT_FORMAT=parquet

# Retry
export MAX_RETRIES=3
export RETRY_DELAY=2

# Processing
export BATCH_SIZE=1000
export ENABLE_PARTITIONING=true
```

## Dependências

Ver `requirements-dev.txt`:

- pytest
- pytest-cov (cobertura de testes)
- pandas
- numpy
- pyspark
- boto3

## Notas de Desenvolvimento

- **Idempotência**: Todas as operações são idempotentes
- **Isolamento**: Execuções não interferem entre si
- **Resiliência**: Retry automático e recuperação de falhas
- **Testabilidade**: 98% de cobertura garantida
- **Manutenibilidade**: Código organizado e documentado
