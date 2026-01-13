# Guia Completo: Como Adicionar uma Nova Regra de Negócio

Este guia detalha **passo a passo** como adicionar uma nova regra de negócio à aplicação, garantindo isolamento, idempotência e integração correta com todos os componentes.

## 📋 Índice

1. [Visão Geral do Fluxo da Aplicação](#1-visão-geral-do-fluxo-da-aplicação)
2. [Pontos Críticos de Atenção](#2-pontos-críticos-de-atenção)
3. [Passo a Passo Completo](#3-passo-a-passo-completo)
4. [Exemplo Prático: Nova Regra de Negócio](#4-exemplo-prático-nova-regra-de-negócio)
5. [Checklist Final](#5-checklist-final)

---

## 1. Visão Geral do Fluxo da Aplicação

### Fluxo Completo (Ponta a Ponta)

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ENTRY POINT: src/main.py                                 │
│    └─> initialize_glue_context()                            │
│        └─> Cria SparkContext, GlueContext, Job              │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. CONFIGURAÇÃO: AppConfig                                   │
│    └─> Carrega variáveis de ambiente                        │
│    └─> Define valores padrão                                │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. HANDLERS: Inicialização                                   │
│    ├─> GlueDataHandler (I/O Glue/S3)                        │
│    ├─> JourneyController (Controle de jornada)              │
│    └─> DynamoDBHandler (Persistência)                       │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. REGRA DE NEGÓCIO: DataProcessor (ou sua nova classe)      │
│    └─> Recebe handlers injetados                             │
│    └─> Implementa lógica de negócio                         │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. EXECUÇÃO COM JORNADA: journey_controller.execute_...    │
│    └─> Garante idempotência                                 │
│    └─> Rastreia execução                                    │
│    └─> Retry automático                                      │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 6. PROCESSAMENTO: Sua função de negócio                     │
│    ├─> Leitura de dados (GlueDataHandler)                   │
│    ├─> Transformação (sua lógica)                           │
│    ├─> Persistência (DynamoDBHandler)                        │
│    └─> Escrita (GlueDataHandler)                             │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 7. FINALIZAÇÃO: job.commit()                                 │
│    └─> Salva estado final                                   │
│    └─> Logs de conclusão                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. Pontos Críticos de Atenção

### ⚠️ **PONTO 1: Isolamento de Execuções**

**O QUE É:**

- Cada execução da regra de negócio deve ser **completamente independente**
- Execuções paralelas não devem interferir entre si
- Estado não deve ser compartilhado entre execuções

**COMO GARANTIR:**

- ✅ Use `idempotency_key` único para cada execução
- ✅ Não use variáveis de classe para estado
- ✅ Cada chamada deve ter seus próprios parâmetros
- ✅ Use `JourneyController` para rastreamento isolado

**EXEMPLO ERRADO:**

```python
class BadProcessor:
    shared_state = {}  # ❌ ERRADO: Estado compartilhado

    def process(self, data):
        self.shared_state['last_data'] = data  # ❌ Afeta outras execuções
```

**EXEMPLO CORRETO:**

```python
class GoodProcessor:
    def __init__(self, handlers):
        self.handlers = handlers  # ✅ Apenas dependências

    def process(self, data, idempotency_key):
        # ✅ Cada execução tem seu próprio contexto
        result = self._process_isolated(data, idempotency_key)
        return result
```

---

### ⚠️ **PONTO 2: Idempotência**

**O QUE É:**

- Executar a mesma operação múltiplas vezes deve produzir o mesmo resultado
- Não deve causar efeitos colaterais duplicados
- Deve ser seguro reexecutar após falhas

**COMO GARANTIR:**

- ✅ Use `JourneyController.execute_with_journey()` com `idempotency_key`
- ✅ Verifique estado antes de executar operações destrutivas
- ✅ Use `DynamoDBHandler.save_congregado()` que já é idempotente
- ✅ Não faça operações que dependem de estado externo mutável

**EXEMPLO ERRADO:**

```python
def process(self, data):
    # ❌ Sempre incrementa, não é idempotente
    counter = self.get_counter()
    self.set_counter(counter + 1)
```

**EXEMPLO CORRETO:**

```python
def process(self, data, idempotency_key):
    # ✅ Verifica se já foi executado
    if self.journey_controller.is_completed(idempotency_key):
        return self.journey_controller.get_result(idempotency_key)

    # ✅ Executa apenas se não foi executado antes
    result = self._do_process(data)
    return result
```

---

### ⚠️ **PONTO 3: Injeção de Dependências**

**O QUE É:**

- Handlers e controllers devem ser **injetados** no construtor
- Não crie instâncias dentro da classe de negócio
- Facilita testes e manutenção

**COMO GARANTIR:**

- ✅ Receba todos os handlers no `__init__`
- ✅ Não faça `new Handler()` dentro da classe
- ✅ Use os handlers injetados para todas as operações

**EXEMPLO ERRADO:**

```python
class BadProcessor:
    def process(self):
        handler = GlueDataHandler(...)  # ❌ Criado dentro
        handler.read_data()
```

**EXEMPLO CORRETO:**

```python
class GoodProcessor:
    def __init__(self, glue_handler, journey_controller, ...):
        self.glue_handler = glue_handler  # ✅ Injetado
        self.journey_controller = journey_controller

    def process(self):
        self.glue_handler.read_data()  # ✅ Usa o injetado
```

---

### ⚠️ **PONTO 4: Tratamento de Erros**

**O QUE É:**

- Erros devem ser tratados adequadamente
- Logs devem ser informativos
- Falhas não devem corromper estado

**COMO GARANTIR:**

- ✅ Use try/except em operações críticas
- ✅ Logue erros com contexto suficiente
- ✅ Re-raise exceções para o `JourneyController` fazer retry
- ✅ Não silencie exceções importantes

**EXEMPLO CORRETO:**

```python
def process(self, data):
    try:
        result = self._do_process(data)
        logger.info(f"Processamento concluído: {result}")
        return result
    except Exception as e:
        logger.error(f"Erro no processamento: {e}", exc_info=True)
        raise  # ✅ Re-raise para retry automático
```

---

### ⚠️ **PONTO 5: Integração com JourneyController**

**O QUE É:**

- Toda execução deve passar pelo `JourneyController`
- Isso garante idempotência, rastreamento e retry

**COMO GARANTIR:**

- ✅ Use `journey_controller.execute_with_journey()`
- ✅ Passe `idempotency_key` único
- ✅ Não execute diretamente, sempre via controller

**EXEMPLO ERRADO:**

```python
# ❌ Executa diretamente, sem controle de jornada
result = processor.process_data(...)
```

**EXEMPLO CORRETO:**

```python
# ✅ Executa via JourneyController
result = journey_controller.execute_with_journey(
    processor.process_data,
    idempotency_key="unique_key",
    database="db",
    table_name="table"
)
```

---

## 3. Passo a Passo Completo

### **PASSO 1: Criar a Classe de Regra de Negócio**

**Localização:** `utils/business/nova_regra_negocio.py`

```python
"""
Nova Regra de Negócio - Descrição do que faz.
"""
import logging
from typing import Dict, Optional
from pyspark.sql import DataFrame

from utils.handlers.glue_handler import GlueDataHandler
from utils.journey_controller import JourneyController
from utils.dynamodb_handler import DynamoDBHandler
from utils.config.settings import AppConfig

logger = logging.getLogger(__name__)


class NovaRegraNegocio:
    """
    Descrição da nova regra de negócio.

    Características:
    - Isolamento: Cada execução é independente
    - Idempotência: Execuções duplicadas não causam efeitos colaterais
    """

    def __init__(
        self,
        glue_handler: GlueDataHandler,
        journey_controller: JourneyController,
        dynamodb_handler: DynamoDBHandler,
        config: AppConfig
    ):
        """
        Inicializa a regra de negócio.

        IMPORTANTE: Recebe todas as dependências injetadas.
        """
        self.glue_handler = glue_handler
        self.journey_controller = journey_controller
        self.dynamodb_handler = dynamodb_handler
        self.config = config
        logger.info("NovaRegraNegocio inicializada")

    def processar_dados(
        self,
        database: str,
        table_name: str,
        output_path: Optional[str] = None,
        parametros_extras: Optional[Dict] = None
    ) -> Dict:
        """
        Processa dados conforme a nova regra de negócio.

        IMPORTANTE: Esta função será chamada via JourneyController,
        então deve ser pura (sem efeitos colaterais não controlados).

        Args:
            database: Nome do banco de dados
            table_name: Nome da tabela
            output_path: Caminho de saída (opcional)
            parametros_extras: Parâmetros adicionais específicos

        Returns:
            Dicionário com resultado do processamento
        """
        logger.info(f"Iniciando processamento: {database}.{table_name}")

        try:
            # ETAPA 1: Ler dados
            df = self.glue_handler.read_from_catalog(
                database=database,
                table_name=table_name
            )
            record_count = df.count()
            logger.info(f"Dados lidos: {record_count} registros")

            # ETAPA 2: Transformar (SUA LÓGICA AQUI)
            dados_transformados = self._transformar_dados(df, parametros_extras)
            logger.info("Dados transformados")

            # ETAPA 3: Salvar congregado (idempotente)
            congregado_result = self.dynamodb_handler.save_congregado(
                congregado_data=dados_transformados,
                primary_key=f"{database}_{table_name}",
                metadata={
                    'database': database,
                    'table_name': table_name,
                    'record_count': record_count
                }
            )
            logger.info(f"Congregado salvo: {congregado_result}")

            # ETAPA 4: Escrever resultado (se necessário)
            if output_path:
                self.glue_handler.write_to_s3(
                    df=df,  # ou dados_transformados
                    path=output_path,
                    format=self.config.default_output_format
                )
                logger.info("Dados escritos com sucesso")

            # Retornar resultado
            result = {
                'status': 'success',
                'record_count': record_count,
                'dados_transformados': dados_transformados,
                'congregado_id': congregado_result.get('id'),
                'output_path': output_path
            }

            logger.info(f"Processamento concluído: {result}")
            return result

        except Exception as e:
            logger.error(f"Erro no processamento: {e}", exc_info=True)
            raise  # Re-raise para retry automático

    def _transformar_dados(
        self,
        df: DataFrame,
        parametros: Optional[Dict] = None
    ) -> Dict:
        """
        Transforma dados conforme a lógica de negócio.

        IMPORTANTE: Esta é onde você implementa sua lógica específica.
        Mantenha esta função pura (sem efeitos colaterais).

        Args:
            df: DataFrame Spark
            parametros: Parâmetros opcionais

        Returns:
            Dicionário com dados transformados
        """
        # IMPLEMENTE SUA LÓGICA AQUI
        # Exemplo:
        total = df.count()

        # Suas transformações específicas...

        return {
            'total': total,
            # ... outros campos
        }
```

---

### **PASSO 2: Atualizar **init**.py**

**Arquivo:** `utils/business/__init__.py`

```python
"""Módulo de regras de negócio da aplicação."""

from .data_processor import DataProcessor
from .nova_regra_negocio import NovaRegraNegocio  # ✅ Adicionar

__all__ = ['DataProcessor', 'NovaRegraNegocio']  # ✅ Adicionar
```

---

### **PASSO 3: Integrar no main.py (Opcional)**

**Arquivo:** `src/main.py`

Se você quiser que a nova regra seja executada automaticamente, adicione:

```python
# Após criar o DataProcessor, adicione:
from utils.business.nova_regra_negocio import NovaRegraNegocio

# Criar instância da nova regra
nova_regra = NovaRegraNegocio(
    glue_handler=glue_handler,
    journey_controller=journey_controller,
    dynamodb_handler=dynamodb_handler,
    config=config
)

# Executar (se necessário)
# result = journey_controller.execute_with_journey(
#     nova_regra.processar_dados,
#     idempotency_key=f"nova_regra_{args.get('table_name')}",
#     database=args.get('database'),
#     table_name=args.get('table_name'),
#     output_path=args.get('output_path')
# )
```

**IMPORTANTE:** Geralmente você não precisa modificar `main.py`. A nova regra pode ser chamada de forma independente.

---

### **PASSO 4: Criar Testes Unitários**

**Arquivo:** `tests/unit/test_nova_regra_negocio.py`

```python
"""Testes unitários para NovaRegraNegocio."""
import unittest
from unittest.mock import MagicMock, patch
from utils.business.nova_regra_negocio import NovaRegraNegocio
from utils.config.settings import AppConfig

class TestNovaRegraNegocio(unittest.TestCase):
    """Testes para NovaRegraNegocio."""

    def setUp(self):
        """Setup para cada teste."""
        self.mock_glue_handler = MagicMock()
        self.mock_journey_controller = MagicMock()
        self.mock_dynamodb_handler = MagicMock()
        self.config = AppConfig()

        self.processor = NovaRegraNegocio(
            glue_handler=self.mock_glue_handler,
            journey_controller=self.mock_journey_controller,
            dynamodb_handler=self.mock_dynamodb_handler,
            config=self.config
        )

    def test_init(self):
        """Testa inicialização."""
        self.assertIsNotNone(self.processor)
        self.assertEqual(self.processor.glue_handler, self.mock_glue_handler)

    def test_processar_dados_success(self):
        """Testa processamento bem-sucedido."""
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        self.mock_glue_handler.read_from_catalog.return_value = mock_df

        # Mock DynamoDB
        self.mock_dynamodb_handler.save_congregado.return_value = {
            'id': 'test_id',
            'status': 'created'
        }

        result = self.processor.processar_dados(
            database="test_db",
            table_name="test_table"
        )

        self.assertEqual(result['status'], 'success')
        self.mock_glue_handler.read_from_catalog.assert_called_once()
        self.mock_dynamodb_handler.save_congregado.assert_called_once()

    def test_processar_dados_exception(self):
        """Testa tratamento de exceção."""
        self.mock_glue_handler.read_from_catalog.side_effect = Exception("Error")

        with self.assertRaises(Exception):
            self.processor.processar_dados("db", "table")

    # Adicione mais testes conforme necessário
```

---

### **PASSO 5: Criar Teste de Integração**

**Arquivo:** `tests/integration/test_nova_regra_negocio_integration.py`

```python
"""Teste de integração para NovaRegraNegocio."""
import pytest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("NovaRegraTest") \
        .master("local[1]") \
        .getOrCreate()

def test_nova_regra_integration(spark):
    """Testa integração completa."""
    # Setup real (modo em memória)
    from utils.business.nova_regra_negocio import NovaRegraNegocio
    from utils.handlers.glue_handler import GlueDataHandler
    from utils.journey_controller import JourneyController
    from utils.dynamodb_handler import DynamoDBHandler
    from utils.config.settings import AppConfig

    mock_glue = MagicMock()
    mock_glue.spark_session = spark

    glue_handler = GlueDataHandler(mock_glue)
    journey_controller = JourneyController("test_journey", dynamodb_client=None)
    dynamodb_handler = DynamoDBHandler("test_congregado", dynamodb_client=None)
    config = AppConfig()

    processor = NovaRegraNegocio(
        glue_handler=glue_handler,
        journey_controller=journey_controller,
        dynamodb_handler=dynamodb_handler,
        config=config
    )

    # Teste de isolamento
    data = [("test", 100)]
    df = spark.createDataFrame(data, ["name", "value"])

    with patch.object(glue_handler, 'read_from_catalog', return_value=df):
        result = processor.processar_dados("db", "table")
        assert result['status'] == 'success'
```

---

## 4. Exemplo Prático: Nova Regra de Negócio

Um exemplo completo foi criado em `utils/business/sales_analyzer.py` que você pode usar como template.

### Como Usar a Nova Regra de Negócio

```python
from utils import (
    GlueDataHandler, JourneyController, DynamoDBHandler, AppConfig
)
from utils.business.sales_analyzer import SalesAnalyzer

# 1. Inicializar componentes (feito no main.py normalmente)
config = AppConfig()
glue_handler = GlueDataHandler(glue_context)
journey_controller = JourneyController(
    table_name=config.journey_table_name,
    region_name=config.aws_region
)
dynamodb_handler = DynamoDBHandler(
    table_name=config.congregado_table_name,
    region_name=config.aws_region
)

# 2. Criar instância da nova regra
analyzer = SalesAnalyzer(
    glue_handler=glue_handler,
    journey_controller=journey_controller,
    dynamodb_handler=dynamodb_handler,
    config=config
)

# 3. Executar via JourneyController (GARANTE IDEMPOTÊNCIA)
result = journey_controller.execute_with_journey(
    analyzer.analisar_vendas,
    idempotency_key="vendas_2024-01_unique",
    metadata={'tipo': 'analise_vendas'},
    database="vendas_db",
    table_name="vendas",
    periodo="2024-01",
    output_path="s3://bucket/analises/2024-01"
)

# 4. Múltiplas execuções isoladas
# Execução 1 - Não impacta outras
result1 = journey_controller.execute_with_journey(
    analyzer.analisar_vendas,
    idempotency_key="vendas_2024-01",
    database="vendas_db",
    table_name="vendas",
    periodo="2024-01"
)

# Execução 2 - Paralela, isolada
result2 = journey_controller.execute_with_journey(
    analyzer.analisar_vendas,
    idempotency_key="vendas_2024-02",
    database="vendas_db",
    table_name="vendas",
    periodo="2024-02"
)

# Execução 3 - Idempotente (mesmos parâmetros)
result3 = journey_controller.execute_with_journey(
    analyzer.analisar_vendas,
    idempotency_key="vendas_2024-01",  # Mesma chave
    database="vendas_db",
    table_name="vendas",
    periodo="2024-01"
)
# result3 == result1 (sem reprocessar)
```

---

## 5. Checklist Final

Antes de considerar sua nova regra de negócio completa, verifique:

### ✅ **Checklist de Implementação**

- [ ] **Classe criada em `utils/business/`**

  - [ ] Nome descritivo e claro
  - [ ] Documentação completa (docstrings)
  - [ ] Imports corretos

- [ ] **Inicialização (`__init__`)**

  - [ ] Recebe todos os handlers injetados
  - [ ] Não cria instâncias internamente
  - [ ] Log de inicialização

- [ ] **Método principal de processamento**

  - [ ] Recebe parâmetros necessários
  - [ ] Retorna dicionário com resultado
  - [ ] Tratamento de erros com re-raise
  - [ ] Logs informativos em cada etapa

- [ ] **Lógica de negócio**

  - [ ] Implementada em método privado (`_nome_metodo`)
  - [ ] Função pura (sem efeitos colaterais)
  - [ ] Bem documentada

- [ ] **Idempotência**

  - [ ] Usa `JourneyController.execute_with_journey()`
  - [ ] `idempotency_key` único e consistente
  - [ ] Não causa efeitos colaterais duplicados

- [ ] **Isolamento**

  - [ ] Não usa variáveis de classe para estado
  - [ ] Cada execução é independente
  - [ ] Testado com múltiplas execuções paralelas

- [ ] **Integração**

  - [ ] Usa `GlueDataHandler` para I/O
  - [ ] Usa `DynamoDBHandler` para persistência
  - [ ] Usa `JourneyController` para controle
  - [ ] Usa `AppConfig` para configurações

- [ ] **Testes**

  - [ ] Testes unitários criados
  - [ ] Teste de integração criado
  - [ ] Testes de isolamento
  - [ ] Testes de idempotência
  - [ ] Testes de tratamento de erros
  - [ ] Cobertura >= 98%

- [ ] **Exportação**

  - [ ] Adicionado em `utils/business/__init__.py`
  - [ ] Incluído em `__all__`

- [ ] **Documentação**
  - [ ] Docstrings completas
  - [ ] Exemplos de uso
  - [ ] Comentários em código complexo

### ✅ **Checklist de Validação**

Execute estes testes para validar:

```bash
# 1. Testes unitários
pytest tests/unit/test_nova_regra_negocio.py -v

# 2. Testes de integração
pytest tests/integration/test_nova_regra_negocio_integration.py -v

# 3. Cobertura
pytest --cov=utils.business.nova_regra_negocio --cov-report=term-missing

# 4. Teste de isolamento manual
# Execute a mesma função múltiplas vezes e verifique que não interfere
```

### ✅ **Checklist de Boas Práticas**

- [ ] **Nomenclatura clara e consistente**
- [ ] **Código limpo e legível**
- [ ] **Sem código duplicado**
- [ ] **Logs apropriados (INFO, ERROR)**
- [ ] **Tratamento de edge cases**
- [ ] **Validação de parâmetros (se necessário)**
- [ ] **Performance considerada**

---

## 📝 Resumo dos Pontos Críticos

### 🔴 **NUNCA FAÇA:**

1. ❌ Criar handlers dentro da classe de negócio
2. ❌ Usar variáveis de classe para estado compartilhado
3. ❌ Executar diretamente sem `JourneyController`
4. ❌ Silenciar exceções importantes
5. ❌ Esquecer de passar `idempotency_key`
6. ❌ Fazer operações não-idempotentes sem verificação

### ✅ **SEMPRE FAÇA:**

1. ✅ Injetar todas as dependências no `__init__`
2. ✅ Usar `JourneyController.execute_with_journey()`
3. ✅ Passar `idempotency_key` único e consistente
4. ✅ Re-raise exceções para retry automático
5. ✅ Logar todas as etapas importantes
6. ✅ Testar isolamento e idempotência
7. ✅ Manter funções puras (sem efeitos colaterais)

---

## 🎯 Exemplo Completo de Fluxo

```
1. main.py inicializa tudo
   └─> Cria handlers e controllers

2. Cria instância da sua regra
   └─> NovaRegraNegocio(handlers...)

3. Executa via JourneyController
   └─> journey_controller.execute_with_journey(
           sua_regra.processar,
           idempotency_key="único",
           ...parâmetros
       )

4. JourneyController verifica idempotência
   └─> Se já executado, retorna resultado
   └─> Se não, executa e salva resultado

5. Sua função processa
   └─> Lê dados (GlueDataHandler)
   └─> Transforma (sua lógica)
   └─> Salva (DynamoDBHandler)
   └─> Escreve (GlueDataHandler)

6. JourneyController salva resultado
   └─> Status: COMPLETED
   └─> Resultado armazenado

7. Próxima execução idêntica
   └─> Retorna resultado armazenado (idempotente)
```

---

## 📚 Referências

- **Exemplo completo:** `utils/business/sales_analyzer.py`
- **Testes de exemplo:** `tests/unit/test_sales_analyzer.py`
- **Regra existente:** `utils/business/data_processor.py`
- **Documentação JourneyController:** `utils/journey_controller.py`
- **Documentação DynamoDBHandler:** `utils/dynamodb_handler.py`

---

**🎉 Parabéns! Agora você tem tudo que precisa para adicionar novas regras de negócio mantendo isolamento, idempotência e qualidade!**
