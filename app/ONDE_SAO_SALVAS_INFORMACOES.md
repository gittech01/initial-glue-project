# Onde São Salvas as Informações das Tabelas?

## 📊 RESUMO EXECUTIVO

A aplicação salva informações em **3 locais diferentes**, cada um com um propósito específico:

1. **Dados Consolidados (Resultado Final)** → **S3 + Glue Data Catalog**
2. **Congregados (Metadados de Processamento)** → **DynamoDB**
3. **Controle de Jornada (Estados de Execução)** → **DynamoDB**

---

## 1. DADOS CONSOLIDADOS (Resultado Final)

### 📍 Local: S3 + Glue Data Catalog

**Onde:**
- **S3**: Localização da tabela no catálogo Glue (obtida automaticamente)
- **Glue Catalog**: Tabela consolidada no banco de dados especificado

**Implementação:**
- **Arquivo**: `app/utils/business/flexible_consolidation_processor.py` (linhas 611-664)
- **Método**: `_write_output()`

**Condições de Salvamento:**

> **IMPORTANTE**: O método `_should_write_output()` verifica se há `tabela_consolidada` + `database` OU `output_path`. Se qualquer um estiver presente, o output será escrito.

#### Caso 1: Com `tabela_consolidada` + `database` (Recomendado)
```python
if tabela_consolidada and database:
    # Salva no S3 (localização da tabela no catálogo)
    # Atualiza o catálogo Glue com nova partição
    self.glue_handler.write_to_catalog(
        df=df_consolidado,
        database=database,
        table_name=tabela_consolidada,
        compression='snappy'
    )
```

**Onde é salvo:**
- **S3**: Caminho obtido do catálogo Glue (`table_location` da tabela)
- **Glue Catalog**: `{database}.{tabela_consolidada}`
- **Formato**: Parquet com compressão Snappy
- **Partição**: Baseada nas colunas de partição da tabela (geralmente `anomesdia`)

**Exemplo:**
- **Database**: `db_batch`
- **Tabela**: `tbl_processado_operacao_consolidada_n1`
- **S3**: `s3://bucket/db_batch/tbl_processado_operacao_consolidada_n1/anomesdia=20240119/`
- **Glue Catalog**: `db_batch.tbl_processado_operacao_consolidada_n1`

#### Caso 2: Apenas com `output_path`
```python
elif output_path:
    # Salva apenas no S3 (sem atualizar catálogo)
    self.glue_handler.write_to_s3(
        df=df_consolidado,
        path=output_path,
        format='parquet',
        compression='snappy'
    )
```

**Onde é salvo:**
- **S3**: Caminho especificado em `output_path`
- **Glue Catalog**: ❌ Não atualiza o catálogo
- **Formato**: Parquet com compressão Snappy

**Exemplo:**
- **Output Path**: `s3://bucket/output/consolidacao/20240119/`
- **Glue Catalog**: ❌ Não atualizado

---

## 2. CONGREGADOS (Metadados de Processamento)

### 📍 Local: DynamoDB

**Tabela DynamoDB:**
- **Nome**: `congregado_data` (configurável via `config.congregado_table_name`)
- **Região**: `sa-east-1` (configurável via `config.aws_region`)

**Implementação:**
- **Arquivo**: `app/utils/business/base_processor.py` (linhas 155-175)
- **Método**: `_save_congregado()`
- **Handler**: `app/utils/dynamodb_handler.py` (linhas 217-290)

**O que é salvo:**
```python
{
    'id': '{database}_{tabela_consolidada}',  # Chave primária
    'idempotency_key': 'hash_do_conteudo',     # Chave de idempotência
    'record_count': 1000,                      # Número de registros
    'tabela_consolidada': 'tbl_processado_operacao_consolidada_n1',
    'database': 'db_batch',
    'chaves_principais': ['num_oper', 'cod_idef_ver_oper'],
    'campos_decisao': ['dat_vlr_even_oper', 'num_prio_even_oper', 'dat_recm_even_oper'],
    'sample_data': [...],                      # Amostra dos dados (até 1000 registros)
    'processor_type': 'FlexibleConsolidationProcessor',
    'created_at': '2024-01-19T10:00:00',
    'updated_at': '2024-01-19T10:00:00',
    'metadata': {
        'processor_type': 'FlexibleConsolidationProcessor',
        'tabela_consolidada': 'tbl_processado_operacao_consolidada_n1',
        'database': 'db_batch',
        'origens': ['sor', 'sot']
    }
}
```

**Chave Primária:**
- Formato: `{database}_{tabela_consolidada}`
- Exemplo: `db_batch_tbl_processado_operacao_consolidada_n1`

**Propósito:**
- Rastreamento de processamentos executados
- Idempotência (evita reprocessamento)
- Auditoria e histórico

---

## 3. CONTROLE DE JORNADA (Estados de Execução)

### 📍 Local: DynamoDB

**Tabela DynamoDB:**
- **Nome**: `journey_control` (configurável via `config.journey_table_name`)
- **Região**: `sa-east-1` (configurável via `config.aws_region`)

**Implementação:**
- **Arquivo**: `app/utils/journey_controller.py` (linhas 323-450)
- **Método**: `execute_with_journey()`

**O que é salvo:**
```python
{
    'idempotency_key': 'hash_do_job_e_parametros',  # Chave de idempotência
    'status': 'COMPLETED',                           # Status da jornada
    'step': 'completed',                             # Etapa atual
    'started_at': '2024-01-19T10:00:00',
    'completed_at': '2024-01-19T10:05:00',
    'retry_count': 0,
    'metadata': {
        'processor_type': 'FlexibleConsolidationProcessor',
        'database': 'db_batch',
        'tabela_consolidada': 'tbl_processado_operacao_consolidada_n1'
    },
    'result': {...},                                 # Resultado do processamento
    'error': None                                    # Erro (se houver)
}
```

**Chave Primária:**
- Formato: `idempotency_key` (hash baseado em job + parâmetros)
- Exemplo: `hash(job_name + database + tabela_consolidada + ...)`

**Propósito:**
- Controle de execução idempotente
- Retry automático em caso de falha
- Rastreamento de estados (PENDING, IN_PROGRESS, COMPLETED, FAILED)
- Recuperação de processos interrompidos

---

## 📋 TABELA RESUMO

| Tipo de Dado | Local | Tabela/Nome | Chave Primária | Propósito |
|--------------|-------|-------------|----------------|-----------|
| **Dados Consolidados** | S3 + Glue Catalog | `{database}.{tabela_consolidada}` | Partição (`anomesdia`) | Resultado final da consolidação |
| **Congregados** | DynamoDB | `congregado_data` | `{database}_{tabela_consolidada}` | Metadados e amostra dos dados processados |
| **Controle de Jornada** | DynamoDB | `journey_control` | `idempotency_key` (hash) | Estados de execução e idempotência |

---

## 🔍 DETALHAMENTO POR TIPO

### 1. Dados Consolidados (S3 + Glue Catalog)

**Fluxo:**
1. Dados são processados e consolidados
2. Se `tabela_consolidada` + `database` fornecidos:
   - Obtém localização S3 da tabela do catálogo Glue
   - Salva dados no S3 (formato Parquet, compressão Snappy)
   - Atualiza catálogo Glue com nova partição
3. Se apenas `output_path` fornecido:
   - Salva dados no S3 no caminho especificado
   - ❌ Não atualiza catálogo Glue

**Código:**
```python
# app/utils/business/flexible_consolidation_processor.py:639-651
if tabela_consolidada and database:
    self.glue_handler.write_to_catalog(
        df=df_consolidado,
        database=database,
        table_name=tabela_consolidada,
        compression=compression
    )
```

**Localização S3:**
- Obtida automaticamente do catálogo Glue
- Formato: `s3://bucket/{database}/{table_name}/{partition_key}={value}/`
- Exemplo: `s3://bucket/db_batch/tbl_processado_operacao_consolidada_n1/anomesdia=20240119/`

---

### 2. Congregados (DynamoDB)

**Fluxo:**
1. Após transformação, dados são salvos como congregado
2. Chave primária: `{database}_{tabela_consolidada}`
3. Inclui amostra dos dados (até 1000 registros)
4. Idempotente: se já existe, retorna existente

**Código:**
```python
# app/utils/business/base_processor.py:155-175
def _save_congregado(self, transformed_data: Dict, **kwargs):
    primary_key = self._get_congregado_key(**kwargs)  # {database}_{tabela_consolidada}
    metadata = self._get_congregado_metadata(**kwargs)
    
    return self.dynamodb_handler.save_congregado(
        congregado_data=transformed_data,
        primary_key=primary_key,
        metadata=metadata
    )
```

**Tabela DynamoDB:**
- **Nome**: `congregado_data` (configurável)
- **Região**: `sa-east-1` (configurável)
- **Chave Primária**: `id` (string)

---

### 3. Controle de Jornada (DynamoDB)

**Fluxo:**
1. Antes de executar processamento, cria/atualiza jornada
2. Status inicial: `PENDING` → `IN_PROGRESS`
3. Após sucesso: `COMPLETED`
4. Em caso de falha: `FAILED` (com retry automático)

**Código:**
```python
# app/utils/journey_controller.py:323-450
def execute_with_journey(self, func, idempotency_key, **kwargs):
    # Cria/atualiza jornada no DynamoDB
    # Executa função
    # Atualiza status
```

**Tabela DynamoDB:**
- **Nome**: `journey_control` (configurável)
- **Região**: `sa-east-1` (configurável)
- **Chave Primária**: `idempotency_key` (string, hash)

---

## ✅ VALIDAÇÃO

### Configurações (settings.py):
- ✅ `journey_table_name = 'journey_control'`
- ✅ `congregado_table_name = 'congregado_data'`
- ✅ `aws_region = 'sa-east-1'`

### Locais de Persistência Confirmados:

1. **Dados Consolidados**:
   - ✅ S3: Localização da tabela no catálogo Glue
   - ✅ Glue Catalog: `{database}.{tabela_consolidada}`

2. **Congregados**:
   - ✅ DynamoDB: Tabela `congregado_data`
   - ✅ Chave: `{database}_{tabela_consolidada}`

3. **Controle de Jornada**:
   - ✅ DynamoDB: Tabela `journey_control`
   - ✅ Chave: `idempotency_key` (hash)

---

## 📝 EXEMPLO PRÁTICO

### Execução:
```python
processor.process(
    database='db_batch',
    tabela_consolidada='tbl_processado_operacao_consolidada_n1',
    output_path=None  # Opcional
)
```

### Onde são salvos:

1. **Dados Consolidados**:
   - **S3**: `s3://bucket/db_batch/tbl_processado_operacao_consolidada_n1/anomesdia=20240119/`
   - **Glue Catalog**: `db_batch.tbl_processado_operacao_consolidada_n1`

2. **Congregado**:
   - **DynamoDB**: Tabela `congregado_data`
   - **Chave**: `db_batch_tbl_processado_operacao_consolidada_n1`

3. **Jornada**:
   - **DynamoDB**: Tabela `journey_control`
   - **Chave**: `hash(job_name + db_batch + tbl_processado_operacao_consolidada_n1 + ...)`

---

## 🎯 CONCLUSÃO

**Todas as informações estão sendo salvas nos locais corretos:**

✅ **Dados Consolidados** → S3 + Glue Catalog (resultado final)  
✅ **Congregados** → DynamoDB (metadados e amostra)  
✅ **Controle de Jornada** → DynamoDB (estados de execução)

**Status: ✅ CONFIRMADO - Persistência correta**
