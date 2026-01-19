# 📊 Tabelas e Locais de Persistência de Dados

**Data**: 2025-01-19  
**Aplicação**: AWS Glue - Consolidação Flexível

Este documento lista **todos os lugares** onde as informações são salvas/persistidas pela aplicação.

---

## 🗄️ 1. DYNAMODB - Tabelas de Controle e Metadados

### 1.1. Tabela: `journey_control` (Controle de Jornada)

**Localização**: DynamoDB  
**Nome Configurável**: `config.journey_table_name` (padrão: `'journey_control'`)  
**Região**: `config.aws_region` (padrão: `'sa-east-1'`)

**Responsabilidade**:  
Controla o ciclo de vida das execuções de processos de negócio, garantindo idempotência e rastreabilidade.

**O que é salvo**:
- `journey_id`: Identificador único da jornada
- `status`: Status da execução (`pending`, `in_progress`, `completed`, `failed`)
- `created_at`: Data/hora de criação
- `updated_at`: Data/hora da última atualização
- `metadata`: Metadados adicionais (job_name, processor_type, etc.)
- `steps`: Histórico de etapas executadas
- `error`: Mensagem de erro (se houver)

**Arquivo**: `app/utils/journey_controller.py`  
**Método**: `_put_item()`, `_update_item()`

**Exemplo de Item**:
```json
{
  "journey_id": "consolidacao_job1_tbl_processado_operacao_consolidada_n1_db_sor",
  "status": "completed",
  "created_at": "2025-01-19T10:00:00",
  "updated_at": "2025-01-19T10:05:00",
  "metadata": {
    "job_name": "job1",
    "processor_type": "flexible_consolidation",
    "database": "db_sor",
    "tabela_consolidada": "tbl_processado_operacao_consolidada_n1"
  },
  "steps": [
    {"step": "read", "status": "completed", "timestamp": "2025-01-19T10:01:00"},
    {"step": "transform", "status": "completed", "timestamp": "2025-01-19T10:02:00"},
    {"step": "save", "status": "completed", "timestamp": "2025-01-19T10:03:00"}
  ]
}
```

---

### 1.2. Tabela: `congregado_data` (Dados Consolidados)

**Localização**: DynamoDB  
**Nome Configurável**: `config.congregado_table_name` (padrão: `'congregado_data'`)  
**Região**: `config.aws_region` (padrão: `'sa-east-1'`)

**Responsabilidade**:  
Armazena dados agregados/consolidados de forma idempotente, permitindo consulta rápida e controle de versão.

**O que é salvo**:
- `id`: Chave primária (formato: `{database}_{tabela_consolidada}`)
- `idempotency_key`: Chave de idempotência (evita duplicatas)
- `version`: Versão dos dados (controle de concorrência)
- `created_at`: Data/hora de criação
- `updated_at`: Data/hora da última atualização
- `metadata`: Metadados (processor_type, tabela_consolidada, database, origens)
- `record_count`: Número de registros processados
- `sample_data`: Amostra dos dados (primeiros 1000 registros)
- `df_consolidado`: **NÃO é salvo** (DataFrame não é serializável, apenas metadados)

**Arquivo**: `app/utils/dynamodb_handler.py`  
**Método**: `save_congregado()`, `batch_save_congregados()`

**Exemplo de Item**:
```json
{
  "id": "db_sor_tbl_processado_operacao_consolidada_n1",
  "idempotency_key": "hash_do_conteudo_dos_dados",
  "version": 1,
  "created_at": "2025-01-19T10:00:00",
  "updated_at": "2025-01-19T10:05:00",
  "metadata": {
    "processor_type": "FlexibleConsolidationProcessor",
    "tabela_consolidada": "tbl_processado_operacao_consolidada_n1",
    "database": "db_sor",
    "origens": ["sor", "sot"]
  },
  "record_count": 1500,
  "sample_data": [
    {
      "num_oper": 12345,
      "cod_idef_ver_oper": "v1",
      "dat_vlr_even_oper": "2024-01-01",
      "num_prio_even_oper": 5,
      "dat_recm_even_oper": "2024-01-01 09:00:00",
      "origem": "online"
    }
  ]
}
```

**Observação Importante**:  
O DataFrame completo (`df_consolidado`) **NÃO é salvo** no DynamoDB, apenas metadados e uma amostra. O DataFrame completo é salvo no Glue Data Catalog ou S3 (ver seções 2 e 3).

---

## 📁 2. AWS GLUE DATA CATALOG - Tabelas de Saída

### 2.1. Tabelas Consolidadas (Resultado Final)

**Localização**: AWS Glue Data Catalog  
**Database**: Configurável via parâmetro `database` (ex: `db_sor`, `db_sot`)  
**Tabelas**: Definidas em `config.consolidacoes_tabelas`

**Tabelas Configuradas** (exemplos):
- `tbl_processado_operacao_consolidada_n1`
- `tbl_processado_operacao_consolidada_n2`
- `tbl_processado_operacao_consolidada_n3`
- `tbl_processado_operacao_consolidada_n4`

**Responsabilidade**:  
Armazena os dados consolidados finais após aplicação da regra de negócio (ranking, joins, etc.).

**O que é salvo**:
- **Todas as colunas** da tabela principal original (após join final)
- **Coluna `origem`**: `'online'` (SoR) ou `'batch'` (SoT)
- **Partição `anomesdia`**: Data da execução (adicionada automaticamente pelo Glue)

**Arquivo**: `app/utils/business/flexible_consolidation_processor.py`  
**Método**: `_write_output()` → `glue_handler.write_to_catalog()`

**Condição para Salvar**:  
- `tabela_consolidada` deve ser fornecido nos kwargs
- `database` deve ser fornecido nos kwargs
- `output_path` **NÃO** deve ser fornecido (se fornecido, salva no S3)

**Exemplo de Estrutura**:
```sql
CREATE TABLE db_sor.tbl_processado_operacao_consolidada_n1 (
  num_oper INT,
  cod_idef_ver_oper STRING,
  dat_vlr_even_oper TIMESTAMP,
  num_prio_even_oper INT,
  dat_recm_even_oper TIMESTAMP,
  -- ... todas as outras colunas da tabela principal ...
  origem STRING,
  anomesdia STRING  -- partição
)
PARTITIONED BY (anomesdia)
STORED AS PARQUET
LOCATION 's3://bucket/db_sor/tbl_processado_operacao_consolidada_n1/'
```

**Arquivo Físico**:  
Os dados são salvos fisicamente no **S3** (localização definida no Glue Data Catalog), mas acessados via tabela no catálogo.

---

## 🪣 3. AMAZON S3 - Armazenamento de Dados

### 3.1. Tabelas Consolidadas (via Glue Data Catalog)

**Localização**: S3  
**Caminho**: Definido no Glue Data Catalog para cada tabela  
**Formato**: Parquet (padrão) ou configurável via `config.default_output_format`

**Estrutura de Diretórios**:
```
s3://bucket/
  └── db_sor/
      └── tbl_processado_operacao_consolidada_n1/
          └── anomesdia=20250119/
              └── part-00000-xxx.parquet
              └── part-00001-xxx.parquet
              └── ...
```

**Responsabilidade**:  
Armazenamento físico dos dados consolidados. Acesso via Glue Data Catalog.

**Arquivo**: `app/utils/handlers/glue_handler.py`  
**Método**: `write_to_catalog()` → `glue_context.write_dynamic_frame.from_catalog()`

---

### 3.2. Output Direto no S3 (quando `output_path` é fornecido)

**Localização**: S3  
**Caminho**: Fornecido via parâmetro `output_path` (ex: `s3://bucket/output/`)  
**Formato**: Parquet (padrão) ou configurável via `config.default_output_format`

**Responsabilidade**:  
Salvar dados consolidados diretamente no S3 sem atualizar o Glue Data Catalog.

**Arquivo**: `app/utils/business/flexible_consolidation_processor.py`  
**Método**: `_write_output()` → `glue_handler.write_to_s3()`

**Condição para Salvar**:  
- `output_path` deve ser fornecido nos kwargs
- Se `tabela_consolidada` também for fornecido, **prioriza** o catálogo (não salva no S3)

**Exemplo de Caminho**:
```
s3://my-bucket/consolidated-data/
  └── part-00000-xxx.parquet
  └── part-00001-xxx.parquet
  └── ...
```

**Com Partições** (se `partition_cols` for fornecido):
```
s3://my-bucket/consolidated-data/
  └── origem=online/
      └── part-00000-xxx.parquet
  └── origem=batch/
      └── part-00000-xxx.parquet
```

---

## 📋 RESUMO DAS PERSISTÊNCIAS

| # | Local | Tabela/Caminho | O que é Salvo | Quando é Salvo |
|---|-------|----------------|---------------|----------------|
| **1** | **DynamoDB** | `journey_control` | Metadados de execução, status, histórico | Sempre (via `JourneyController`) |
| **2** | **DynamoDB** | `congregado_data` | Metadados consolidados, amostra de dados | Sempre (via `BaseBusinessProcessor._save_congregado()`) |
| **3** | **Glue Catalog** | `{database}.{tabela_consolidada}` | Dados consolidados completos | Se `tabela_consolidada` e `database` fornecidos, **sem** `output_path` |
| **4** | **S3** | Via Glue Catalog | Dados físicos das tabelas do catálogo | Automaticamente quando salva no catálogo |
| **5** | **S3** | `output_path` fornecido | Dados consolidados completos | Se `output_path` fornecido e **sem** `tabela_consolidada` |

---

## 🔍 FLUXO DE PERSISTÊNCIA

```
┌─────────────────────────────────────────────────────────────┐
│ 1. EXECUÇÃO INICIA                                          │
│    └─> JourneyController salva em DynamoDB (journey_control)│
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. PROCESSAMENTO                                            │
│    ├─> Lê dados (Glue Catalog ou S3)                       │
│    ├─> Transforma (ranking, joins, etc.)                   │
│    └─> JourneyController atualiza status                    │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. SALVAMENTO DE CONGREGADO                                 │
│    └─> DynamoDBHandler salva em DynamoDB (congregado_data) │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. ESCRITA DE RESULTADO                                     │
│    ├─> Se tabela_consolidada + database:                    │
│    │     └─> Glue Catalog + S3 (físico)                   │
│    └─> Se output_path (sem tabela_consolidada):            │
│          └─> S3 direto                                      │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. FINALIZAÇÃO                                              │
│    └─> JourneyController marca como 'completed'            │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚙️ CONFIGURAÇÕES

### DynamoDB Tables

**Arquivo**: `app/utils/config/settings.py`

```python
# Tabelas DynamoDB
self.journey_table_name: str = 'journey_control'
self.congregado_table_name: str = 'congregado_data'
self.aws_region: str = 'sa-east-1'
```

### Glue Data Catalog

**Arquivo**: `app/utils/config/settings.py`

```python
# Configurações Glue
self.default_database_output: str = 'default_database'
self.default_output_format: str = 'parquet'
```

### Tabelas Consolidadas

**Arquivo**: `app/utils/config/settings.py`

```python
self.consolidacoes_tabelas: Dict[str, Any] = {
    "tbl_processado_operacao_consolidada_n1": {
        "principais": {
            "sor": {"database": "db_sor", "table": "tbl_processado_operacao_sor_n1"},
            "sot": {"database": "db_sot", "table": "tbl_processado_operacao_apropriada_n1"}
        },
        # ... outras configurações
    },
    # ... outras tabelas consolidadas
}
```

---

## 📝 OBSERVAÇÕES IMPORTANTES

1. **DynamoDB - Modo em Memória**:  
   Se as credenciais AWS não estiverem disponíveis (ambiente local/testes), o DynamoDB usa armazenamento em memória (`_in_memory_store`). Os dados **não são persistidos** nesse caso.

2. **DataFrame Completo**:  
   O DataFrame completo (`df_consolidado`) **NÃO é salvo** no DynamoDB, apenas metadados e uma amostra. O DataFrame completo é salvo no Glue Data Catalog ou S3.

3. **Idempotência**:  
   Todas as operações de salvamento são **idempotentes**. Executar o mesmo processo múltiplas vezes não cria duplicatas.

4. **Partições**:  
   As tabelas no Glue Data Catalog são particionadas por `anomesdia` (data da execução). Cada execução cria uma nova partição.

5. **Prioridade de Escrita**:  
   - Se `tabela_consolidada` + `database` fornecidos: salva no **Glue Catalog** (e S3 físico)
   - Se `output_path` fornecido (sem `tabela_consolidada`): salva no **S3 direto**
   - Se ambos fornecidos: **prioriza Glue Catalog**

---

## 🔗 ARQUIVOS RELACIONADOS

- `app/utils/dynamodb_handler.py` - Handler DynamoDB
- `app/utils/journey_controller.py` - Controller de jornada
- `app/utils/handlers/glue_handler.py` - Handler Glue/S3
- `app/utils/business/flexible_consolidation_processor.py` - Processador de consolidação
- `app/utils/config/settings.py` - Configurações

---

**Última Atualização**: 2025-01-19
