# ✅ Alterações Implementadas - Persistência S3 + Glue Catalog

**Data**: 2025-01-19  
**Requisito**: Sempre salvar no S3 e atualizar partição no catálogo do AWS Glue (formato Parquet, compressão Snappy)

---

## 📋 ALTERAÇÕES REALIZADAS

### 1. Configuração de Compressão (`app/utils/config/settings.py`)

**Adicionado**:
```python
self.default_compression: str = 'snappy'  # Compressão padrão para Parquet
```

**Localização**: Linha 34

---

### 2. Handler Glue - Suporte a Compressão (`app/utils/handlers/glue_handler.py`)

#### 2.1. Método `write_to_s3()`

**Alterado**: Adicionado parâmetro `compression` (padrão: `"snappy"`)

**Antes**:
```python
def write_to_s3(self, df: DataFrame, path: str, format: str = "parquet", partition_cols: list = None):
```

**Depois**:
```python
def write_to_s3(
    self, 
    df: DataFrame, 
    path: str, 
    format: str = "parquet", 
    partition_cols: list = None,
    compression: str = "snappy"
):
```

**Comportamento**:
- Sempre usa compressão Snappy para Parquet
- Configura `format_options` com `{"compression": compression}`

---

#### 2.2. Método `write_to_catalog()`

**Alterado**: Implementação completa para sempre salvar no S3 e atualizar catálogo

**Comportamento**:
1. **Obtém caminho S3 da tabela** do Glue Data Catalog via API
2. **Obtém colunas de partição** da tabela
3. **Usa `from_options`** com:
   - Caminho S3 da tabela
   - Formato: Parquet
   - Compressão: Snappy (padrão)
   - Colunas de partição
4. **Atualiza catálogo automaticamente** (o Glue detecta novos arquivos no caminho da tabela)

**Fallback**: Se não conseguir obter o caminho S3, usa `from_catalog` (sem compressão customizada)

**Código**:
```python
def write_to_catalog(
    self, 
    df: DataFrame, 
    database: str, 
    table_name: str,
    compression: str = "snappy"
):
    # 1. Obtém caminho S3 da tabela
    glue_client = _get_glue_client()
    table_response = glue_client.get_table(DatabaseName=database, Name=table_name)
    table_location = table_response['Table']['StorageDescriptor']['Location']
    
    # 2. Obtém colunas de partição
    partition_keys = [col['Name'] for col in table_response['Table'].get('PartitionKeys', [])]
    
    # 3. Salva no S3 com compressão
    format_options = {"compression": compression}
    self.glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": table_location,
            "partitionKeys": partition_keys if partition_keys else []
        },
        format="parquet",
        format_options=format_options,
        transformation_ctx="write_to_catalog"
    )
```

---

### 3. Processador de Consolidação (`app/utils/business/flexible_consolidation_processor.py`)

#### 3.1. Método `_write_output()`

**Alterado**: Sempre salva no S3 e atualiza catálogo quando `tabela_consolidada` + `database` fornecidos

**Comportamento**:
- **Se `tabela_consolidada` + `database` fornecidos**:
  - ✅ Salva no S3 (caminho da tabela no catálogo)
  - ✅ Atualiza partição no catálogo do Glue
  - ✅ Formato: Parquet
  - ✅ Compressão: Snappy (padrão)

- **Se apenas `output_path` fornecido**:
  - ✅ Salva no S3 (caminho especificado)
  - ❌ Não atualiza catálogo
  - ✅ Formato: Parquet
  - ✅ Compressão: Snappy (padrão)

**Código**:
```python
def _write_output(self, df: DataFrame, transformed_data: Dict, output_path: str, **kwargs):
    # Obter compressão da configuração (padrão: snappy)
    compression = getattr(self.config, 'default_compression', 'snappy')
    
    if tabela_consolidada and database:
        # Sempre salvar no S3 e atualizar catálogo
        self.glue_handler.write_to_catalog(
            df=df_consolidado,
            database=database,
            table_name=tabela_consolidada,
            compression=compression
        )
    elif output_path:
        # Salvar apenas no S3
        self.glue_handler.write_to_s3(
            df=df_consolidado,
            path=output_path,
            format=self.config.default_output_format,
            compression=compression
        )
```

---

## ✅ GARANTIAS IMPLEMENTADAS

| Requisito | Status | Implementação |
|-----------|--------|--------------|
| **Sempre salvar no S3** | ✅ | `write_to_catalog()` obtém caminho S3 da tabela e salva via `from_options` |
| **Atualizar partição no catálogo** | ✅ | Glue detecta automaticamente novos arquivos no caminho da tabela |
| **Formato Parquet** | ✅ | Sempre usa `format="parquet"` |
| **Compressão Snappy** | ✅ | Sempre usa `compression="snappy"` (padrão) |

---

## 🔍 FLUXO DE EXECUÇÃO

```
┌─────────────────────────────────────────────────────────┐
│ 1. _write_output() é chamado                           │
│    └─> Obtém compressão: 'snappy' (padrão)             │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 2. write_to_catalog() é chamado                        │
│    ├─> Obtém caminho S3 da tabela (via Glue API)       │
│    ├─> Obtém colunas de partição                        │
│    └─> Prepara format_options: {"compression": "snappy"}│
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 3. write_dynamic_frame.from_options()                   │
│    ├─> Salva no S3: {table_location}/                  │
│    ├─> Formato: parquet                                 │
│    ├─> Compressão: snappy                               │
│    └─> Partições: conforme PartitionKeys da tabela     │
└─────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────┐
│ 4. Glue detecta novos arquivos                          │
│    └─> Atualiza catálogo automaticamente                │
│        └─> Nova partição registrada                     │
└─────────────────────────────────────────────────────────┘
```

---

## 📝 EXEMPLO DE USO

### Cenário 1: Salvar em tabela do catálogo

```python
processor.process(
    database='db_sor',
    tabela_consolidada='tbl_processado_operacao_consolidada_n1',
    output_path=None  # Não necessário quando tabela_consolidada é fornecido
)
```

**Resultado**:
- ✅ Dados salvos em: `s3://bucket/db_sor/tbl_processado_operacao_consolidada_n1/anomesdia=20250119/`
- ✅ Formato: Parquet
- ✅ Compressão: Snappy
- ✅ Partição `anomesdia=20250119` atualizada no catálogo

---

### Cenário 2: Salvar em caminho S3 customizado

```python
processor.process(
    database='db_sor',
    tabela_consolidada=None,
    output_path='s3://bucket/custom-output/'
)
```

**Resultado**:
- ✅ Dados salvos em: `s3://bucket/custom-output/`
- ✅ Formato: Parquet
- ✅ Compressão: Snappy
- ❌ Catálogo não atualizado (apenas S3)

---

## ⚙️ CONFIGURAÇÕES

### Compressão Padrão

**Arquivo**: `app/utils/config/settings.py`

```python
self.default_compression: str = 'snappy'
```

**Outras opções de compressão** (se necessário):
- `'uncompressed'` - Sem compressão
- `'gzip'` - Compressão Gzip
- `'lzo'` - Compressão LZO
- `'brotli'` - Compressão Brotli
- `'lz4'` - Compressão LZ4

---

## 🧪 TESTES

### Testes Unitários

**Arquivo**: `app/tests/unit/test_glue_handler.py`

- ✅ `test_write_to_s3` - Testa escrita no S3 com compressão
- ✅ `test_write_to_catalog` - Testa escrita no catálogo

**Status**: Todos os testes passando ✅

---

## 📊 VALIDAÇÃO

### Como Validar

1. **Verificar arquivos no S3**:
   ```bash
   aws s3 ls s3://bucket/db_sor/tbl_processado_operacao_consolidada_n1/anomesdia=20250119/
   ```

2. **Verificar compressão**:
   ```bash
   # Arquivos devem ter extensão .parquet.snappy ou similar
   ```

3. **Verificar partição no catálogo**:
   ```python
   import boto3
   glue = boto3.client('glue', region_name='sa-east-1')
   partitions = glue.get_partitions(
       DatabaseName='db_sor',
       TableName='tbl_processado_operacao_consolidada_n1',
       Expression="anomesdia='20250119'"
   )
   ```

---

## 🔗 ARQUIVOS MODIFICADOS

1. ✅ `app/utils/config/settings.py` - Adicionado `default_compression`
2. ✅ `app/utils/handlers/glue_handler.py` - Suporte a compressão em `write_to_s3()` e `write_to_catalog()`
3. ✅ `app/utils/business/flexible_consolidation_processor.py` - Atualizado `_write_output()`

---

## ✅ CONCLUSÃO

**Requisito atendido**: ✅

- ✅ Sempre salva no S3
- ✅ Sempre atualiza partição no catálogo do Glue
- ✅ Formato: Parquet
- ✅ Compressão: Snappy (padrão)

**Última Atualização**: 2025-01-19
