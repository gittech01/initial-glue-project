# Quando Ocorre Empate no Ranking?

## 📊 DEFINIÇÃO DE EMPATE

**Empate ocorre quando dois ou mais registros têm:**
1. ✅ **Mesmas chaves principais** (mesmo grupo no PARTITION BY)
2. ✅ **Mesmos valores em TODOS os campos de decisão** (mesma ordem no ORDER BY)

---

## 🔍 EXEMPLO PRÁTICO

### Cenário: Empate Total

Imagine que temos dois registros com as mesmas chaves principais:

| num_oper | cod_idef_ver_oper | dat_vlr_even_oper | num_prio_even_oper | dat_recm_even_oper | origem |
|----------|-------------------|-------------------|-------------------|-------------------|--------|
| 12345    | v1                | 2024-01-15        | 5                 | 2024-01-15 10:00  | online |
| 12345    | v1                | 2024-01-15        | 5                 | 2024-01-15 10:00  | batch  |

**Análise:**
- ✅ Mesmas chaves principais: `num_oper=12345`, `cod_idef_ver_oper=v1`
- ✅ Mesmos valores em TODOS os campos de decisão:
  - `dat_vlr_even_oper = 2024-01-15` (igual)
  - `num_prio_even_oper = 5` (igual)
  - `dat_recm_even_oper = 2024-01-15 10:00` (igual)
- ⚠️ **ÚNICA diferença**: `origem` ('online' vs 'batch')

**Resultado: EMPATE TOTAL** 🎯

---

## ⚠️ O QUE ACONTECE NO RANKING ATUAL

### SQL Original (linhas 101-109):
```sql
row_number() over (
    partition by num_oper, cod_idef_ver_oper
    order by
        dat_vlr_even_oper desc
        , num_prio_even_oper desc
        , dat_recm_even_oper desc
        -- ❌ NÃO TEM origem no ORDER BY
) as rank
```

### Comportamento do ROW_NUMBER() em caso de empate:

1. **PARTITION BY**: Agrupa por `num_oper, cod_idef_ver_oper`
   - Ambos os registros estão no mesmo grupo (12345, v1)

2. **ORDER BY**: Ordena por campos de decisão
   - Ambos têm os mesmos valores → **EMPATE**
   - Como não há critério adicional, o ROW_NUMBER() atribui rank de forma **não determinística**

3. **Resultado Possível:**
   - Registro 'online' pode receber rank=1
   - Registro 'batch' pode receber rank=2
   - **OU vice-versa** (não determinístico!)

---

## 🎯 CENÁRIOS DE EMPATE

### Cenário 1: Empate Total (Todos os campos iguais)
```
Registro A: (12345, v1, 2024-01-15, 5, 2024-01-15 10:00, 'online')
Registro B: (12345, v1, 2024-01-15, 5, 2024-01-15 10:00, 'batch')
```
**Resultado:** EMPATE → ROW_NUMBER() escolhe aleatoriamente

### Cenário 2: Empate Parcial (Alguns campos iguais)
```
Registro A: (12345, v1, 2024-01-15, 5, 2024-01-15 10:00, 'online')
Registro B: (12345, v1, 2024-01-15, 5, 2024-01-15 11:00, 'batch')
```
**Resultado:** NÃO é empate → `dat_recm_even_oper` diferente → Registro B vence (11:00 > 10:00)

### Cenário 3: Sem Empate
```
Registro A: (12345, v1, 2024-01-15, 5, 2024-01-15 10:00, 'online')
Registro B: (12345, v1, 2024-01-16, 5, 2024-01-15 10:00, 'batch')
```
**Resultado:** NÃO é empate → `dat_vlr_even_oper` diferente → Registro B vence (2024-01-16 > 2024-01-15)

---

## 🔴 PROBLEMA ATUAL

### O que acontece em caso de empate:

1. **Ranking (ROW_NUMBER):**
   - Ambos os registros podem receber rank=1 (comportamento não determinístico)
   - **OU** um recebe rank=1 e outro rank=2 (também não determinístico)

2. **Join Final:**
   - Filtra por `origem = 'online'` → seleciona registros 'online' com rank=1
   - Filtra por `origem = 'batch'` → seleciona registros 'batch' com rank=1
   - **Resultado:** Se ambos tiverem rank=1, AMBOS aparecem no resultado final

3. **Problema:**
   - A regra diz "preferência por 'online'", mas isso não é garantido
   - Em caso de empate, ambos podem aparecer, não apenas 'online'

---

## ✅ SOLUÇÃO: Adicionar Origem no ORDER BY

Para garantir que em caso de empate, apenas 'online' seja escolhido:

```python
# Adicionar preferência por origem 'online' como último critério de desempate
origens_unicas = df_unificado.select('origem').distinct().count()
if origens_unicas > 1:
    # Preferir 'online' sobre 'batch' em caso de empate
    ordem_origem = F.when(F.col('origem') == F.lit('online'), 1).otherwise(0)
    order_cols.append(ordem_origem.desc())
```

**Resultado:**
- Em caso de empate, 'online' sempre recebe rank=1
- 'batch' recebe rank=2
- Apenas 'online' aparece no resultado final

---

## 📋 RESUMO

### Quando temos empate?
✅ **Quando dois ou mais registros têm:**
- Mesmas chaves principais (mesmo grupo)
- Mesmos valores em TODOS os campos de decisão
- Diferentes origens ('online' vs 'batch')

### O que acontece atualmente?
⚠️ **Comportamento não determinístico:**
- ROW_NUMBER() pode escolher qualquer um
- Ambos podem aparecer no resultado final

### O que deveria acontecer?
✅ **Preferência por 'online':**
- Em caso de empate, apenas 'online' deve ser escolhido
- Isso requer adicionar origem no ORDER BY como último critério
