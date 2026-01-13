# Compatibilidade com Windows

## ✅ Resposta Rápida: SIM, funciona no Windows!

A aplicação **funciona perfeitamente no Windows**, mas há algumas diferenças nos scripts e comandos. Este guia explica tudo.

---

## 🔍 Análise de Compatibilidade

### ✅ **O que funciona 100% no Windows:**

1. **Código Python** - Todo o código Python é multiplataforma
2. **Bibliotecas** - Todas as dependências funcionam no Windows:
   - `pytest`, `pytest-cov`
   - `boto3`
   - `pyspark`
   - `pandas`, `numpy`
3. **Lógica de negócio** - Toda a lógica é independente de SO
4. **AWS Glue** - Funciona normalmente (quando executado na AWS)

### ⚠️ **O que precisa de ajuste:**

1. **Scripts de teste** - Scripts `.sh` não funcionam nativamente
2. **Caminhos hardcoded** - Alguns caminhos absolutos do Linux
3. **Comandos shell** - Comandos bash específicos

---

## 🛠️ Soluções Implementadas

### 1. Script Multiplataforma (`run_tests.py`)

Criado script Python que funciona em **Windows, Linux e macOS**:

```bash
# Windows
python run_tests.py

# Linux/Mac
python3 run_tests.py
```

### 2. Script Batch para Windows (`run_tests.bat`)

Script nativo do Windows:

```cmd
run_tests.bat
```

### 3. Script Bash Original (`run_tests.sh`)

Continua funcionando no Linux/Mac:

```bash
./run_tests.sh
```

---

## 📋 Guia de Uso no Windows

### **Opção 1: Script Python (Recomendado)**

```cmd
# No PowerShell ou CMD
cd C:\caminho\para\glue_project\app
python run_tests.py
```

### **Opção 2: Script Batch**

```cmd
cd C:\caminho\para\glue_project\app
run_tests.bat
```

### **Opção 3: Comandos Diretos**

```cmd
# Ativar virtualenv
venv\Scripts\activate

# Instalar dependências (se necessário)
pip install -r requirements-dev.txt

# Executar testes
set PYTHONPATH=%CD%;%PYTHONPATH%
pytest --cov=src --cov=utils --cov-report=term-missing --cov-fail-under=98 -v tests/
```

---

## 🔧 Diferenças Windows vs Linux

### **1. Separadores de Caminho**

| Linux/Mac            | Windows                 |
| -------------------- | ----------------------- |
| `/home/user/project` | `C:\Users\user\project` |
| `venv/bin/activate`  | `venv\Scripts\activate` |
| `./script.sh`        | `script.bat`            |

### **2. Ativação de Virtualenv**

**Linux/Mac:**

```bash
source venv/bin/activate
```

**Windows (CMD):**

```cmd
venv\Scripts\activate.bat
```

**Windows (PowerShell):**

```powershell
venv\Scripts\Activate.ps1
```

### **3. Variáveis de Ambiente**

**Linux/Mac:**

```bash
export PYTHONPATH=/caminho:$PYTHONPATH
```

**Windows (CMD):**

```cmd
set PYTHONPATH=%CD%;%PYTHONPATH%
```

**Windows (PowerShell):**

```powershell
$env:PYTHONPATH = "$PWD;$env:PYTHONPATH"
```

### **4. Execução de Scripts**

**Linux/Mac:**

```bash
./run_tests.sh
python run_tests.py
```

**Windows:**

```cmd
run_tests.bat
python run_tests.py
```

---

## ✅ Checklist para Windows

### **Pré-requisitos:**

- [ ] Python 3.7+ instalado
- [ ] JDK 17 instalado (para PySpark)
- [ ] Git Bash ou WSL (opcional, para scripts `.sh`)

### **Configuração:**

- [ ] Criar virtualenv: `python -m venv venv`
- [ ] Ativar virtualenv: `venv\Scripts\activate`
- [ ] Instalar dependências: `pip install -r requirements-dev.txt`

### **Execução:**

- [ ] Usar `run_tests.py` (recomendado) ou `run_tests.bat`
- [ ] Ou executar `pytest` diretamente com PYTHONPATH configurado

---

## 🐛 Problemas Comuns e Soluções

### **Problema 1: "pytest não encontrado"**

**Solução:**

```cmd
pip install pytest pytest-cov
```

### **Problema 2: "PYTHONPATH não funciona"**

**Solução:**

```cmd
# CMD
set PYTHONPATH=%CD%

# PowerShell
$env:PYTHONPATH = $PWD
```

### **Problema 3: "Script .sh não executa"**

**Solução:** Use `run_tests.py` ou `run_tests.bat` em vez de `.sh`

### **Problema 4: "Spark não inicia"**

**Solução:**

- Verificar se JAVA_HOME está configurado
- Instalar JDK 17
- Verificar variáveis de ambiente

### **Problema 5: "Caminhos com barras invertidas"**

**Solução:** O código Python usa `pathlib` e `os.path`, que funcionam em ambos

---

## 📝 Exemplo Completo no Windows

### **PowerShell:**

```powershell
# 1. Navegar para o diretório
cd C:\projetos\glue_project\app

# 2. Criar virtualenv (se não existir)
python -m venv ..\venv

# 3. Ativar virtualenv
..\venv\Scripts\Activate.ps1

# 4. Instalar dependências
pip install -r requirements-dev.txt

# 5. Executar testes
python run_tests.py
```

### **CMD:**

```cmd
REM 1. Navegar para o diretório
cd C:\projetos\glue_project\app

REM 2. Criar virtualenv (se não existir)
python -m venv ..\venv

REM 3. Ativar virtualenv
..\venv\Scripts\activate.bat

REM 4. Instalar dependências
pip install -r requirements-dev.txt

REM 5. Executar testes
python run_tests.py
```

---

## 🎯 Resumo

| Item              | Windows            | Linux/Mac      |
| ----------------- | ------------------ | -------------- |
| **Código Python** | ✅ Funciona        | ✅ Funciona    |
| **Bibliotecas**   | ✅ Funciona        | ✅ Funciona    |
| **Scripts .sh**   | ❌ Não nativo      | ✅ Funciona    |
| **Scripts .bat**  | ✅ Funciona        | ❌ Não nativo  |
| **Scripts .py**   | ✅ Funciona        | ✅ Funciona    |
| **Virtualenv**    | `Scripts\activate` | `bin/activate` |
| **Separadores**   | `\`                | `/`            |

---

## 🚀 Conclusão

**SIM, a aplicação funciona perfeitamente no Windows!**

- ✅ Todo o código Python é multiplataforma
- ✅ Scripts multiplataforma criados (`run_tests.py`)
- ✅ Scripts nativos do Windows criados (`run_tests.bat`)
- ✅ Documentação atualizada

**Use `run_tests.py` para máxima compatibilidade!**

---

## 📚 Referências

- **Script multiplataforma:** `run_tests.py`
- **Script Windows:** `run_tests.bat`
- **Script Linux/Mac:** `run_tests.sh`
- **Documentação geral:** `README.md`
