#!/bin/bash
# Script para executar testes com cobertura

set -e

echo "=== Executando testes com cobertura ==="
echo ""

# Ativar virtualenv se existir
if [ -d "../venv" ]; then
    source ../venv/bin/activate
fi

# Instalar dependências se necessário
if ! python -c "import pytest" 2>/dev/null; then
    echo "Instalando dependências..."
    pip install -r requirements-dev.txt
fi

# Executar testes com cobertura
echo "Executando testes..."
# Usar caminho relativo para compatibilidade multiplataforma
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH}" \
pytest \
    --cov=src \
    --cov=utils \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    --cov-fail-under=98 \
    -v \
    tests/

echo ""
echo "=== Testes concluídos ==="
echo "Relatório HTML disponível em: htmlcov/index.html"
