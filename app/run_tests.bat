@echo off
REM Script para executar testes com cobertura (Windows)

echo === Executando testes com cobertura ===
echo.

REM Ativar virtualenv se existir
if exist "..\venv\Scripts\activate.bat" (
    call ..\venv\Scripts\activate.bat
)

REM Instalar dependências se necessário
python -c "import pytest" 2>nul
if errorlevel 1 (
    echo Instalando dependências...
    pip install -r requirements-dev.txt
)

REM Executar testes com cobertura
echo Executando testes...
set PYTHONPATH=%CD%;%PYTHONPATH%
pytest --cov=src --cov=utils --cov-report=term-missing --cov-report=html --cov-report=xml --cov-fail-under=98 -v tests/

echo.
echo === Testes concluídos ===
echo Relatório HTML disponível em: htmlcov\index.html
