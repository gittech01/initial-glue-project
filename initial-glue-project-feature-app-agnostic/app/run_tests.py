#!/usr/bin/env python3
"""
Script multiplataforma para executar testes com cobertura.

Funciona em Windows, Linux e macOS.
"""
import os
import sys
import subprocess
from pathlib import Path


def main():
    """Executa testes com cobertura."""
    print("=== Executando testes com cobertura ===")
    print()
    
    # Obter diretório do script
    script_dir = Path(__file__).parent.absolute()
    project_root = script_dir.parent
    
    # Ativar virtualenv se existir
    if sys.platform == "win32":
        venv_activate = project_root / "venv" / "Scripts" / "activate.bat"
        venv_python = project_root / "venv" / "Scripts" / "python.exe"
    else:
        venv_activate = project_root / "venv" / "bin" / "activate"
        venv_python = project_root / "venv" / "bin" / "python"
    
    # Verificar se pytest está instalado
    try:
        import pytest
    except ImportError:
        print("Instalando dependências...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements-dev.txt"], 
                      cwd=script_dir, check=True)
    
    # Configurar PYTHONPATH
    pythonpath = str(script_dir)
    if "PYTHONPATH" in os.environ:
        pythonpath = f"{pythonpath}{os.pathsep}{os.environ['PYTHONPATH']}"
    os.environ["PYTHONPATH"] = pythonpath
    
    # Executar pytest
    print("Executando testes...")
    cmd = [
        sys.executable, "-m", "pytest",
        "--cov=src",
        "--cov=utils",
        "--cov-report=term-missing",
        "--cov-report=html",
        "--cov-report=xml",
        "--cov-fail-under=98",
        "-v",
        "tests/"
    ]
    
    result = subprocess.run(cmd, cwd=script_dir)
    
    print()
    print("=== Testes concluídos ===")
    if sys.platform == "win32":
        print("Relatório HTML disponível em: htmlcov\\index.html")
    else:
        print("Relatório HTML disponível em: htmlcov/index.html")
    
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
