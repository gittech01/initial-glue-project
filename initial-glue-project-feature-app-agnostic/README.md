# Projeto glue_project — instruções para executar testes localmente

Este repositório contém um handler simples que usa APIs do AWS Glue e alguns testes unitários e de integração que rodam localmente com um virtualenv.

Passos para reproduzir o ambiente (Linux / bash):

1) Ative o virtualenv já existente no projeto:

```bash
cd /home/edsojor/workspace/glue_project
source venv/bin/activate
```

2) (Opcional) Instale dependências de desenvolvimento no venv:

```bash
pip install -r requirements-dev.txt
```

3) Garanta que o JDK 17 esteja instalado no sistema (necessário para a versão do Spark usada nos testes):

```bash
# Debian/Ubuntu
sudo apt-get update && sudo apt-get install -y openjdk-17-jdk

# Verifique
java -version
```

4) Rode os testes (os testes usam imports relativos; defino PYTHONPATH para lidar com ambos os casos):

```bash
PYTHONPATH=/home/edsojor/workspace:/home/edsojor/workspace/glue_project ./venv/bin/pytest -q
```

ou, com venv ativado:

```bash
PYTHONPATH=/home/edsojor/workspace:/home/edsojor/workspace/glue_project pytest -q
```

Notas:
- `requirements-dev.txt` traz pacotes úteis para rodar os testes localmente. Se o `venv` já contém as dependências, a instalação é opcional.
- Se preferir usar outra estrutura de imports (por exemplo sempre `src.`), os testes já foram padronizados para `src.glue_handler`.
# AWS Glue Data Engineering Handler

Este projeto fornece uma abstração robusta para operações de leitura e escrita no AWS Glue usando PySpark.

## Estrutura do Projeto

- `src/glue_handler.py`: Classe principal `GlueDataHandler`.
- `tests/unit/`: Testes unitários com mocks.
- `tests/integration/`: Testes de integração (simulados).

## Como usar

```python
from src.glue_handler import GlueDataHandler
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Inicialização padrão do Glue
sc = SparkContext()
glueContext = GlueContext(sc)
handler = GlueDataHandler(glueContext)

# Leitura do Catálogo
df = handler.read_from_catalog(database="vendas", table_name="faturamento")

# Escrita no S3 com Particionamento
handler.write_to_s3(df, path="s3://meu-bucket/curated/vendas/", partition_cols=["ano", "mes"])
```

## Executando Testes

Para rodar os testes unitários:
```bash
python3 -m unittest discover tests/unit
```

Para rodar os testes de integração (requer pytest):
```bash
pytest tests/integration
```

## Considerações de Engenharia
- **Abstração de DynamicFrames**: A classe converte automaticamente para Spark DataFrames para facilitar transformações complexas.
- **Testabilidade**: O design permite injetar mocks do GlueContext, facilitando o teste sem necessidade de um ambiente AWS real.
- **Performance**: Utiliza `transformation_ctx` para suportar bookmarks do Glue.
