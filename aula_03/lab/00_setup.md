# Lab Setup - Aula 3: Ingestão e Persistência de Dados

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Hoje vamos integrar dados de 3 parceiros diferentes — cada um com seu formato e suas peculiaridades. Antes de começar, precisamos garantir que o ambiente está rodando e que os 3 datasets estão acessíveis. Atenção especial ao CSV do Parceiro A: encoding ISO-8859-1 pode causar problemas se o ambiente não estiver correto."

## Pré-requisitos

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop / Engine | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| Git | 2.30+ | `git --version` |
| RAM disponível | 8 GB | Docker Desktop → Settings → Resources |

---

## Passo 1: Atualizar o Repositório

```bash
cd Mackenzie_BigDataProcessing
git pull origin main
```

---

## Passo 2: Subir o Ambiente

```bash
docker compose -f shared/docker-compose.yml up -d
```

Se já estiver rodando da aula anterior, basta confirmar:

```bash
docker compose -f shared/docker-compose.yml ps
```

**Resultado esperado:** Container `jupyter-spark` com status "Up".

---

## Passo 3: Verificar os Datasets da Aula 3

Acesse o Jupyter (http://localhost:8888) e execute:

```python
import os

base = "/home/jovyan/work/data/aula_03"
parceiros = {
    "parceiro_a": ["vendas_legacy_01_2023.csv", "vendas_legacy_06_2023.csv", "vendas_legacy_12_2023.csv"],
    "parceiro_b": ["api_dump_page_001.json"],
    "parceiro_c": []  # verificar existência da pasta
}

print("📂 Verificando datasets da Aula 3:\n")
for parceiro, arquivos in parceiros.items():
    path = os.path.join(base, parceiro)
    if os.path.exists(path):
        contents = os.listdir(path)
        print(f"  ✅ {parceiro}/ ({len(contents)} arquivos)")
        for f in sorted(contents)[:5]:
            size = os.path.getsize(os.path.join(path, f)) / 1024
            print(f"     - {f} ({size:.1f} KB)")
    else:
        print(f"  ❌ {parceiro}/ — NÃO ENCONTRADO!")
```

**Resultado esperado:**
```
📂 Verificando datasets da Aula 3:

  ✅ parceiro_a/ (3 arquivos)
     - vendas_legacy_01_2023.csv
     - vendas_legacy_06_2023.csv
     - vendas_legacy_12_2023.csv
  ✅ parceiro_b/ (X arquivos)
     - api_dump_page_001.json
  ✅ parceiro_c/ (X arquivos)
```

---

## Passo 4: Testar Leitura Multi-Formato

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataFlow-Aula03-Setup") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Teste CSV com encoding especial
df_csv = spark.read.csv(
    "/home/jovyan/work/data/aula_03/parceiro_a/vendas_legacy_01_2023.csv",
    header=True, sep=";", encoding="ISO-8859-1"
)
print(f"✅ CSV (Parceiro A): {df_csv.count()} registros")

# Teste JSON
df_json = spark.read.json(
    "/home/jovyan/work/data/aula_03/parceiro_b/",
    multiLine=True
)
print(f"✅ JSON (Parceiro B): {df_json.count()} registros")

print("\n✅ Ambiente pronto para o lab!")
spark.stop()
```

---

## Passo 5: Criar Diretório de Output (Data Lake local)

```python
import os
for camada in ["bronze", "silver", "gold"]:
    os.makedirs(f"/tmp/datalake/{camada}", exist_ok=True)
print("✅ Diretórios do data lake criados: /tmp/datalake/{bronze,silver,gold}")
```

---

## Troubleshooting

### CSV com caracteres estranhos (encoding)

**Sintoma:** Nomes com acentos aparecem como `Ã§`, `Ã£o`.

**Solução:** Usar `encoding="ISO-8859-1"` na leitura:
```python
df = spark.read.csv("arquivo.csv", header=True, sep=";", encoding="ISO-8859-1")
```

### JSON com AnalysisException

**Sintoma:** `AnalysisException: cannot resolve column 'pedidos[0].valor'`

**Solução:** JSON aninhado precisa de `explode()` + seleção de campos:
```python
from pyspark.sql.functions import explode, col
df_flat = df.select(explode(col("array_field")).alias("item"))
```

---

## Checklist de Validação

- [ ] Container `jupyter-spark` está "Up"
- [ ] Jupyter acessível em http://localhost:8888
- [ ] Parceiro A: CSVs acessíveis com encoding ISO-8859-1
- [ ] Parceiro B: JSONs acessíveis
- [ ] Parceiro C: Parquets acessíveis (se disponível)
- [ ] SparkSession cria e lê os 3 formatos
- [ ] Diretórios /tmp/datalake/{bronze,silver,gold} criados

> **Carlos:** "Tudo certo! Temos CSV legado, JSON de API e Parquet pronto. Vamos construir o pipeline Medallion!"
