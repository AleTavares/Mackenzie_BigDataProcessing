# Lab Setup - Aula 6: Qualidade de Dados e Monitoramento

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Depois do incidente com dados negativos que foram parar no relatório do board, Marina decretou: zero tolerância com dados ruins. Hoje vamos construir um framework de qualidade. O ambiente precisa ter Spark (para os checks) e Airflow (para os quality gates). Os datasets desta aula contêm erros intencionais — nulls, duplicatas, valores fora de range. É proposital."

## Pré-requisitos

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop / Engine | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| RAM disponível | 8 GB+ | Docker Desktop → Settings → Resources |
| Aulas 1-5 completas | — | Sabe Spark + Airflow |

---

## Passo 1: Subir o Ambiente Completo

```bash
cd Mackenzie_BigDataProcessing
git pull origin main
docker compose -f shared/docker-compose.full.yml up -d
```

---

## Passo 2: Verificar Serviços

| Serviço | Porta | URL |
|---------|-------|-----|
| Jupyter + Spark | 8888 | http://localhost:8888 |
| Airflow Webserver | 8081 | http://localhost:8081 |
| Spark UI | 4040 | http://localhost:4040 (quando ativo) |

---

## Passo 3: Verificar Datasets com Problemas Intencionais

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan

spark = SparkSession.builder \
    .appName("DataFlow-Aula06-Setup") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Carregar dataset com problemas
df = spark.read.parquet("/home/jovyan/work/data/aula_06/vendas_com_problemas.parquet")

print(f"✅ Dataset carregado: {df.count():,} registros")
print(f"   Colunas: {len(df.columns)}")
print()

# Preview dos problemas que vamos detectar
print("📊 Preview de qualidade (o que vamos encontrar e corrigir):")
null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
]).collect()[0]

for c in df.columns:
    nulls = null_counts[c]
    if nulls > 0:
        print(f"   ⚠️  {c}: {nulls} nulls ({nulls/df.count()*100:.1f}%)")

spark.stop()
print("\n✅ Ambiente pronto! Os dados têm problemas — é isso que vamos resolver hoje.")
```

---

## Passo 4: Criar Diretório de Quarentena

```python
import os
os.makedirs("/tmp/datalake/quarentena", exist_ok=True)
os.makedirs("/tmp/datalake/quality_reports", exist_ok=True)
print("✅ Diretórios criados:")
print("   /tmp/datalake/quarentena/       ← dados rejeitados")
print("   /tmp/datalake/quality_reports/  ← relatórios de qualidade")
```

---

## Troubleshooting

### Dataset não encontrado

```bash
# Verificar se os datasets da aula 06 existem
ls datasets/aula_06/

# Se vazio, atualize o repositório
git pull origin main
```

### OutOfMemoryError nos checks

```python
# Reduzir paralelismo
spark.conf.set("spark.sql.shuffle.partitions", "4")
```

---

## Checklist de Validação

- [ ] Jupyter acessível em http://localhost:8888
- [ ] Airflow acessível em http://localhost:8081
- [ ] Dataset `vendas_com_problemas.parquet` carrega com sucesso
- [ ] Dataset contém nulls e problemas (proposital)
- [ ] Diretórios de quarentena e relatórios criados
- [ ] SparkSession funciona

> **Carlos:** "Dados carregados — e sim, eles têm problemas. É exatamente isso que vamos detectar e tratar. Nenhum dado sujo passa daqui!"
