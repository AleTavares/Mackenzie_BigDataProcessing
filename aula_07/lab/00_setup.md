# Lab Setup - Aula 7: Pipeline End-to-End em Produção

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Esta é a aula de integração total — vamos pegar tudo que fizemos nas aulas 1 a 6 e montar um pipeline de produção completo. O ambiente precisa ter Spark, Airflow e Jupyter rodando simultaneamente. Vamos converter o notebook em script, adicionar logging, orquestrar com Airflow e validar qualidade — tudo containerizado."

## Pré-requisitos

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop / Engine | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| RAM disponível | 10 GB+ | Docker Desktop → Settings → Resources |
| Aulas 1-6 completas | — | Domina Spark + Airflow + Qualidade |

> **⚠️ Importante:** Esta aula usa a stack completa (Spark + Airflow + Jupyter). Recomendamos **10 GB de RAM** para o Docker.

---

## Passo 1: Subir a Stack Completa

```bash
cd Mackenzie_BigDataProcessing
git pull origin main
docker compose -f shared/docker-compose.full.yml up -d
```

---

## Passo 2: Verificar Todos os Serviços

```bash
docker compose -f shared/docker-compose.full.yml ps
```

| Serviço | Porta | URL | Status esperado |
|---------|-------|-----|----------------|
| Jupyter + Spark | 8888 | http://localhost:8888 | Up |
| Airflow Webserver | 8081 | http://localhost:8081 | Up |
| Airflow Scheduler | — | — | Up |
| Spark UI | 4040 | http://localhost:4040 | Up (quando job roda) |

---

## Passo 3: Verificar Scripts de Produção

```bash
# Os scripts que vamos usar/criar estão em:
ls aula_07/code/spark_jobs/
# Esperado: pipeline_vendas.py, structured_logging.py

ls aula_07/code/dags/
# Esperado: dag_pipeline_vendas.py
```

---

## Passo 4: Testar Spark via Linha de Comando

```bash
# Verificar que spark-submit funciona dentro do container
docker exec jupyter-spark spark-submit --version
```

**Resultado esperado:**
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.x
      /_/
```

---

## Passo 5: Testar Script com argparse

```bash
# Testar o script de produção com parâmetros
docker exec jupyter-spark python /home/jovyan/work/aula_07/code/spark_jobs/pipeline_vendas.py --help
```

Se o script ainda não existir (será criado durante o lab), prossiga para os exercícios.

---

## Passo 6: Preparar Diretórios de Output

```python
import os

dirs = [
    "/tmp/pipeline/logs",
    "/tmp/pipeline/output/bronze",
    "/tmp/pipeline/output/silver",
    "/tmp/pipeline/output/gold",
    "/tmp/pipeline/output/quarentena",
]

for d in dirs:
    os.makedirs(d, exist_ok=True)

print("✅ Diretórios de produção criados:")
for d in dirs:
    print(f"   {d}")
```

---

## Troubleshooting

### Memória insuficiente (containers reiniciando)

A stack completa consome mais memória. Se containers ficam em restart:

1. Docker Desktop → Settings → Resources → **12 GB RAM**
2. Ou reduzir serviços: `docker compose -f shared/docker-compose.yml up -d` (sem Airflow)

### spark-submit não encontrado

```bash
# Verificar PATH dentro do container
docker exec jupyter-spark which spark-submit
docker exec jupyter-spark echo $SPARK_HOME
```

### DAG não aparece no Airflow

```bash
# Verificar que a DAG está no diretório correto
docker exec airflow-scheduler ls /opt/airflow/dags/

# Forçar re-parse
docker exec airflow-scheduler airflow dags reserialize
```

---

## Checklist de Validação

- [ ] Stack completa rodando (Jupyter + Airflow + Spark)
- [ ] Jupyter acessível em http://localhost:8888
- [ ] Airflow acessível em http://localhost:8081
- [ ] `spark-submit --version` funciona no container
- [ ] Scripts em `aula_07/code/spark_jobs/` acessíveis
- [ ] DAG em `aula_07/code/dags/` acessível
- [ ] Diretórios de output criados

> **Carlos:** "Stack completa rodando! Hoje é o dia da integração — notebook vira script, script vira job orquestrado, tudo containerizado. É a preparação final antes do projeto."
