# Lab Setup - Aula 5: Orquestração Avançada com Airflow

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Hoje vamos tornar o pipeline inteligente — branching condicional, sensors que detectam quando dados chegam e integração Spark+Airflow. O ambiente é o mesmo da Aula 4 (Airflow + Spark), mas vamos confirmar que tudo está rodando e preparar os arquivos de teste para os sensors."

## Pré-requisitos

| Requisito | Versão Mínima | Como Verificar |
|-----------|---------------|----------------|
| Docker Desktop / Engine | 24.0+ | `docker --version` |
| Docker Compose v2 | 2.20+ | `docker compose version` |
| RAM disponível | 8 GB+ | Docker Desktop → Settings → Resources |
| Aula 4 completa | — | Sabe criar DAGs, PythonOperator, XComs |

---

## Passo 1: Subir o Ambiente Completo (Airflow + Spark)

```bash
cd Mackenzie_BigDataProcessing
git pull origin main
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d
```

---

## Passo 2: Verificar Serviços

```bash
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml ps
```

**Serviços esperados:**

| Serviço | Porta | Status |
|---------|-------|--------|
| Jupyter + Spark | 8888 | Up |
| Airflow Webserver | 8081 | Up |
| Airflow Scheduler | — | Up |

**Acessar:**
- Jupyter: http://localhost:8888
- Airflow UI: http://localhost:8081 (login: `airflow` / `airflow`)

---

## Passo 3: Verificar DAGs Disponíveis

Na Airflow UI (http://localhost:8081), você deve ver as DAGs da Aula 5:

- `dag_branching_processamento` — DAG com BranchPythonOperator
- `dag_sensor_arquivo` — DAG com FileSensor
- `dag_taskgroups_multi_fonte` — DAG com TaskGroups

Se as DAGs não aparecerem, verifique:
```bash
docker exec airflow-scheduler ls /opt/airflow/dags/
```

---

## Passo 4: Preparar Arquivo de Teste para FileSensor

O exercício de FileSensor precisa de um arquivo que "chega" durante a execução:

```bash
# Criar diretório de incoming (onde o sensor vai monitorar)
docker exec airflow-scheduler mkdir -p /opt/airflow/data/incoming/

# Verificar que está vazio (sensor vai esperar)
docker exec airflow-scheduler ls /opt/airflow/data/incoming/
```

> **Nota:** Durante o exercício, você vai criar um arquivo nesse diretório para "disparar" o sensor.

---

## Passo 5: Verificar Spark via Airflow

```bash
# Confirmar que Spark está acessível do container Airflow
docker exec airflow-scheduler which spark-submit || echo "SparkSubmit não encontrado"
```

Se `spark-submit` não estiver disponível, o SparkSubmitOperator usará conexão remota — siga as instruções no exercício 04.

---

## Troubleshooting

### DAGs não aparecem na UI

```bash
# Verificar logs do scheduler
docker compose logs airflow-scheduler | tail -20

# Forçar re-scan
docker exec airflow-scheduler airflow dags list
```

### FileSensor timeout

O FileSensor tem timeout padrão de 300s. Se o arquivo não for criado a tempo:
```python
# Aumentar timeout no código da DAG
FileSensor(
    task_id="aguardar_arquivo",
    filepath="/opt/airflow/data/incoming/vendas.csv",
    poke_interval=10,   # Verificar a cada 10s
    timeout=600,        # Timeout de 10 minutos
    mode="poke"
)
```

---

## Checklist de Validação

- [ ] Airflow Webserver acessível em http://localhost:8081
- [ ] Login `airflow/airflow` funciona
- [ ] DAGs da Aula 5 visíveis na UI
- [ ] Jupyter acessível em http://localhost:8888
- [ ] Diretório `/opt/airflow/data/incoming/` criado
- [ ] Spark acessível (local ou via container separado)

> **Carlos:** "Ambiente pronto! Hoje vamos fazer o pipeline tomar decisões sozinho — branching, sensors e integração Spark. Bora!"
