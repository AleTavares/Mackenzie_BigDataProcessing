# Plano de Teste: Docker Cold Start — Validação dos Labs

## Objetivo

Este documento descreve o procedimento completo para testar todos os laboratórios do curso
em um ambiente Docker **limpo** (cold start), simulando a experiência de um aluno que executa
o material pela primeira vez em uma máquina sem cache de imagens Docker.

O teste valida que:
- Todos os `docker-compose` sobem sem erros a partir do zero
- Os serviços ficam acessíveis nas portas esperadas
- Os healthchecks passam dentro do tempo razoável
- Os volumes montam corretamente os datasets e notebooks

---

## 1. Pré-requisitos

### 1.1 Requisitos de Sistema

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disco livre | 15 GB | 25 GB |
| Internet | 50 Mbps (apenas para pull inicial) | 100 Mbps |

### 1.2 Software Necessário

- Docker Engine 24.0+ (ou Docker Desktop 4.20+)
- Docker Compose v2.x (integrado ao Docker CLI)
- `curl` (para verificações de saúde)
- `bash` 4.0+
- `time` (utilitário de medição de tempo)

### 1.3 Verificação de Pré-requisitos

Execute antes de iniciar os testes:

```bash
# Verificar Docker
docker --version       # Esperado: Docker version 24.x ou superior
docker compose version # Esperado: Docker Compose version v2.x

# Verificar recursos disponíveis
docker info | grep -E "CPUs|Total Memory"
# Esperado: CPUs >= 4, Total Memory >= 8GB

# Verificar espaço em disco
df -h /var/lib/docker  # Esperado: >= 15GB disponíveis

# Verificar conectividade (necessária apenas para o pull)
curl -s https://hub.docker.com/ > /dev/null && echo "OK" || echo "FALHA"
```

### 1.4 Preparação do Ambiente Limpo

Para garantir um cold start verdadeiro, remova **tudo** do Docker:

```bash
# ⚠️ CUIDADO: Remove TODOS os containers, imagens e volumes do Docker!
# Use apenas em máquina de teste dedicada.
docker stop $(docker ps -aq) 2>/dev/null
docker rm $(docker ps -aq) 2>/dev/null
docker rmi $(docker images -aq) 2>/dev/null
docker volume prune -f
docker network prune -f
docker system prune -af --volumes
```

---

## 2. Procedimento de Teste

### 2.1 Teste — Aula 01 a 03: Spark + Jupyter (docker-compose.yml base)

**Contexto**: Ambiente utilizado nas aulas 1, 2 e 3. Contém Spark Master, Spark Worker e
Jupyter Notebook com PySpark.

**Comando de subida**:

```bash
cd shared/
docker compose up -d
```

**Serviços esperados**:

| Serviço | Container | Porta | URL de Verificação |
|---------|-----------|-------|-------------------|
| Spark Master | spark-master | 8080 | http://localhost:8080 |
| Spark Worker | spark-worker | — | (registra no master) |
| Jupyter Notebook | jupyter-notebook | 8888 | http://localhost:8888 |

**Verificações**:

```bash
# 1. Todos os containers estão running?
docker compose ps
# Esperado: 3 serviços com status "running" ou "healthy"

# 2. Spark Master UI acessível?
curl -sf http://localhost:8080 | grep -q "Spark Master" && echo "✅ Spark Master OK" || echo "❌ FALHA"

# 3. Spark Worker registrado no Master?
curl -sf http://localhost:8080 | grep -q "Workers" && echo "✅ Worker registrado" || echo "❌ FALHA"

# 4. Jupyter Notebook acessível?
curl -sf http://localhost:8888 | grep -q -i "jupyter" && echo "✅ Jupyter OK" || echo "❌ FALHA"

# 5. Volume de datasets montado?
docker exec jupyter-notebook ls /home/jovyan/work/data/ 2>/dev/null && echo "✅ Dados OK" || echo "❌ FALHA"
```

**Critérios de aprovação**:
- [  ] Todos os 3 containers em status "running"
- [  ] Spark Master UI responde em http://localhost:8080
- [  ] Worker aparece registrado na interface do Master
- [  ] Jupyter Notebook acessível sem token em http://localhost:8888
- [  ] Diretório `/home/jovyan/work/data/` contém os datasets

**Cleanup**:

```bash
docker compose down -v
```

---

### 2.2 Teste — Aula 04 a 05: Spark + Airflow (docker-compose.yml + override airflow)

**Contexto**: A partir da Aula 4, o Airflow é adicionado ao stack existente via arquivo
override.

**Comando de subida**:

```bash
cd shared/
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
```

**Serviços esperados**:

| Serviço | Container | Porta | URL de Verificação |
|---------|-----------|-------|-------------------|
| Spark Master | spark-master | 8080 | http://localhost:8080 |
| Spark Worker | spark-worker | — | (registra no master) |
| Jupyter Notebook | jupyter-notebook | 8888 | http://localhost:8888 |
| Airflow Init | airflow-init | — | (encerra após setup) |
| Airflow Webserver | airflow-webserver | 8081 | http://localhost:8081 |
| Airflow Scheduler | airflow-scheduler | — | (processo interno) |

**Verificações**:

```bash
# 1. Containers ativos (airflow-init encerra após sucesso)
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps
# Esperado: spark-master, spark-worker, jupyter-notebook, airflow-webserver, airflow-scheduler running

# 2. Spark Master UI acessível?
curl -sf http://localhost:8080 | grep -q "Spark Master" && echo "✅ Spark Master OK" || echo "❌ FALHA"

# 3. Airflow Webserver acessível?
curl -sf http://localhost:8081/health | grep -q "healthy" && echo "✅ Airflow OK" || echo "❌ FALHA"

# 4. Login do Airflow funciona? (admin/admin)
curl -sf -X POST http://localhost:8081/api/v1/security/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin"}' | grep -q "access_token" \
  && echo "✅ Login Airflow OK" || echo "⚠️ Login API não disponível (verificar manualmente)"

# 5. Jupyter acessível?
curl -sf http://localhost:8888 | grep -q -i "jupyter" && echo "✅ Jupyter OK" || echo "❌ FALHA"

# 6. DAGs da Aula 4 visíveis?
docker exec airflow-scheduler ls /opt/airflow/dags/aula_04/ 2>/dev/null \
  && echo "✅ DAGs Aula 04 montadas" || echo "❌ FALHA"
```

**Critérios de aprovação**:
- [  ] Spark Master + Worker + Jupyter running
- [  ] Airflow Init completou com sucesso (exit code 0)
- [  ] Airflow Webserver responde em http://localhost:8081
- [  ] Credenciais admin/admin funcionam no Airflow
- [  ] Scheduler está processando DAGs
- [  ] DAGs da Aula 04 estão montadas no volume

**Cleanup**:

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml down -v
```

---

### 2.3 Teste — Aula 06 a 07: Stack Completa de Produção (docker-compose.full.yml)

**Contexto**: Nas aulas 6 e 7, usa-se o stack completo autocontido que inclui Spark +
Airflow + Jupyter em um único arquivo.

**Comando de subida**:

```bash
cd shared/
docker compose -f docker-compose.full.yml up -d
```

**Serviços esperados**:

| Serviço | Container | Porta | URL de Verificação |
|---------|-----------|-------|-------------------|
| Spark Master | spark-master | 8080 | http://localhost:8080 |
| Spark Worker | spark-worker | — | (registra no master) |
| Airflow Init | airflow-init | — | (encerra após setup) |
| Airflow Webserver | airflow-webserver | 8081 | http://localhost:8081 |
| Airflow Scheduler | airflow-scheduler | — | (processo interno) |
| Jupyter Notebook | jupyter-notebook | 8888 | http://localhost:8888 |

**Verificações**:

```bash
# 1. Containers ativos
docker compose -f docker-compose.full.yml ps
# Esperado: 5 serviços running (airflow-init encerrado com sucesso)

# 2. Spark Master UI
curl -sf http://localhost:8080 | grep -q "Spark Master" && echo "✅ Spark Master OK" || echo "❌ FALHA"

# 3. Spark Worker registrado
curl -sf http://localhost:8080/json/ 2>/dev/null | grep -q "alive" \
  && echo "✅ Worker alive" || echo "⚠️ Verificar Worker manualmente"

# 4. Airflow Webserver
curl -sf http://localhost:8081/health | grep -q "healthy" && echo "✅ Airflow OK" || echo "❌ FALHA"

# 5. Jupyter Notebook
curl -sf http://localhost:8888 | grep -q -i "jupyter" && echo "✅ Jupyter OK" || echo "❌ FALHA"

# 6. Comunicação entre serviços (Jupyter → Spark)
docker exec jupyter-notebook python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').master('spark://spark-master:7077').getOrCreate()
df = spark.range(10)
assert df.count() == 10
spark.stop()
print('✅ PySpark → Spark cluster OK')
" 2>/dev/null || echo "❌ FALHA na comunicação Jupyter→Spark"

# 7. Volumes de datasets
docker exec jupyter-notebook ls /home/jovyan/work/data/ 2>/dev/null \
  && echo "✅ Datasets montados" || echo "❌ FALHA"
```

**Critérios de aprovação**:
- [  ] Todos os 5 serviços estão saudáveis (airflow-init encerrou com exit 0)
- [  ] Spark Master acessível na porta 8080
- [  ] Worker registrado e funcional
- [  ] Airflow Webserver saudável na porta 8081
- [  ] Jupyter Notebook acessível na porta 8888
- [  ] Comunicação Jupyter → Spark funcional (job PySpark executa)
- [  ] Datasets montados e acessíveis no Jupyter

**Cleanup**:

```bash
docker compose -f docker-compose.full.yml down -v
```

---

## 3. Expectativas de Tempo

### 3.1 Cold Start (primeira vez, sem cache)

| Etapa | Tempo Estimado | Observação |
|-------|---------------|------------|
| Pull `bitnami/spark:3.5` | 2–4 min | ~500MB por imagem |
| Pull `apache/airflow:2.8-python3.11` | 3–5 min | ~1.2GB |
| Pull `jupyter/pyspark-notebook:latest` | 5–8 min | ~3GB (maior imagem) |
| **Total pull de imagens** | **10–17 min** | Depende da velocidade da internet |
| Start Spark Master + Worker | 30–60s | Healthcheck com start_period de 30s |
| Airflow Init (db + user) | 20–40s | Inicialização do SQLite |
| Airflow Webserver healthy | 60–90s | start_period de 60s no healthcheck |
| Jupyter pronto | 15–30s | Mais rápido que Airflow |
| **Total subida (após pull)** | **2–4 min** | |
| **Total cold start completo** | **12–21 min** | Pull + Start |

### 3.2 Warm Start (imagens em cache)

| Etapa | Tempo Estimado |
|-------|---------------|
| Start Stack Base (Spark + Jupyter) | 60–90s |
| Start Stack Airflow (Base + Override) | 2–3 min |
| Start Stack Full (Produção) | 2–3 min |

### 3.3 Tempo Total do Teste Completo

| Cenário | Tempo |
|---------|-------|
| Cold start com limpeza total | 30–45 min |
| Warm start (imagens já baixadas) | 10–15 min |

---

## 4. Modos de Falha Comuns

### 4.1 Falha: Portas já em uso

**Sintoma**: `Error: bind: address already in use`

**Solução**:
```bash
# Identificar processo na porta
sudo lsof -i :8080  # ou 8081, 8888
# Encerrar processo conflitante
sudo kill -9 <PID>
# Ou parar Docker Compose anterior
docker compose down
```

### 4.2 Falha: Memória insuficiente

**Sintoma**: Container reiniciando (OOMKilled) ou Spark Worker não registra.

**Solução**:
```bash
# Verificar memória disponível
free -h

# Verificar se Docker tem memória suficiente (Docker Desktop)
docker info | grep "Total Memory"

# Se < 8GB, ajustar no Docker Desktop: Settings → Resources → Memory
# Ou reduzir SPARK_WORKER_MEMORY no docker-compose.yml para 1g
```

### 4.3 Falha: Airflow Init não completa

**Sintoma**: `airflow-webserver` fica em "waiting" indefinidamente.

**Solução**:
```bash
# Verificar logs do init
docker logs airflow-init

# Causas comuns:
# 1. Volume corrompido → remover e recriar
docker volume rm shared_airflow-data
# 2. Permissão de arquivo → garantir que pasta dags/ existe
mkdir -p shared/dags

# Reiniciar
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
```

### 4.4 Falha: Spark Worker não registra no Master

**Sintoma**: Worker inicia mas não aparece na UI do Master.

**Solução**:
```bash
# Verificar rede
docker network ls | grep dataflow

# Verificar conectividade entre containers
docker exec spark-worker ping -c 3 spark-master

# Se rede não existe, recriar
docker network create dataflow-network
docker compose up -d
```

### 4.5 Falha: Jupyter não conecta ao Spark

**Sintoma**: `Py4JNetworkError` ou `Connection refused` ao criar SparkSession.

**Solução**:
```bash
# Verificar se Spark Master está acessível do container Jupyter
docker exec jupyter-notebook curl -sf http://spark-master:8080 > /dev/null \
  && echo "Rede OK" || echo "Problema de rede"

# Verificar variáveis de ambiente
docker exec jupyter-notebook env | grep SPARK

# Reiniciar Jupyter após Spark estar saudável
docker compose restart jupyter
```

### 4.6 Falha: Timeout no pull de imagens

**Sintoma**: Download de imagens fica parado ou falha.

**Solução**:
```bash
# Fazer pull individual com retry
docker pull bitnami/spark:3.5
docker pull apache/airflow:2.8-python3.11
docker pull jupyter/pyspark-notebook:latest

# Se atrás de proxy corporativo
# Configurar em /etc/docker/daemon.json ou Docker Desktop Settings
```

### 4.7 Falha: Espaço em disco insuficiente

**Sintoma**: `no space left on device`

**Solução**:
```bash
# Verificar uso do Docker
docker system df

# Limpar imagens não utilizadas
docker image prune -a

# Verificar disco
df -h /var/lib/docker
```

---

## 5. Checklist Final de Aprovação

### Stack Base (Aulas 01–03)
- [  ] Spark Master UI responde ✅
- [  ] Worker registrado no cluster ✅
- [  ] Jupyter acessível sem token ✅
- [  ] Datasets montados ✅
- [  ] Cold start < 20 min ✅

### Stack Airflow (Aulas 04–05)
- [  ] Tudo da stack base ✅
- [  ] Airflow Webserver saudável (porta 8081) ✅
- [  ] Login admin/admin funciona ✅
- [  ] Scheduler rodando ✅
- [  ] DAGs da Aula 04 visíveis ✅
- [  ] Cold start < 25 min ✅

### Stack Completa (Aulas 06–07)
- [  ] Todos os 5 serviços saudáveis ✅
- [  ] Comunicação Jupyter → Spark funcional ✅
- [  ] Pipeline PySpark executa no cluster ✅
- [  ] Airflow orquestra tasks corretamente ✅
- [  ] Cold start < 25 min ✅

---

## 6. Script Automatizado

O script `shared/test_cold_start.sh` automatiza todo este procedimento.
Execute com:

```bash
cd BigDataProcessing/
chmod +x shared/test_cold_start.sh
./shared/test_cold_start.sh
```

O script gera um relatório final com resultados PASS/FAIL para cada etapa.

---

## 7. Notas para o Professor/Monitor

1. **Primeira execução do semestre**: Execute o cold start completo 48h antes da primeira
   aula para identificar problemas com antecedência.

2. **Máquinas do laboratório**: Se as máquinas tiverem rede restrita, faça o `docker pull`
   de todas as imagens previamente e distribua via `docker save`/`docker load`.

3. **Codespaces como fallback**: Se Docker local falhar, os alunos podem usar GitHub
   Codespaces (configurado em `.devcontainer/`). Tenha o link do repositório pronto.

4. **Tempo de aula**: Reserve os primeiros 15 minutos da Aula 1 para troubleshooting de
   ambiente. Nas aulas seguintes, 5 minutos são suficientes (imagens já em cache).

5. **Versões fixas**: As imagens usam tags fixas (`bitnami/spark:3.5`, `apache/airflow:2.8-python3.11`)
   para garantir reprodutibilidade entre semestres.
