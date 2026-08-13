# Validação do Ambiente Docker

> Documento de validação e troubleshooting para o ambiente Docker do curso Big Data Processing.

## Como Validar o Ambiente

### Validação Automática (Script Python)

Execute o script de validação incluído no repositório:

```bash
cd shared/
python validate_docker.py
```

O script verifica automaticamente:
1. ✅ Sintaxe YAML dos 3 arquivos compose
2. ✅ Imagens Docker referenciadas (bitnami/spark:3.5, apache/airflow:2.8-python3.11, jupyter/pyspark-notebook:latest)
3. ✅ Ausência de conflitos de porta entre serviços
4. ✅ Consistência da configuração de rede
5. ✅ Validação via `docker compose config` (se Docker estiver disponível)

### Validação Manual (Docker Compose)

```bash
# 1. Validar configuração sem iniciar containers
docker compose -f shared/docker-compose.yml config

# 2. Validar configuração com Airflow (override)
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml config

# 3. Validar stack completa
docker compose -f shared/docker-compose.full.yml config
```

---

## Comportamento Esperado ao Executar `docker compose up`

### Ambiente Base (Aulas 1-3): Spark + Jupyter

```bash
docker compose -f shared/docker-compose.yml up -d
```

**Sequência esperada:**
1. Pull das imagens (apenas no primeiro uso):
   - `bitnami/spark:3.5` (~500MB)
   - `jupyter/pyspark-notebook:latest` (~3GB)
2. Criação da rede `dataflow-network`
3. Inicialização do Spark Master (healthcheck em ~30s)
4. Inicialização do Spark Worker (aguarda Master ficar healthy)
5. Inicialização do Jupyter Notebook

**Tempo estimado:** 1-3 minutos (após pull das imagens)

### Ambiente com Airflow (Aulas 4-5): Spark + Airflow + Jupyter

```bash
docker compose -f shared/docker-compose.yml -f shared/docker-compose.airflow.yml up -d
```

**Sequência esperada:**
1. Pull da imagem `apache/airflow:2.8-python3.11` (~1GB)
2. Spark Master + Worker inicializam (como acima)
3. `airflow-init` executa: inicializa banco SQLite + cria usuário admin
4. `airflow-webserver` inicia após init completar
5. `airflow-scheduler` inicia após webserver ficar healthy
6. Jupyter Notebook inicia

**Tempo estimado:** 2-5 minutos (inclui inicialização do Airflow)

### Stack Completa (Aulas 6-7): Todos os Serviços

```bash
docker compose -f shared/docker-compose.full.yml up -d
```

**Sequência esperada:** Mesmo comportamento do ambiente com Airflow, mas em arquivo autocontido.

---

## Portas Acessíveis (Requisito 4.6)

| Serviço | URL | Porta Host | Porta Container |
|---------|-----|:----------:|:---------------:|
| Spark Master UI | http://localhost:8080 | 8080 | 8080 |
| Spark Master (cluster) | spark://localhost:7077 | 7077 | 7077 |
| Airflow Webserver | http://localhost:8081 | 8081 | 8080 |
| Jupyter Notebook | http://localhost:8888 | 8888 | 8888 |

### Credenciais

- **Airflow:** admin / admin
- **Jupyter:** sem token (acesso direto)

---

## Verificação de Serviços Rodando

```bash
# Verificar status de todos os containers
docker compose -f shared/docker-compose.full.yml ps

# Verificar logs em tempo real
docker compose -f shared/docker-compose.full.yml logs -f

# Verificar apenas um serviço
docker compose -f shared/docker-compose.full.yml logs spark-master
```

**Saída esperada do `ps`:**
```
NAME               STATUS                    PORTS
spark-master       running (healthy)         0.0.0.0:7077->7077/tcp, 0.0.0.0:8080->8080/tcp
spark-worker       running                   
airflow-init       exited (0)                
airflow-webserver  running (healthy)         0.0.0.0:8081->8080/tcp
airflow-scheduler  running                   
jupyter-notebook   running                   0.0.0.0:8888->8888/tcp
```

---

## Troubleshooting: Problemas Comuns

### 1. Portas já em uso

**Erro:**
```
Error response from daemon: driver failed programming external connectivity: 
Bind for 0.0.0.0:8080 failed: port is already allocated
```

**Solução:**
```bash
# Identificar quem está usando a porta
# Linux/Mac:
lsof -i :8080
# Windows:
netstat -ano | findstr :8080

# Parar o serviço conflitante ou alterar a porta no compose
```

### 2. Memória insuficiente (OOM)

**Erro:**
```
spark-worker exited with code 137
```

**Solução:**
- Verificar que há pelo menos 8GB RAM disponíveis
- Aumentar memória do Docker Desktop (Settings → Resources → Memory)
- Reduzir `SPARK_WORKER_MEMORY` de `2g` para `1g` no compose

```bash
# Verificar memória disponível para Docker
docker info | grep "Total Memory"
```

### 3. Spark Worker não conecta ao Master

**Erro:**
```
WARN Master: Got heartbeat from unregistered worker
```

**Solução:**
```bash
# Reiniciar o worker após o master estar healthy
docker compose restart spark-worker

# Verificar que o healthcheck do master está passando
docker inspect spark-master | grep -A 5 "Health"
```

### 4. Airflow Webserver não inicia

**Erro:**
```
airflow-webserver: condition: service_healthy -> failed
```

**Solução:**
```bash
# Verificar se o init completou com sucesso
docker compose logs airflow-init

# Recriar o volume do Airflow (reset completo)
docker compose down -v
docker compose up -d
```

### 5. Jupyter não encontra os datasets

**Erro:**
```python
FileNotFoundError: [Errno 2] No such file or directory: 'data/vendas_2023.csv'
```

**Solução:**
- Verificar que a pasta `datasets/` existe na raiz do projeto
- No Jupyter, os dados estão montados em `/home/jovyan/work/data/`
- Usar caminho relativo: `data/vendas_2023.csv` (a partir do diretório de trabalho)

```bash
# Verificar montagem dos volumes
docker exec jupyter-notebook ls /home/jovyan/work/data/
```

### 6. DAGs não aparecem no Airflow

**Erro:** A interface do Airflow mostra "No DAGs found" ou "DAG Import Error"

**Solução:**
```bash
# Verificar se as DAGs estão na pasta correta
ls shared/dags/

# Verificar erros de importação dentro do container
docker exec airflow-scheduler airflow dags list-import-errors

# Forçar reparse das DAGs
docker exec airflow-scheduler airflow dags reserialize
```

### 7. Rede não encontrada (ao usar override)

**Erro:**
```
network dataflow-network declared as external, but could not be found
```

**Solução:**
```bash
# Criar a rede manualmente antes de usar o override
docker network create dataflow-network

# OU iniciar primeiro o compose base (que cria a rede)
docker compose -f shared/docker-compose.yml up -d
docker compose -f shared/docker-compose.airflow.yml up -d
```

### 8. Container reiniciando em loop

**Erro:**
```
STATUS: restarting (x seconds ago)
```

**Solução:**
```bash
# Ver os logs do container com problemas
docker compose logs --tail=50 <nome-do-servico>

# Parar e remover containers + volumes (fresh start)
docker compose -f shared/docker-compose.full.yml down -v
docker compose -f shared/docker-compose.full.yml up -d
```

---

## Comandos Úteis

```bash
# Iniciar ambiente
./shared/start_env.sh

# Parar ambiente
./shared/stop_env.sh

# Resetar dados para estado inicial
./shared/reset_data.sh

# Rebuild sem cache (após alterações nos Dockerfiles)
docker compose -f shared/docker-compose.full.yml build --no-cache

# Remover tudo (containers, volumes, redes)
docker compose -f shared/docker-compose.full.yml down -v --remove-orphans
```

---

## Requisitos de Sistema

| Recurso | Mínimo | Recomendado |
|---------|--------|-------------|
| RAM | 8 GB | 16 GB |
| CPU | 4 cores | 8 cores |
| Disco | 10 GB livre | 20 GB livre |
| Docker | v24+ | Última versão |
| Docker Compose | v2.x | v2.20+ |

---

## Resultado da Última Validação

- **Data:** Validação realizada via script `validate_docker.py`
- **Sintaxe YAML:** ✅ Válida (3 arquivos)
- **Imagens Docker:** ✅ Corretas (3 imagens referenciadas)
- **Conflitos de porta:** ✅ Nenhum conflito detectado
- **Consistência de rede:** ✅ Todos usam `dataflow-network`
- **Docker Compose config:** ✅ Configuração validada com sucesso
