# Troubleshooting — Aula 07: Pipeline End-to-End em Produção

## Contexto

> **Marina Silva (CTO):** "Colocar um pipeline em produção é onde a maioria dos problemas reais aparece. O código funciona no notebook, mas no Docker dá OOM. A idempotência está configurada, mas dados duplicam. Os logs existem, mas somem no container. Compilei aqui os problemas mais comuns que enfrentamos ao integrar Spark + Airflow + Docker em produção na DataFlow Analytics."

---

## 1. Idempotência Falha — Pipeline Reprocessa mas Gera Dados Duplicados

**Sintoma:**
```
# Você executa o pipeline 2x para a mesma data_ref e ao verificar:
spark.read.parquet("silver/vendas").filter("data_ref = '2024-01-15'").count()
# 1ª execução: 950 registros
# 2ª execução: 1900 registros  ← DUPLICOU!
```

**Causa:** Existem três causas principais:

1. **`partitionOverwriteMode` não configurado como `dynamic`** — O Spark usa `static` por padrão
2. **Usando `mode("append")` ao invés de `mode("overwrite")`** — Append sempre adiciona
3. **Coluna de partição com valor diferente a cada execução** — ex: `current_timestamp()`

**Solução:**
```python
# 1. Verificar se partitionOverwriteMode está configurado:
print(spark.conf.get("spark.sql.sources.partitionOverwriteMode"))
# Se retornar "static" → esse é o problema!

# ✅ CORRETO — Configurar na SparkSession:
spark = SparkSession.builder \
    .appName("pipeline-vendas") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# 2. Verificar se está usando overwrite (não append):
# ❌ ERRADO:
df.write.mode("append").partitionBy("data_ref").parquet(caminho)

# ✅ CORRETO:
df.write.mode("overwrite").partitionBy("data_ref").parquet(caminho)

# 3. Verificar se a coluna de partição é determinística:
# ❌ ERRADO — muda a cada execução:
df = df.withColumn("data_ref", current_timestamp().cast("date"))

# ✅ CORRETO — valor fixo passado por argumento:
df = df.withColumn("data_ref", lit(args.data_ref))
```

**Checklist das 3 peças obrigatórias:**

| # | Peça | Onde configurar | Verificação |
|---|------|-----------------|-------------|
| 1 | `partitionOverwriteMode=dynamic` | `SparkSession.builder.config(...)` | `spark.conf.get(...)` |
| 2 | `.mode("overwrite")` | Em cada `.write` do pipeline | Inspecionar código |
| 3 | `.partitionBy("data_ref")` | Em cada `.write` do pipeline | Inspecionar código |

**Prevenção:**
- Sempre valide idempotência rodando o pipeline 2x e comparando contagens
- Nunca use `current_timestamp()` ou `uuid()` em colunas de partição
- Adicione um teste automatizado que executa 2x e verifica `count_1 == count_2`

---
## 2. partitionOverwriteMode Dynamic Não Funciona — Apaga Todas as Partições

**Sintoma:**
```
# Você tem dados de 3 dias (13, 14, 15), reprocessa só o dia 15:
python pipeline_vendas.py --data-ref 2024-01-15

# Mas ao verificar, dias 13 e 14 sumiram!
spark.read.parquet("silver/vendas").groupBy("data_ref").count().show()
# +----------+-----+
# |  data_ref|count|
# +----------+-----+
# |2024-01-15|  950|    ← só sobrou este!
# +----------+-----+
```

**Causa:**
1. **`partitionOverwriteMode` configurado como `static` (padrão)** — O modo static faz overwrite no diretório raiz
2. **Configuração definida no lugar errado** — Definiu após criar a SparkSession, ou em runtime sem efeito
3. **SparkSession reutilizada sem a config** — Em ambiente interativo (Jupyter), a session pode já existir sem a config

**Solução:**
```python
# 1. Verificar o valor ATUAL da configuração:
print(spark.conf.get("spark.sql.sources.partitionOverwriteMode"))
# Se "static" → precisa recriar a session

# 2. A config DEVE ser definida ANTES ou DURANTE a criação da session:
# ✅ CORRETO — na criação:
spark = SparkSession.builder \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# ❌ ERRADO — definir depois pode não ter efeito em todas as versões:
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Em Spark < 3.0, isso pode não funcionar!

# 3. Em Jupyter/PySpark shell, parar e recriar a session:
spark.stop()
spark = SparkSession.builder \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# 4. Verificar novamente:
assert spark.conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"
```

```bash
# 5. No spark-submit, passar como --conf:
spark-submit \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    /opt/spark/jobs/pipeline_vendas.py --data-ref 2024-01-15
```

**Prevenção:**
- Sempre configure `partitionOverwriteMode` no builder da SparkSession, não depois
- Em ambientes interativos, faça `spark.stop()` antes de recriar com novas configs
- Valide com `spark.conf.get(...)` antes de executar escritas críticas

---
## 3. Logs Estruturados Não Aparecem no Docker

**Sintoma:**
```bash
# Você executa o pipeline no container e espera ver JSON nos logs:
docker logs spark-worker 2>&1 | grep "pipeline_vendas"
# ... nenhuma saída! Ou só logs do Spark framework (INFO SparkContext, etc.)

# Ou os logs aparecem em texto livre ao invés de JSON:
# [2024-01-15 06:03:22] INFO | pipeline_vendas | Etapa bronze concluída
# ... ao invés de {"timestamp":"...", "level":"INFO", ...}
```

**Causa:**
1. **Logger escrevendo em `stderr` ao invés de `stdout`** — Docker captura ambos, mas `docker logs` pode filtrar
2. **Log level configurado acima de INFO** — Se `WARNING` ou `ERROR`, logs INFO não aparecem
3. **Spark substitui o logger** — O Spark configura seu próprio logging (log4j) que pode sobrescrever o Python logger
4. **Buffer do stdout não flushado** — Em containers, Python pode bufferizar stdout

**Solução:**
```python
# 1. Garantir que o handler usa sys.stdout (não stderr):
import sys

handler = logging.StreamHandler(sys.stdout)  # ✅ stdout
# handler = logging.StreamHandler()  # ❌ stderr por padrão!

# 2. Desabilitar buffering do Python no container:
# No Dockerfile ou docker-compose:
# environment:
#   - PYTHONUNBUFFERED=1
```

```yaml
# No docker-compose.yml:
spark-worker:
  environment:
    - PYTHONUNBUFFERED=1    # Força flush imediato do stdout
    - PYTHONDONTWRITEBYTECODE=1
```

```python
# 3. Verificar log level:
logger = logging.getLogger("pipeline_vendas")
print(f"Log level atual: {logger.level}")  # 20=INFO, 30=WARNING
# Se > 20, logs INFO não aparecem

# Forçar nível INFO:
logger.setLevel(logging.INFO)

# 4. Silenciar logs excessivos do Spark/Java:
spark = SparkSession.builder \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")  # Reduz barulho do framework
```

```bash
# 5. Ver logs separando stdout de stderr:
docker logs spark-worker 1>/tmp/stdout.log 2>/tmp/stderr.log
cat /tmp/stdout.log | jq .  # Seus logs JSON devem estar aqui

# 6. Se o log está indo para arquivo dentro do container:
docker exec spark-worker cat /opt/spark/logs/pipeline.log

# 7. Seguir logs em tempo real durante execução:
docker logs -f spark-worker 2>&1 | grep --line-buffered "pipeline"
```

**Prevenção:**
- Sempre use `sys.stdout` no handler e defina `PYTHONUNBUFFERED=1` no container
- Configure `spark.sparkContext.setLogLevel("WARN")` para reduzir ruído do framework
- Teste localmente com `python pipeline_vendas.py | jq .` antes de containerizar

---
## 4. Logs Truncados — Saída Incompleta nos Containers

**Sintoma:**
```bash
# Os logs aparecem cortados no meio de uma linha JSON:
docker logs spark-worker --tail=5
# {"timestamp":"2024-01-15T06:03:22","level":"INFO","message":"Etapa bron
# ... linha cortada, JSON inválido!

# Ou o último log antes de um crash não aparece:
# Último log visível: "Iniciando etapa silver"
# Mas o erro que causou o crash não foi registrado
```

**Causa:**
1. **Buffer do Python não flushado antes do crash** — O processo morreu antes do flush
2. **Docker log driver com limite de tamanho** — `json-file` driver tem limite padrão
3. **Container OOM-killed** — Processo morreu abruptamente, buffer perdido
4. **Log rotation removendo logs antigos** — Docker rotaciona por padrão

**Solução:**
```python
# 1. Forçar flush após cada log (garante que nada se perde):
import sys

class FlushHandler(logging.StreamHandler):
    """Handler que faz flush após cada mensagem."""
    def emit(self, record):
        super().emit(record)
        self.flush()  # Força escrita imediata

# Usar no setup:
handler = FlushHandler(sys.stdout)
handler.setFormatter(JsonFormatter(pipeline_name, data_ref))
logger.addHandler(handler)
```

```yaml
# 2. Configurar Docker log driver sem truncamento:
# No docker-compose.yml:
services:
  spark-worker:
    logging:
      driver: "json-file"
      options:
        max-size: "50m"     # Aumentar limite por arquivo
        max-file: "5"       # Manter 5 arquivos de rotação
```

```bash
# 3. Se logs foram perdidos por OOM, verificar no host:
# Logs do Docker ficam em:
sudo cat /var/lib/docker/containers/<container_id>/<container_id>-json.log

# 4. Ver eventos do Docker (mostra se container foi killed):
docker events --filter container=spark-worker --since 1h

# 5. Para debug, redirecionar logs para arquivo persistente via volume:
# No docker-compose.yml:
# volumes:
#   - ./logs:/opt/spark/logs
# E no pipeline, adicionar FileHandler além do StreamHandler
```

```python
# 6. Adicionar handler de arquivo como backup:
file_handler = logging.FileHandler("/opt/spark/logs/pipeline.log")
file_handler.setFormatter(JsonFormatter(pipeline_name, data_ref))
logger.addHandler(file_handler)
# Com volume montado, o arquivo persiste mesmo se o container morrer
```

**Prevenção:**
- Use `PYTHONUNBUFFERED=1` ou `FlushHandler` para garantir flush imediato
- Configure limites de log generosos no Docker (`max-size: 50m`)
- Monte um volume para logs persistentes como backup
- Sempre registre um log antes de operações que podem dar OOM

---
## 5. Container OOM — Spark Executor Out of Memory

**Sintoma:**
```bash
# O container morre abruptamente durante processamento:
docker compose ps
# spark-worker   Exited (137)    ← código 137 = killed por OOM

# Ou nos logs do Spark antes do crash:
# java.lang.OutOfMemoryError: Java heap space
# ERROR Executor: Exception in task 0.0 in stage 2.0 (TID 5)

# Ou o Docker mata o container:
# docker events mostra:
# container kill spark-worker (signal=9)  ← SIGKILL do OOM Killer
```

**Causa:**
1. **Memória do container Docker menor que o Spark tenta usar** — Spark aloca mais que o limite do container
2. **`spark.executor.memory` + overhead > memória do container** — O Spark precisa de memória extra (10%) para overhead
3. **Dados muito grandes para a memória disponível** — Cache/persist em memória esgota heap
4. **Memory leak em UDFs** — UDFs que acumulam dados sem liberar

**Solução:**
```yaml
# 1. Aumentar memória do container no Docker Compose:
services:
  spark-worker:
    image: bitnami/spark:3.5
    environment:
      - SPARK_WORKER_MEMORY=4g          # Total disponível para o worker
    deploy:
      resources:
        limits:
          memory: 4G                    # Limite hard do container
        reservations:
          memory: 2G                    # Mínimo garantido
```

```python
# 2. Ajustar memória do Spark proporcionalmente ao container:
spark = SparkSession.builder \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memoryOverhead", "512m") \
    .config("spark.driver.memoryOverhead", "512m") \
    .getOrCreate()

# Regra geral para containers Docker:
# container_memory >= executor_memory + memoryOverhead + 512MB (sistema)
# Exemplo: container 4G → executor 2G + overhead 512M + sistema 512M = 3G (OK)
```

```bash
# 3. Verificar uso de memória em tempo real:
docker stats spark-worker
# CONTAINER      CPU %   MEM USAGE / LIMIT   MEM %
# spark-worker   85%     3.2GiB / 4GiB       80%    ← perigoso se subir mais

# 4. Verificar se o container foi morto por OOM:
docker inspect spark-worker | grep -i oom
# "OOMKilled": true  ← confirmado!

# 5. Ver memória disponível no host:
free -h
# Se o host não tem memória suficiente, reduzir o cluster
```

```python
# 6. Otimizar uso de memória no pipeline:
# ❌ ERRADO — cache sem necessidade:
df = spark.read.parquet(caminho)
df.cache()  # Carrega tudo na memória!
df.count()
df.groupBy("estado").sum("valor").show()

# ✅ CORRETO — processar sem cache quando possível:
df = spark.read.parquet(caminho)
df.groupBy("estado").sum("valor").show()

# ✅ Se precisar de cache, usar DISK quando memória é limitada:
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# 7. Para datasets grandes, processar em batches por partição:
for data in datas_para_processar:
    df_dia = spark.read.parquet(f"incoming/{data}/")
    processar(df_dia)
    df_dia.unpersist()  # Liberar memória após processar
```

**Prevenção:**
- Container Docker deve ter no mínimo 2x a memória do `spark.executor.memory`
- Monitore com `docker stats` durante desenvolvimento para calibrar
- Evite `.cache()` em pipelines de produção — deixe o Spark gerenciar
- Use `spark.sql.adaptive.enabled=true` (Spark 3+) para otimização automática

---
## 6. SparkSubmitOperator Não Conecta ao spark-master

**Sintoma:**
```
# No log da task no Airflow:
[2024-01-15 06:00:15] ERROR - Connection refused to spark://spark-master:7077
# Ou:
[2024-01-15 06:00:15] ERROR - Could not connect to spark-master:7077

# Ou o job fica pendente eternamente:
# WARN StandaloneAppClient: Failed to connect to master spark-master:7077
# ... retrying ...
```

**Causa:**
1. **Spark master não está rodando** — Container crashou ou não subiu
2. **Airflow e Spark em redes Docker diferentes** — Não conseguem se comunicar
3. **Conexão `spark_default` não configurada no Airflow** — Operator usa conn_id errado
4. **Hostname errado** — Usando `localhost` ao invés do nome do serviço Docker

**Solução:**
```bash
# 1. Verificar se o Spark master está rodando:
docker compose ps spark-master
# Deve mostrar "Up" com porta 7077

# 2. Testar conectividade entre containers:
docker exec airflow-scheduler ping spark-master
# Se falhar → redes diferentes

# 3. Verificar se estão na mesma rede Docker:
docker network inspect dataflow_network | grep -A5 "spark-master"
docker network inspect dataflow_network | grep -A5 "airflow-scheduler"
# Ambos devem aparecer na mesma rede
```

```yaml
# 4. No docker-compose, garantir mesma rede para todos:
services:
  spark-master:
    networks:
      - dataflow_network

  airflow-scheduler:
    networks:
      - dataflow_network

networks:
  dataflow_network:
    driver: bridge
```

```bash
# 5. Configurar conexão spark_default no Airflow:
docker exec airflow-scheduler airflow connections add spark_default \
    --conn-type spark \
    --conn-host "spark://spark-master" \
    --conn-port 7077

# 6. Verificar conexão existente:
docker exec airflow-scheduler airflow connections get spark_default

# 7. Se a conexão existe mas com host errado:
docker exec airflow-scheduler airflow connections delete spark_default
docker exec airflow-scheduler airflow connections add spark_default \
    --conn-type spark \
    --conn-host "spark://spark-master" \
    --conn-port 7077
```

```python
# 8. No SparkSubmitOperator, verificar conn_id:
spark_task = SparkSubmitOperator(
    task_id="spark_submit_vendas",
    conn_id="spark_default",                    # ✅ Deve existir no Airflow
    application="/opt/spark/jobs/pipeline_vendas.py",  # Path no container
    application_args=["--data-ref", "{{ ds }}"],
)
```

**Prevenção:**
- Sempre coloque todos os serviços na mesma rede Docker
- Crie a conexão `spark_default` no serviço `airflow-init` (inicialização)
- Use hostname do serviço Docker (`spark-master`), nunca `localhost`
- Teste com `docker exec airflow-scheduler ping spark-master` antes de executar

---
## 7. FileSensor Nunca Detecta o Arquivo

**Sintoma:**
```
# A task FileSensor fica em "running" por horas e eventualmente:
[2024-01-15 08:00:00] ERROR - Sensor timed out after 7200 seconds
# AirflowSensorTimeout: Snap. Time is OUT. DAG id: dataflow_pipeline_vendas_producao

# Ou continua em "sensing" indefinidamente:
[2024-01-15 06:00:05] INFO - Poking for file: incoming/2024-01-15/vendas.parquet
[2024-01-15 06:05:05] INFO - Poking for file: incoming/2024-01-15/vendas.parquet
# ... repete infinitamente ...
```

**Causa:**
1. **Arquivo está em path diferente do que o sensor verifica** — Path do host vs path do container
2. **Volume não montado corretamente** — O container não vê o diretório de dados
3. **Permissões incorretas** — O processo Airflow não tem permissão de leitura
4. **Conexão `fs_default` não configurada** — FileSensor precisa de uma conn do tipo File
5. **Template `{{ ds }}` não renderiza como esperado** — Formato de data diferente

**Solução:**
```bash
# 1. Verificar se o arquivo existe DENTRO do container:
docker exec airflow-scheduler ls -la /opt/spark/data/incoming/2024-01-15/
# Se "No such file or directory" → volume não montado ou path errado

# 2. Verificar volumes montados:
docker inspect airflow-scheduler | grep -A10 "Mounts"
# Confirmar que o diretório de dados está montado

# 3. Verificar permissões:
docker exec airflow-scheduler ls -la /opt/spark/data/incoming/
# O usuário airflow precisa ter permissão de leitura
```

```yaml
# 4. Garantir que o volume está montado no scheduler:
services:
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ../data/aula_07/producao:/opt/spark/data  # ← Dados acessíveis!
```

```python
# 5. Verificar o filepath do FileSensor (path DENTRO do container):
sensor = FileSensor(
    task_id="aguardar_arquivo_vendas",
    # ❌ ERRADO — path do host:
    # filepath="data/aula_07/producao/incoming/{{ ds }}/vendas.parquet",
    
    # ✅ CORRETO — path dentro do container:
    filepath="/opt/spark/data/incoming/{{ ds }}/vendas.parquet",
    
    fs_conn_id="fs_default",  # Conexão do tipo File (path)
    timeout=7200,             # 2 horas
    poke_interval=300,        # Verificar a cada 5 minutos
)

# 6. Testar renderização do template:
# docker exec airflow-scheduler airflow tasks render \
#     dataflow_pipeline_vendas_producao aguardar_arquivo_vendas 2024-01-15
```

```bash
# 7. Configurar conexão fs_default (se necessário):
docker exec airflow-scheduler airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/"}'

# 8. Simular chegada do arquivo para teste:
docker exec airflow-scheduler mkdir -p /opt/spark/data/incoming/2024-01-15/
docker exec airflow-scheduler touch /opt/spark/data/incoming/2024-01-15/vendas.parquet
```

**Prevenção:**
- O filepath do FileSensor deve ser o path **dentro do container**, não do host
- Monte os dados como volume compartilhado entre Airflow e Spark
- Use `airflow tasks render` para validar que `{{ ds }}` resolve corretamente
- Configure `timeout` adequado (2h para produção) e `poke_interval` (5min)

---
## 8. DAG Não Aparece na Interface do Airflow

**Sintoma:**
```
# Você criou dag_pipeline_vendas.py mas na UI do Airflow:
# - A DAG não aparece na lista
# - Ou aparece com ícone de erro vermelho
# - Ou mensagem: "DAG Import Error"

# Verificando via CLI:
docker exec airflow-scheduler airflow dags list-import-errors
# /opt/airflow/dags/dag_pipeline_vendas.py:
#   ModuleNotFoundError: No module named 'airflow.providers.apache.spark'
```

**Causa:**
1. **Arquivo DAG não está no volume montado** — O scheduler não vê o arquivo
2. **Provider `apache-airflow-providers-apache-spark` não instalado** — SparkSubmitOperator precisa do provider
3. **Erro de sintaxe no arquivo Python** — Qualquer erro impede o parse
4. **DAG sem objeto DAG no escopo global** — Parser não encontra a DAG

**Solução:**
```bash
# 1. Verificar se o arquivo está dentro do container:
docker exec airflow-scheduler ls -la /opt/airflow/dags/
# dag_pipeline_vendas.py deve aparecer aqui

# 2. Verificar erros de importação:
docker exec airflow-scheduler airflow dags list-import-errors

# 3. Instalar provider do Spark (se faltando):
docker exec airflow-scheduler pip install apache-airflow-providers-apache-spark

# 4. Testar sintaxe Python do arquivo:
docker exec airflow-scheduler python /opt/airflow/dags/dag_pipeline_vendas.py
# Se não houver output → arquivo válido

# 5. Forçar re-scan:
docker exec airflow-scheduler airflow dags reserialize
```

```yaml
# 6. No docker-compose, garantir volume de DAGs:
services:
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags    # ← Volume com arquivos .py
    environment:
      - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
      - AIRFLOW__CORE__LOAD_EXAMPLES=False

  airflow-webserver:
    volumes:
      - ./dags:/opt/airflow/dags    # ← Webserver também precisa!
```

```bash
# 7. Se o provider não persistir após restart, instalar via Dockerfile ou requirements:
# Criar arquivo requirements.txt:
echo "apache-airflow-providers-apache-spark" > requirements.txt

# No docker-compose, instalar na inicialização:
# command: bash -c "pip install -r /opt/airflow/requirements.txt && airflow scheduler"
```

**Prevenção:**
- Monte o diretório `dags/` como volume tanto no scheduler quanto no webserver
- Pré-instale providers necessários na imagem ou no `airflow-init`
- Sempre teste com `python dag_file.py` antes de copiar para o volume
- Aguarde 30-60 segundos após criar o arquivo para o scheduler detectar

---
## 9. Docker Compose Serviços Falhando ao Subir

**Sintoma:**
```bash
docker compose -f docker-compose.producao.yml up -d
# Creating spark-master ... error
# ERROR: for spark-master  Cannot start service spark-master:
#   driver failed programming external connectivity on endpoint spark-master:
#   Bind for 0.0.0.0:8080 failed: port is already allocated

# Ou:
# ERROR: for airflow-webserver  pull access denied for apache/airflow:2.8.1
#   repository does not exist or may require 'docker login'

# Ou:
# spark-worker | WARN: Failed to connect to spark-master:7077
# spark-worker | Exiting with code 1
```

**Causa:**
1. **Conflito de portas** — Outro serviço já usa a porta 8080 ou 8081
2. **Imagem Docker não encontrada** — Tag errada ou sem acesso ao registry
3. **Ordem de inicialização incorreta** — Worker sobe antes do master estar pronto
4. **Falta de memória no host** — Muitos containers para a máquina

**Solução:**
```bash
# 1. Identificar conflito de portas:
lsof -i :8080
# Se outro processo usa, trocar a porta no docker-compose:
# ports:
#   - "8090:8080"  # Mapear para porta alternativa no host

# 2. Verificar se a imagem existe:
docker pull bitnami/spark:3.5
docker pull apache/airflow:2.8.1
# Se falhar, verificar o nome correto no Docker Hub

# 3. Derrubar containers antigos que podem conflitar:
docker compose down -v
docker compose -f docker-compose.producao.yml down -v

# 4. Verificar memória disponível no host:
free -h
docker system df
# Se pouca memória, limpar imagens e containers antigos:
docker system prune -f
```

```yaml
# 5. Configurar depends_on com healthcheck para ordem correta:
services:
  spark-master:
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 10s
      timeout: 5s
      retries: 5

  spark-worker:
    depends_on:
      spark-master:
        condition: service_healthy  # Só sobe quando master estiver saudável

  airflow-scheduler:
    depends_on:
      airflow-init:
        condition: service_completed_successfully
```

```bash
# 6. Subir serviços em ordem (para debug):
docker compose -f docker-compose.producao.yml up -d spark-master
sleep 10
docker compose -f docker-compose.producao.yml up -d spark-worker
sleep 5
docker compose -f docker-compose.producao.yml up -d airflow-init
# ... e assim por diante

# 7. Ver logs de um serviço específico que falhou:
docker compose -f docker-compose.producao.yml logs spark-worker --tail=30
```

**Prevenção:**
- Use portas não-padrão no docker-compose do lab (8081 para Airflow, 8090 para Spark)
- Defina `depends_on` com `condition: service_healthy` para garantir ordem
- Verifique memória antes de subir o stack: precisa de no mínimo 4GB livres
- Use `docker compose config` para validar o YAML antes de subir

---
## 10. Quality Checks Falhando com Falsos Positivos

**Sintoma:**
```
# A task de quality check falha, mas os dados parecem corretos:
[2024-01-15 06:10:00] ERROR - Quality check FAILED: completude
#   Resultado: 97.5% (threshold: 99%)
#   Coluna: email — 2.5% de nulls

# Ou:
[2024-01-15 06:10:00] ERROR - Quality check FAILED: unicidade
#   Duplicatas encontradas: 150 registros
#   Mas são duplicatas legítimas (mesma pessoa, compras diferentes)
```

**Causa:**
1. **Threshold muito restritivo** — 99% completude para colunas opcionais
2. **Check aplicado na coluna errada** — Verificando unicidade em coluna não-chave
3. **Dados de teste com características diferentes dos de produção** — Amostra não representa
4. **Check não considera regras de negócio** — Nulls podem ser válidos para certos campos

**Solução:**
```python
# 1. Ajustar thresholds por coluna (nem todas precisam ser 99%):
quality_checks = {
    "completude": {
        "order_id": 1.0,        # Chave — DEVE ser 100%
        "valor_total": 1.0,     # Obrigatório
        "email": 0.90,          # Opcional — 90% é aceitável
        "telefone": 0.80,       # Opcional — 80% é aceitável
    },
    "unicidade": {
        "order_id": 1.0,        # Chave — DEVE ser única
        # NÃO verificar unicidade em: nome, email, cidade
    }
}

# 2. Verificar unicidade apenas em colunas-chave:
# ❌ ERRADO — email não é chave (pessoa pode ter várias compras):
df.groupBy("email").count().filter("count > 1")

# ✅ CORRETO — verificar apenas o order_id:
duplicatas = df.groupBy("order_id").count().filter("count > 1")
if duplicatas.count() > 0:
    raise Exception(f"Duplicatas em order_id: {duplicatas.count()}")

# 3. Adicionar exceções para campos conhecidamente parciais:
COLUNAS_OPCIONAIS = ["email", "telefone", "complemento"]

for col_name, threshold in checks["completude"].items():
    completude = df.filter(col(col_name).isNotNull()).count() / total
    if completude < threshold:
        if col_name in COLUNAS_OPCIONAIS:
            logger.warning(f"Completude baixa em {col_name}: {completude:.1%}")
        else:
            raise Exception(f"Check falhou: {col_name} = {completude:.1%}")
```

```bash
# 4. Investigar os dados que estão falhando o check:
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark -e "
df = spark.read.parquet('silver/vendas/data_ref=2024-01-15')
# Ver quais colunas têm nulls:
from pyspark.sql.functions import count, when, col
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
"
```

**Prevenção:**
- Defina thresholds baseados em dados históricos reais, não em valores ideais
- Separe checks obrigatórios (hard fail) de alertas (soft warning)
- Campos opcionais devem ter thresholds mais baixos (80-90%)
- Revise checks periodicamente conforme o volume e qualidade dos dados evoluem

---
## 11. Pipeline Funciona Localmente mas Falha no Docker

**Sintoma:**
```bash
# Localmente (no host):
python pipeline_vendas.py --data-ref 2024-01-15
# ✅ Funciona perfeitamente!

# No Docker (via SparkSubmitOperator ou docker exec):
docker exec spark-worker spark-submit /opt/spark/jobs/pipeline_vendas.py --data-ref 2024-01-15
# ❌ FileNotFoundError: /opt/spark/data/incoming/2024-01-15/vendas.parquet
# Ou:
# ModuleNotFoundError: No module named 'structured_logging'
```

**Causa:**
1. **Caminhos absolutos do host não existem no container** — `/home/user/data/` não existe dentro do container
2. **Módulos Python auxiliares não estão no PYTHONPATH do container** — `structured_logging.py` não foi copiado
3. **Variáveis de ambiente diferentes** — O container tem PATH e ENV diferentes do host
4. **Versão do Python/Spark diferente** — Container pode ter versão diferente da local

**Solução:**
```yaml
# 1. Montar TODOS os arquivos necessários como volumes:
services:
  spark-worker:
    volumes:
      - ./spark_jobs:/opt/spark/jobs            # Scripts Python
      - ../data/aula_07/producao:/opt/spark/data  # Dados
    environment:
      - PYTHONPATH=/opt/spark/jobs              # ← Módulos encontráveis!
```

```python
# 2. No pipeline_vendas.py, usar caminhos relativos ou configuráveis:
# ❌ ERRADO — path absoluto do host:
INPUT_PATH = "/home/user/data/aula_07/producao"

# ✅ CORRETO — path via argumento CLI:
parser.add_argument("--input-path", default="/opt/spark/data")
# Funciona tanto local quanto no container (basta trocar o argumento)
```

```bash
# 3. Verificar se os módulos estão acessíveis no container:
docker exec spark-worker ls /opt/spark/jobs/
# Deve listar: pipeline_vendas.py, structured_logging.py

# 4. Testar import dentro do container:
docker exec spark-worker python -c "
import sys
sys.path.insert(0, '/opt/spark/jobs')
from structured_logging import setup_structured_logger
print('✅ Import OK')
"

# 5. No spark-submit, adicionar --py-files para módulos auxiliares:
spark-submit \
    --py-files /opt/spark/jobs/structured_logging.py \
    /opt/spark/jobs/pipeline_vendas.py --data-ref 2024-01-15
```

```python
# 6. No SparkSubmitOperator da DAG, incluir py_files:
spark_task = SparkSubmitOperator(
    task_id="spark_submit_vendas",
    conn_id="spark_default",
    application="/opt/spark/jobs/pipeline_vendas.py",
    application_args=["--data-ref", "{{ ds }}"],
    py_files="/opt/spark/jobs/structured_logging.py",  # ← Módulos auxiliares!
)
```

**Prevenção:**
- Use sempre paths relativos ou configuráveis via CLI/variável de ambiente
- Monte o diretório completo de scripts (não só o main) no container
- Configure `PYTHONPATH` no container para incluir o diretório dos scripts
- Use `--py-files` no spark-submit para dependências Python extras

---
## Quick Reference: Tabela de Diagnóstico Rápido

| Sintoma | Causa Provável | Solução Rápida |
|---------|---------------|----------------|
| Dados duplicados após rerun | `partitionOverwriteMode` não é `dynamic` | Adicionar `.config("...partitionOverwriteMode", "dynamic")` |
| Overwrite apaga todas partições | Modo `static` (padrão) ativo | Configurar `dynamic` **antes** de criar SparkSession |
| Logs JSON não aparecem no Docker | Handler usa stderr ou buffer não flushado | `PYTHONUNBUFFERED=1` + `StreamHandler(sys.stdout)` |
| Logs cortados antes do crash | Buffer Python não flushado | Usar `FlushHandler` ou `PYTHONUNBUFFERED=1` |
| Container exit code 137 | OOM Killer matou o processo | Aumentar `deploy.resources.limits.memory` |
| `java.lang.OutOfMemoryError` | `spark.executor.memory` insuficiente | Aumentar memória ou reduzir cache |
| SparkSubmit "Connection refused" | Spark master não está pronto ou rede errada | Verificar `depends_on` e rede Docker |
| FileSensor timeout | Arquivo em path errado no container | Verificar volumes e usar path do container |
| DAG Import Error | Provider `apache-spark` não instalado | `pip install apache-airflow-providers-apache-spark` |
| Port already allocated | Outra aplicação usando a porta | Trocar porta no docker-compose ou `docker compose down` |
| Quality check falha (dados OK) | Threshold muito restritivo | Ajustar para dados opcionais (80-90%) |
| "ModuleNotFoundError" no Docker | Módulo não está no PYTHONPATH do container | Montar volume + configurar `PYTHONPATH` |
| Pipeline OK local, falha no Docker | Paths absolutos do host | Usar paths via CLI args ou variáveis |

---

## Fluxo de Diagnóstico: Árvore de Decisão

```
Pipeline não funciona?
├── Dados duplicados → Problema 1 ou 2 (idempotência)
│   ├── count aumenta a cada run? → mode("append") — trocar para overwrite
│   ├── Partições desaparecem? → partitionOverwriteMode static — trocar para dynamic
│   └── Coluna de partição muda? → Usar lit(data_ref), não current_timestamp()
│
├── Logs não aparecem → Problema 3 ou 4 (logging)
│   ├── Nenhum log visível? → Verificar PYTHONUNBUFFERED e handler stdout
│   ├── Logs cortados? → FlushHandler + limites de log Docker
│   └── Só logs do Spark (não do pipeline)? → setLogLevel("WARN") + verificar logger
│
├── Container morre → Problema 5 (OOM)
│   ├── Exit code 137? → OOM → aumentar memória do container
│   ├── java.lang.OutOfMemoryError? → Aumentar spark.executor.memory
│   └── docker stats mostra 99%? → Otimizar pipeline ou adicionar memória
│
├── Airflow não executa → Problemas 6, 7 ou 8
│   ├── SparkSubmit falha com connection refused? → Problema 6 (rede/master)
│   ├── FileSensor em timeout? → Problema 7 (path/volume)
│   └── DAG não aparece na UI? → Problema 8 (import/provider)
│
├── Docker Compose não sobe → Problema 9
│   ├── Port conflict? → Trocar porta ou derrubar containers antigos
│   ├── Image not found? → Verificar nome/tag no Docker Hub
│   └── Worker falha ao conectar? → depends_on com healthcheck
│
└── Funciona local, falha no Docker → Problema 11
    ├── FileNotFoundError? → Montar volume correto
    ├── ModuleNotFoundError? → PYTHONPATH ou --py-files
    └── Versão diferente? → Verificar Python/Spark no container
```

---

## Comandos Úteis para Debug

```bash
# === Status Geral ===
docker compose -f docker-compose.producao.yml ps     # Status de todos os serviços
docker compose -f docker-compose.producao.yml logs --tail=30  # Logs recentes
docker stats                                          # Uso de recursos em tempo real

# === Spark ===
docker exec spark-master cat /opt/bitnami/spark/logs/spark-master.out  # Logs do master
docker exec spark-worker cat /opt/bitnami/spark/logs/spark-worker.out  # Logs do worker
docker exec spark-worker env | grep SPARK             # Variáveis do Spark

# === Airflow ===
docker exec airflow-scheduler airflow dags list                    # Listar DAGs
docker exec airflow-scheduler airflow dags list-import-errors      # Erros de import
docker exec airflow-scheduler airflow connections list              # Listar conexões
docker exec airflow-scheduler airflow tasks test \
    dataflow_pipeline_vendas_producao spark_submit_vendas 2024-01-15  # Testar task

# === Pipeline ===
docker exec spark-worker python /opt/spark/jobs/pipeline_vendas.py --help  # Verificar CLI
docker exec spark-worker python -c "import structured_logging; print('OK')"  # Testar import

# === Diagnóstico de Memória ===
docker inspect spark-worker | grep -i "oom\|memory"   # OOM e limites
free -h                                                # Memória do host
docker system df                                       # Espaço usado por Docker

# === Rede ===
docker network ls                                      # Listar redes
docker exec airflow-scheduler ping spark-master        # Testar conectividade
docker exec airflow-scheduler nslookup spark-master    # Resolução DNS
```

### URLs Importantes

| Serviço | URL | Função |
|---------|-----|--------|
| Spark Master UI | http://localhost:8080 | Workers, jobs em execução, memória |
| Airflow Webserver | http://localhost:8081 | DAGs, tasks, logs, XComs |
| Spark Worker UI | http://localhost:8081 | Executors, logs dos jobs |

---

> **Marina:** "Produção é onde os problemas reais aparecem. A maioria se concentra em 3 categorias: idempotência (dados), observabilidade (logs) e recursos (memória). Minha dica: sempre teste no Docker **antes** de confiar que 'funciona local'. E quando algo der errado, o primeiro lugar para olhar são os logs do container — `docker logs <container>` é seu melhor amigo. Quase sempre a resposta está ali."

