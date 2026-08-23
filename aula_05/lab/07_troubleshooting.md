# Troubleshooting — Aula 5: Orquestração Avançada com Airflow

## Contexto

> **Carlos Mendes (Engenheiro de Dados):** "Depois de dominar as DAGs básicas, achei que a parte avançada seria tranquila. Errado. Sensors que travam o scheduler, branches que pulam tasks sem explicação, SparkSubmitOperator que não encontra o jar... Marina me salvou com este guia. Os problemas avançados são mais sutis, mas uma vez que você entende o padrão, resolve em minutos."

---

## 1. Sensor Timeout Inesperado

**Sintoma:**
```python
# FileSensor falha com timeout mesmo que o arquivo exista:
[2024-01-15 08:30:00] INFO - Poking for file: /data/vendas/2024-01-15/vendas.csv
[2024-01-15 08:30:30] INFO - Poking for file: /data/vendas/2024-01-15/vendas.csv
...
[2024-01-15 09:00:00] ERROR - Sensor timed out after 1800.0 seconds
# Mas o arquivo ESTÁ lá! Você verificou manualmente.
```

**Causa:**
1. **Caminho do arquivo incorreto** — O sensor procura em path diferente de onde o arquivo foi criado
2. **`fs_conn_id` errado ou não configurado** — A connection aponta para diretório diferente
3. **Permissões de leitura** — O usuário do Airflow não tem acesso ao diretório
4. **Arquivo com extensão/formato diferente** — O sensor procura `.csv` mas o arquivo é `.csv.gz`

**Solução:**
```python
# ❌ ERRADO — caminho relativo ou incorreto:
sensor = FileSensor(
    task_id='esperar_arquivo',
    filepath='vendas/2024-01-15/vendas.csv',  # Relativo a quê?
    fs_conn_id='fs_default',
    timeout=1800,
    dag=dag,
)

# ✅ CORRETO — caminho absoluto dentro do container:
sensor = FileSensor(
    task_id='esperar_arquivo',
    filepath='/opt/airflow/data/vendas/2024-01-15/vendas.csv',
    fs_conn_id='fs_default',  # Connection com path="" (raiz)
    timeout=1800,
    poke_interval=30,
    dag=dag,
)
```

```bash
# 1. Verificar onde o arquivo realmente está DENTRO do container:
docker exec airflow-scheduler ls -la /opt/airflow/data/vendas/2024-01-15/

# 2. Verificar a connection fs_default na UI:
# Admin → Connections → fs_default
# O campo "Extra" ou "Path" define o diretório base

# 3. Verificar permissões:
docker exec airflow-scheduler stat /opt/airflow/data/vendas/2024-01-15/vendas.csv

# 4. Testar o sensor manualmente:
docker exec airflow-scheduler python -c "
import os
path = '/opt/airflow/data/vendas/2024-01-15/vendas.csv'
print(f'Existe: {os.path.exists(path)}')
print(f'Leitura: {os.access(path, os.R_OK)}')
"
```

```python
# 5. Usar wildcard para flexibilidade no nome:
from airflow.sensors.filesystem import FileSensor

sensor = FileSensor(
    task_id='esperar_arquivo',
    filepath='/opt/airflow/data/vendas/2024-01-15/vendas*',  # Aceita .csv, .csv.gz, etc.
    fs_conn_id='fs_default',
    timeout=1800,
    poke_interval=30,
    dag=dag,
)
```

**Prevenção:**
- Sempre use caminhos absolutos no `filepath` do FileSensor
- Teste o caminho manualmente no container antes de configurar o sensor
- Configure a connection `fs_default` com path vazio (`""`) e use paths absolutos no sensor
- Use `{{ ds }}` ou `{{ ds_nodash }}` para datas dinâmicas no caminho do arquivo

---

## 2. Sensor Ocupa Workers por Horas

**Sintoma:**
```
# Na UI do Airflow, o sensor aparece "running" por horas:
# Task "esperar_arquivo" — Running há 3 horas
# Outras tasks ficam em "queued" sem executar
# Pool "default_pool" mostra 16/16 slots ocupados — todos por sensors!
```

**Causa:**
1. **`mode='poke'` (padrão) com timeout longo** — O sensor ocupa um worker slot durante todo o tempo de poke
2. **Muitos sensors rodando simultaneamente** — Cada sensor em mode=poke bloqueia 1 slot do pool
3. **`timeout` muito alto sem necessidade** — Sensor fica horas esperando algo que nunca vai chegar

**Solução:**
```python
# ❌ ERRADO — mode='poke' bloqueia o worker slot por até 6 horas:
sensor = FileSensor(
    task_id='esperar_arquivo',
    filepath='/opt/airflow/data/vendas.csv',
    mode='poke',          # Padrão! Ocupa slot continuamente
    timeout=6 * 3600,     # 6 horas bloqueando o slot
    poke_interval=60,
    dag=dag,
)

# ✅ CORRETO — mode='reschedule' libera o slot entre pokes:
sensor = FileSensor(
    task_id='esperar_arquivo',
    filepath='/opt/airflow/data/vendas.csv',
    mode='reschedule',    # Libera o slot entre verificações!
    timeout=6 * 3600,     # Pode esperar 6h sem bloquear
    poke_interval=300,    # Verifica a cada 5 minutos
    dag=dag,
)
```

```python
# Comparação de comportamento:
#
# mode='poke' (padrão):
#   [slot ocupado]──poke──sleep──poke──sleep──poke──[slot liberado]
#   Worker fica BLOQUEADO durante todo o período
#
# mode='reschedule':
#   [slot ocupado]──poke──[slot LIVRE]──...──[slot ocupado]──poke──[slot LIVRE]
#   Worker é LIBERADO entre as verificações

# Para cenários com MUITOS sensors simultâneos:
sensor = FileSensor(
    task_id='esperar_arquivo',
    filepath='/opt/airflow/data/vendas.csv',
    mode='reschedule',
    timeout=3600,
    poke_interval=300,
    pool='sensor_pool',   # Pool dedicado para sensors!
    dag=dag,
)
```

```bash
# Criar pool dedicado para sensors (limita quantos rodam ao mesmo tempo):
docker exec airflow-scheduler airflow pools set sensor_pool 4 "Pool exclusivo para sensors"
```

**Prevenção:**
- **Sempre** use `mode='reschedule'` para sensors com timeout > 5 minutos
- Crie um pool dedicado para sensors para não bloquear tasks de processamento
- Use `poke_interval` proporcional à expectativa: arquivo diário → 300s, arquivo a cada minuto → 30s
- Defina `timeout` razoável: se o arquivo deveria chegar em 1h, use timeout de 2h (não 24h)

---

## 3. SparkSubmitOperator: Application Not Found

**Sintoma:**
```
[2024-01-15 08:00:05] INFO - Submitting application: /opt/airflow/dags/scripts/etl_spark.py
[2024-01-15 08:00:06] ERROR - Exception: Cannot execute: /opt/spark/bin/spark-submit.
  File /opt/airflow/dags/scripts/etl_spark.py does not exist
```

Ou:
```
Error: Cannot load main class from file:/opt/airflow/dags/scripts/etl_spark.py
```

**Causa:**
1. **Caminho do script incorreto dentro do container** — O path existe no host mas não no container do Spark
2. **Volume não montado no container do Spark** — O arquivo está acessível no Airflow mas não no Spark Master/Worker
3. **Script em diretório não compartilhado** — Airflow e Spark rodam em containers diferentes com filesystems isolados
4. **Nome do arquivo com typo ou extensão errada** — `.py` vs `.jar`, maiúsculas/minúsculas

**Solução:**
```python
# ❌ ERRADO — path que só existe no container do Airflow:
spark_task = SparkSubmitOperator(
    task_id='processar_vendas',
    application='/opt/airflow/dags/scripts/etl_spark.py',  # Não existe no Spark!
    conn_id='spark_default',
    dag=dag,
)

# ✅ CORRETO — path compartilhado via volume entre Airflow e Spark:
spark_task = SparkSubmitOperator(
    task_id='processar_vendas',
    application='/opt/spark-apps/etl_spark.py',  # Volume montado em ambos containers
    conn_id='spark_default',
    dag=dag,
)
```

```yaml
# docker-compose.yml — montar volume compartilhado:
services:
  airflow-scheduler:
    volumes:
      - ./dags:/opt/airflow/dags
      - ./spark-apps:/opt/spark-apps    # ← Compartilhado!

  spark-master:
    volumes:
      - ./spark-apps:/opt/spark-apps    # ← Mesmo volume!
      - ./data:/opt/data

  spark-worker:
    volumes:
      - ./spark-apps:/opt/spark-apps    # ← Workers também precisam!
      - ./data:/opt/data
```

```bash
# 1. Verificar se o arquivo existe no container do Spark:
docker exec spark-master ls -la /opt/spark-apps/etl_spark.py

# 2. Verificar se o volume está montado corretamente:
docker inspect spark-master | grep -A5 "Mounts"

# 3. Se estiver usando spark-submit local (mesmo container):
docker exec airflow-scheduler ls -la /opt/spark/bin/spark-submit
docker exec airflow-scheduler /opt/spark/bin/spark-submit --version
```

**Prevenção:**
- Use um diretório compartilhado (`/opt/spark-apps`) montado em TODOS os containers (Airflow + Spark)
- Nunca referencie paths do container do Airflow (`/opt/airflow/...`) no SparkSubmitOperator
- Teste o caminho no container correto: `docker exec spark-master ls <path>`
- Mantenha scripts Spark separados dos DAGs para evitar confusão de paths

---

## 4. SparkSubmitOperator: Connection Refused

**Sintoma:**
```
[2024-01-15 08:00:05] ERROR - Connection refused to spark://localhost:7077
# Ou:
[2024-01-15 08:00:05] ERROR - java.net.ConnectException: Connection refused
  (Connection refused to spark-master:7077)
# Ou:
[2024-01-15 08:00:05] ERROR - Could not connect to spark cluster at spark://spark-master:7077
```

**Causa:**
1. **Cluster Spark não está rodando** — Container spark-master não subiu ou crashou
2. **`conn_id` apontando para host errado** — Connection usa `localhost` mas o Spark está em outro container
3. **Rede Docker não compartilhada** — Airflow e Spark em redes Docker diferentes
4. **Porta incorreta na connection** — Spark Master usa 7077 para submissão, não 8080 (que é a UI)

**Solução:**
```bash
# 1. Verificar se o Spark Master está rodando:
docker compose ps spark-master
# Deve mostrar status "Up" — se "Exited", o container crashou

# 2. Ver logs do Spark Master:
docker compose logs spark-master --tail=20

# 3. Testar conectividade do Airflow para o Spark:
docker exec airflow-scheduler ping -c 2 spark-master
docker exec airflow-scheduler nc -zv spark-master 7077
```

```python
# 4. Configurar connection corretamente na UI do Airflow:
# Admin → Connections → spark_default (ou criar nova)
#
# Connection Id: spark_default
# Connection Type: Spark
# Host: spark://spark-master     ← Nome do serviço Docker, NÃO localhost!
# Port: 7077                     ← Porta de submissão (não 8080)
# Extra: {"deploy-mode": "client"}
```

```yaml
# 5. Garantir que Airflow e Spark compartilham a mesma rede Docker:
# docker-compose.yml:
services:
  airflow-scheduler:
    networks:
      - bigdata_net

  spark-master:
    networks:
      - bigdata_net

  spark-worker:
    networks:
      - bigdata_net

networks:
  bigdata_net:
    driver: bridge
```

```bash
# 6. Se a connection não existe, criar via CLI:
docker exec airflow-scheduler airflow connections add spark_default \
    --conn-type spark \
    --conn-host "spark://spark-master" \
    --conn-port 7077 \
    --conn-extra '{"deploy-mode": "client"}'

# 7. Reiniciar o Spark se necessário:
docker compose restart spark-master spark-worker
# Aguardar ~30 segundos para o cluster estabilizar
```

**Prevenção:**
- Sempre use o nome do serviço Docker (`spark-master`) e não `localhost` nas connections
- Verifique que todos os serviços estão na mesma rede Docker
- Porta 7077 = submissão de jobs | Porta 8080 = UI web do Spark (não confundir!)
- Adicione `depends_on: spark-master` no serviço do Airflow no docker-compose

---

## 5. BranchPythonOperator: Task ID Not Found

**Sintoma:**
```python
# Erro no log da task de branching:
[2024-01-15 08:00:05] ERROR - 'processar_grandes' is not a valid task_id in this DAG
# Ou:
[2024-01-15 08:00:05] ERROR - The task_id 'processar_grandes' returned by the branch
  callable is not a task in the DAG
```

**Causa:**
1. **Retorno da função não corresponde ao `task_id` real** — Typo ou nome diferente
2. **Task dentro de TaskGroup usa namespace prefixado** — O ID real é `grupo.task_id`, não apenas `task_id`
3. **Retornando nome da callable ao invés do `task_id`** — Confusão entre nome da função e ID da task
4. **Branch retorna lista com task_id inexistente** — Um dos IDs na lista não existe na DAG

**Solução:**
```python
# ❌ ERRADO — retorna string que não corresponde ao task_id:
def escolher_branch(**context):
    volume = context['ti'].xcom_pull(task_ids='calcular_volume')
    if volume > 10000:
        return 'processar_grandes'  # ← Não existe essa task!
    return 'processar_pequenos'

# Tasks reais definidas:
processar_lotes_grandes = PythonOperator(
    task_id='processar_lotes_grandes',  # ← O nome real é diferente!
    python_callable=proc_grandes,
    dag=dag,
)

# ✅ CORRETO — retornar o task_id EXATO:
def escolher_branch(**context):
    volume = context['ti'].xcom_pull(task_ids='calcular_volume')
    if volume > 10000:
        return 'processar_lotes_grandes'  # ← Exatamente igual ao task_id
    return 'processar_lotes_pequenos'
```

```python
# ❌ ERRADO — task dentro de TaskGroup sem prefixo:
from airflow.utils.task_group import TaskGroup

with DAG('minha_dag', ...) as dag:
    with TaskGroup('etl') as grupo_etl:
        processar = PythonOperator(
            task_id='processar',  # ID real será "etl.processar"!
            python_callable=processar_dados,
        )

    def escolher_branch(**context):
        return 'processar'  # ❌ Deveria ser 'etl.processar'

# ✅ CORRETO — usar o namespace completo do TaskGroup:
    def escolher_branch(**context):
        return 'etl.processar'  # ← Prefixo do grupo + task_id
```

```python
# Dica: imprimir os task_ids disponíveis para verificar:
def escolher_branch(**context):
    dag = context['dag']
    print(f"Tasks disponíveis: {[t.task_id for t in dag.tasks]}")
    # Output: ['calcular_volume', 'branch', 'etl.processar', 'etl.validar', 'notificar']
    
    volume = context['ti'].xcom_pull(task_ids='calcular_volume')
    if volume > 10000:
        return 'etl.processar'
    return 'notificar'
```

```bash
# Verificar task_ids reais da DAG:
docker exec airflow-scheduler airflow tasks list minha_dag
# Mostra: calcular_volume, branch, etl.processar, etl.validar, notificar
```

**Prevenção:**
- Use constantes para task_ids em vez de strings hardcoded:
  ```python
  TASK_PROCESSAR = 'etl.processar'
  ```
- Quando usar TaskGroups, lembre que o ID real é `grupo.task_id`
- Teste com `airflow tasks list <dag_id>` para ver os IDs reais
- O retorno do branch DEVE ser uma string (ou lista de strings) com task_ids existentes

---

## 6. Task Pós-Branch Nunca Executa

**Sintoma:**
```
# Após o BranchPythonOperator, a task de join/merge fica com status "skipped":
#
# branch_task → processar_grandes → join_task (SKIPPED!)
#             → processar_pequenos ↗
#
# O branch escolheu "processar_grandes" (executou com sucesso),
# mas "join_task" foi marcada como SKIPPED mesmo assim!
```

**Causa:**
O `trigger_rule` padrão de toda task é `all_success`. Isso significa que a task só executa se **todos** os upstreams tiverem sucesso. Quando o BranchPythonOperator escolhe um caminho, as tasks do outro caminho ficam com status `skipped`. A task de convergência (join) vê que tem um upstream com `skipped` e, pela regra `all_success`, também é marcada como `skipped`.

**Solução:**
```python
# ❌ ERRADO — join_task usa trigger_rule padrão (all_success):
join_task = PythonOperator(
    task_id='join_resultados',
    python_callable=consolidar,
    # trigger_rule='all_success'  ← Padrão implícito! Falha com branch
    dag=dag,
)

# ✅ CORRETO — usar trigger_rule adequado para pós-branch:
from airflow.utils.trigger_rule import TriggerRule

join_task = PythonOperator(
    task_id='join_resultados',
    python_callable=consolidar,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # ← Ignora skipped!
    dag=dag,
)
```

```python
# Explicação dos trigger_rules úteis para branching:
#
# TriggerRule.ALL_SUCCESS (padrão)
#   → Executa SOMENTE se TODOS os upstreams tiverem sucesso
#   → ❌ NÃO funciona com branches (skipped conta como não-sucesso)
#
# TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS (recomendado para branches)
#   → Executa se nenhum upstream falhou E pelo menos um teve sucesso
#   → ✅ Ignora upstreams com status "skipped"
#
# TriggerRule.NONE_FAILED
#   → Executa se nenhum upstream falhou (skipped é OK)
#   → ✅ Funciona, mas executa mesmo se TODOS foram skipped
#
# TriggerRule.ONE_SUCCESS
#   → Executa assim que QUALQUER upstream tiver sucesso
#   → ⚠️ Pode executar antes de todos os upstreams terminarem

# Exemplo completo com branch e join:
with DAG('pipeline_branch', start_date=datetime(2024, 1, 1), catchup=False) as dag:
    
    inicio = PythonOperator(task_id='inicio', python_callable=extrair)
    
    branch = BranchPythonOperator(
        task_id='decidir_caminho',
        python_callable=escolher_branch,
    )
    
    caminho_a = PythonOperator(task_id='caminho_a', python_callable=proc_a)
    caminho_b = PythonOperator(task_id='caminho_b', python_callable=proc_b)
    
    # ✅ Task de convergência com trigger_rule correto:
    fim = PythonOperator(
        task_id='finalizar',
        python_callable=finalizar,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    
    inicio >> branch >> [caminho_a, caminho_b] >> fim
```

**Prevenção:**
- **Sempre** defina `trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` em tasks que convergem após um branch
- Visualize a DAG na UI antes de executar — verifique se os caminhos de convergência estão corretos
- Lembre: qualquer task downstream de um branch deve considerar que alguns upstreams serão `skipped`
- Documente o trigger_rule com comentário no código para futuros mantenedores

---

## 7. TaskGroup: Namespace Issues com XCom

**Sintoma:**
```python
# xcom_pull retorna None para uma task que está dentro de um TaskGroup:
def usar_resultado(**context):
    # Task "processar" está dentro do TaskGroup "etl"
    resultado = context['ti'].xcom_pull(task_ids='processar')
    print(f"Resultado: {resultado}")
    # Output: Resultado: None  ← Deveria ter dados!
```

**Causa:**
1. **`task_ids` no xcom_pull sem o prefixo do TaskGroup** — O ID real inclui o namespace do grupo
2. **TaskGroups aninhados multiplicam o prefixo** — `grupo_pai.grupo_filho.task_id`
3. **Confusão entre ID do grupo e ID da task** — O `group_id` não é um `task_id` válido para xcom_pull

**Solução:**
```python
# Estrutura com TaskGroup:
with DAG('minha_dag', ...) as dag:
    
    with TaskGroup('etl') as grupo_etl:
        extrair = PythonOperator(
            task_id='extrair',      # ID real: "etl.extrair"
            python_callable=extrair_dados,
        )
        processar = PythonOperator(
            task_id='processar',    # ID real: "etl.processar"
            python_callable=processar_dados,
        )
    
    # Task FORA do grupo tentando acessar XCom:
    def notificar(**context):
        # ❌ ERRADO — task_id sem prefixo do grupo:
        resultado = context['ti'].xcom_pull(task_ids='processar')
        # Retorna None!
        
        # ✅ CORRETO — usar namespace completo:
        resultado = context['ti'].xcom_pull(task_ids='etl.processar')
        print(f"Resultado: {resultado}")
    
    notif = PythonOperator(
        task_id='notificar',
        python_callable=notificar,
    )
    
    grupo_etl >> notif
```

```python
# Com TaskGroups ANINHADOS — o prefixo acumula:
with DAG('minha_dag', ...) as dag:
    
    with TaskGroup('pipeline') as grupo_pipeline:
        with TaskGroup('bronze') as grupo_bronze:
            ingerir = PythonOperator(
                task_id='ingerir',  # ID real: "pipeline.bronze.ingerir"
                python_callable=ingerir_dados,
            )
    
    def consolidar(**context):
        # ✅ Usar o namespace completo (todos os níveis):
        dados = context['ti'].xcom_pull(task_ids='pipeline.bronze.ingerir')
        print(f"Dados: {dados}")
    
    consolidar_task = PythonOperator(
        task_id='consolidar',
        python_callable=consolidar,
    )
```

```bash
# Descobrir os task_ids reais (com namespace):
docker exec airflow-scheduler airflow tasks list minha_dag
# Output:
# etl.extrair
# etl.processar
# notificar

# Verificar XComs existentes na UI:
# Admin → XComs → filtrar por dag_id
# A coluna "task_id" mostra o namespace completo
```

**Prevenção:**
- Ao usar `xcom_pull` para tasks em grupos, **sempre** inclua o prefixo: `grupo.task_id`
- Use `airflow tasks list <dag_id>` para confirmar os IDs reais
- Armazene task_ids em constantes para evitar erros de digitação:
  ```python
  TASK_ETL_PROCESSAR = 'etl.processar'
  ```
- Na UI, a aba XComs (Admin → XComs) mostra o task_id com namespace — use-a como referência

---

## 8. Callback Não Dispara

**Sintoma:**
```python
# Você configurou on_failure_callback mas a task falha e nenhum alerta é enviado:
def alerta_falha(context):
    print(f"ALERTA: Task {context['task_instance'].task_id} falhou!")
    # Enviar email, Slack, etc.

dag = DAG(
    dag_id='pipeline_alertas',
    on_failure_callback=alerta_falha,
    start_date=datetime(2024, 1, 1),
)

# Task falha... mas alerta_falha nunca executa!
```

**Causa:**
1. **Callback no lugar errado** — `on_failure_callback` no nível da DAG vs no nível da task têm comportamentos diferentes
2. **Retries mascaram a falha** — Com `retries=3`, o callback da TASK só dispara após todas as tentativas falharem
3. **Erro de sintaxe na função de callback** — O callback falha silenciosamente se houver exception não capturada
4. **Callback na DAG vs callback na task** — O callback da DAG só dispara quando TODA a DAG falha, não tasks individuais

**Solução:**
```python
# ❌ ERRADO — callback na DAG (só dispara quando toda a DAG falha):
dag = DAG(
    dag_id='pipeline_alertas',
    on_failure_callback=alerta_falha,  # ← Só para falha da DAG inteira!
    start_date=datetime(2024, 1, 1),
)

# ✅ CORRETO — callback na TASK (dispara quando a task específica falha):
task = PythonOperator(
    task_id='processar_dados',
    python_callable=processar,
    on_failure_callback=alerta_falha,  # ← Dispara ao falhar esta task
    retries=0,  # Sem retries, callback dispara na primeira falha
    dag=dag,
)
```

```python
# Callback com retries — entendendo o timing:
task = PythonOperator(
    task_id='processar_dados',
    python_callable=processar,
    on_failure_callback=alerta_falha,   # Dispara APÓS todas as retries falharem
    on_retry_callback=alerta_retry,     # Dispara em CADA retry
    retries=3,
    retry_delay=timedelta(minutes=5),
    dag=dag,
)

# Se quer ser alertado em CADA falha (não só na final):
def alerta_retry(context):
    tentativa = context['task_instance'].try_number
    print(f"⚠️ Retry {tentativa}: Task {context['task_instance'].task_id}")

def alerta_falha(context):
    print(f"🚨 FALHA DEFINITIVA: Task {context['task_instance'].task_id}")
    print(f"   Todas as {context['task_instance'].max_tries + 1} tentativas falharam")
```

```python
# ❌ ERRADO — erro de sintaxe no callback (falha silenciosa):
def alerta_falha(context):
    task_id = context['task_instance'].task_id
    # TypeError: f-string sem f!
    mensagem = "Task {task_id} falhou às {context['execution_date']}"
    enviar_slack(mensagem)  # Nunca chega aqui

# ✅ CORRETO — callback robusto com try/except:
def alerta_falha(context):
    try:
        ti = context['task_instance']
        mensagem = f"🚨 Task {ti.task_id} falhou na DAG {ti.dag_id}"
        mensagem += f"\n   Execução: {context['logical_date']}"
        mensagem += f"\n   Erro: {context.get('exception', 'N/A')}"
        print(mensagem)
        # enviar_slack(mensagem)
    except Exception as e:
        print(f"ERRO NO CALLBACK: {e}")  # Pelo menos loga o erro do callback
```

```python
# Aplicar callback padrão para TODAS as tasks via default_args:
default_args = {
    'owner': 'dataflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': alerta_falha,   # ← Aplicado a toda task!
    'on_retry_callback': alerta_retry,
}

dag = DAG(
    dag_id='pipeline_alertas',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
```

```bash
# Testar o callback isoladamente:
docker exec airflow-scheduler python -c "
# Simular o context que o Airflow passa ao callback:
context = {
    'task_instance': type('TI', (), {'task_id': 'test', 'dag_id': 'test_dag', 'try_number': 1, 'max_tries': 3})(),
    'logical_date': '2024-01-15',
    'exception': ValueError('dados inválidos'),
}

# Importar e testar sua função:
import sys
sys.path.insert(0, '/opt/airflow/dags')
from minha_dag import alerta_falha
alerta_falha(context)
"
```

**Prevenção:**
- Use `on_failure_callback` na **task** (não na DAG) para alertas de tasks individuais
- Sempre envolva o corpo do callback em `try/except` para evitar falhas silenciosas
- Use `on_retry_callback` se quer ser notificado antes da falha definitiva
- Defina callbacks em `default_args` para aplicar a todas as tasks da DAG
- Teste o callback manualmente com um context simulado antes de usar na DAG

---

## Quick Reference: Tabela de Diagnóstico Rápido

| Sintoma | Causa Provável | Solução Rápida |
|---------|---------------|----------------|
| Sensor timeout com arquivo existente | Path errado ou permissão | Verificar path absoluto no container |
| Sensors bloqueiam todos os workers | `mode='poke'` com timeout longo | Usar `mode='reschedule'` |
| "File does not exist" no SparkSubmit | Volume não montado no Spark | Montar mesmo volume em Airflow + Spark |
| "Connection refused" ao Spark | Cluster parado ou conn_id errado | Verificar `spark-master:7077` e connection |
| "task_id not valid" no branch | Retorno não corresponde ao task_id | Usar `airflow tasks list` para IDs reais |
| Task com TaskGroup no branch | Falta prefixo do grupo | Retornar `grupo.task_id` no branch |
| Task pós-branch fica "skipped" | `trigger_rule=all_success` (padrão) | Usar `NONE_FAILED_MIN_ONE_SUCCESS` |
| xcom_pull retorna None (TaskGroup) | task_id sem namespace do grupo | Usar `grupo.task_id` no xcom_pull |
| Callback não dispara na falha | Callback na DAG ao invés da task | Mover para `on_failure_callback` da task |
| Callback dispara só no final | `retries > 0` mascara falhas intermediárias | Usar `on_retry_callback` para cada retry |
| Callback falha silenciosamente | Exception não capturada no callback | Envolver em `try/except` |

---

## Fluxo de Diagnóstico: Árvore de Decisão

```
Sensor falha ou trava?
├── Timeout com arquivo existente → Problema 1 (path/permissão)
├── Sensor ocupa slots por horas → Problema 2 (mode='poke')
└── Sensor nunca detecta arquivo → Verificar fs_conn_id e path no container

SparkSubmitOperator falha?
├── "File does not exist" → Problema 3 (volume não montado)
├── "Connection refused" → Problema 4 (cluster parado/conn_id)
└── "Class not found" → Verificar dependências e --packages

Branch não funciona como esperado?
├── "task_id not valid" → Problema 5 (nome incorreto/namespace)
├── Task pós-branch skipped → Problema 6 (trigger_rule)
└── Branch retorna None → Verificar se a função tem return

TaskGroup com comportamento estranho?
├── xcom_pull retorna None → Problema 7 (namespace no task_id)
├── Branch não encontra task → Problema 5 (prefixo do grupo)
└── Dependências não respeitadas → Verificar task_ids com airflow tasks list

Callback não funciona?
├── Nenhum alerta ao falhar task → Problema 8 (callback na DAG vs task)
├── Alerta só no final de retries → Usar on_retry_callback
└── Callback com erro silencioso → Adicionar try/except e testar isolado
```

---

## Comandos Úteis para Debug

```bash
# === Sensors ===
docker exec airflow-scheduler ls -la /opt/airflow/data/          # Verificar arquivos disponíveis
docker exec airflow-scheduler airflow connections get fs_default  # Ver config da connection

# === SparkSubmit ===
docker compose ps spark-master spark-worker                       # Status do cluster Spark
docker exec spark-master ls /opt/spark-apps/                      # Verificar scripts no Spark
docker exec airflow-scheduler nc -zv spark-master 7077            # Testar conectividade
docker exec airflow-scheduler airflow connections get spark_default  # Ver connection

# === Branching e TaskGroups ===
docker exec airflow-scheduler airflow tasks list <dag_id>         # Ver todos os task_ids reais
docker exec airflow-scheduler airflow tasks test <dag_id> <task> 2024-01-01  # Testar task isolada
docker exec airflow-scheduler airflow dags show <dag_id>          # Ver estrutura da DAG

# === Callbacks ===
docker exec airflow-scheduler airflow tasks test <dag_id> <task> 2024-01-01  # Força execução (dispara callbacks se falhar)
docker compose logs airflow-scheduler --tail=50 | grep "callback"  # Verificar erros de callbacks

# === Geral ===
docker exec airflow-scheduler airflow dags list-import-errors     # Erros de parse em DAGs
docker exec airflow-scheduler airflow config get-value core executor  # Ver executor configurado
```

---

> **Carlos:** "A lição que aprendi é que orquestração avançada exige atenção aos detalhes: namespaces de TaskGroups, trigger_rules pós-branch, e paths compartilhados entre containers. Minha regra de ouro: sempre use `airflow tasks list` para confirmar os IDs reais das tasks, sempre defina `trigger_rule` explicitamente após branches, e nunca confie em paths relativos entre containers diferentes. Parece óbvio depois que você erra, mas são armadilhas que pegam todo mundo na primeira vez."
