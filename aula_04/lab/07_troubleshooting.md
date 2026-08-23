# Troubleshooting — Aula 4: Introdução ao Apache Airflow

## Contexto

> **Marina Silva (Arquiteta de Dados):** "Quando implantamos o Airflow na DataFlow Analytics, Carlos passou os primeiros dias frustrado — DAGs que não apareciam, imports quebrando, o scheduler morrendo do nada. Hoje ele ri dessas situações porque já sabe exatamente onde procurar. Vou compilar aqui os problemas mais comuns que toda equipe enfrenta nas primeiras semanas com Airflow."

---

## 1. DAG Não Aparece na Interface Web

**Sintoma:**
```
# Você criou o arquivo my_dag.py, mas na UI do Airflow:
# - A DAG não aparece na lista
# - Ou aparece com ícone de erro (triângulo vermelho)
# - Ou aparece esmaecida com "DAG Import Error"
```

**Causa:** Existem quatro causas principais:

1. **Arquivo fora da pasta `dags/`** — O Airflow só escaneia o diretório configurado em `dags_folder`
2. **Nenhum objeto DAG no escopo do módulo** — O parser precisa encontrar uma variável do tipo `DAG`
3. **Erro de sintaxe no arquivo** — Qualquer `SyntaxError` impede o parse completo
4. **Scheduler não está rodando** — Sem scheduler, novas DAGs não são detectadas

**Solução:**
```bash
# 1. Verificar se o arquivo está na pasta correta:
docker exec airflow-scheduler ls /opt/airflow/dags/
# Seu arquivo deve aparecer aqui

# 2. Verificar se o scheduler consegue parsear o arquivo:
docker exec airflow-scheduler airflow dags list 2>&1 | grep "my_dag"

# 3. Ver erros de importação detectados pelo Airflow:
docker exec airflow-scheduler airflow dags list-import-errors
```

```python
# 4. Verificar se o arquivo tem um objeto DAG no escopo global:

# ❌ ERRADO — DAG criada dentro de função (não detectada):
def criar_dag():
    dag = DAG("minha_dag", start_date=datetime(2024, 1, 1))
    return dag

# ✅ CORRETO — DAG no escopo global do módulo:
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="minha_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
)

# ✅ CORRETO — Usando context manager (recomendado):
with DAG(
    dag_id="minha_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
) as dag:
    pass  # tasks aqui
```

```bash
# 5. Testar se o arquivo tem erros de sintaxe:
docker exec airflow-scheduler python /opt/airflow/dags/my_dag.py

# 6. Forçar re-scan das DAGs:
docker exec airflow-scheduler airflow dags reserialize
```

**Prevenção:**
- Sempre teste o arquivo com `python my_dag.py` antes de copiar para a pasta `dags/`
- Use nomes únicos para `dag_id` — IDs duplicados causam conflitos silenciosos
- Verifique que o arquivo `.py` não começa com `.` ou `_` (o Airflow ignora esses por padrão)
- Aguarde até 30 segundos após copiar o arquivo (intervalo de scan do scheduler)

---
## 2. Import Error na DAG

**Sintoma:**
```
Broken DAG: [/opt/airflow/dags/etl_vendas.py]
Traceback (most recent call last):
  File "/opt/airflow/dags/etl_vendas.py", line 3, in <module>
    from utils.helpers import limpar_dados
ModuleNotFoundError: No module named 'utils.helpers'
```

Ou:
```
Broken DAG: [/opt/airflow/dags/etl_vendas.py]
ImportError: cannot import name 'SparkSubmitOperator' from 'airflow.providers.apache.spark'
```

**Causa:**
1. **Módulo custom não está no PYTHONPATH** — Arquivos auxiliares em subpastas não são encontrados automaticamente
2. **Caminho de import incorreto** — O Airflow resolve imports relativamente ao `dags_folder`
3. **Pacote/provider não instalado** — Operators de providers externos precisam ser instalados separadamente
4. **Conflito de versão** — Versão do Airflow não tem o módulo no caminho esperado (mudanças entre Airflow 1.x e 2.x)

**Solução:**
```python
# Caso 1: Módulo custom em subpasta
# Estrutura:
# dags/
# ├── etl_vendas.py
# └── utils/
#     ├── __init__.py    ← OBRIGATÓRIO!
#     └── helpers.py

# No etl_vendas.py:
# ✅ CORRETO (com __init__.py na pasta utils/):
from utils.helpers import limpar_dados

# ✅ ALTERNATIVA — adicionar ao path explicitamente:
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from utils.helpers import limpar_dados
```

```bash
# Caso 2: Instalar provider faltante
docker exec airflow-scheduler pip install apache-airflow-providers-apache-spark

# Verificar providers instalados:
docker exec airflow-scheduler airflow providers list

# Caso 3: Verificar versão do Airflow e imports corretos
docker exec airflow-scheduler airflow version
# Airflow 2.x usa: from airflow.operators.python import PythonOperator
# Airflow 1.x usava: from airflow.operators.python_operator import PythonOperator
```

```python
# Caso 4: Imports corretos para Airflow 2.x (usado neste curso):
from airflow import DAG
from airflow.operators.python import PythonOperator        # ✅ Airflow 2.x
from airflow.operators.bash import BashOperator            # ✅ Airflow 2.x
from airflow.operators.empty import EmptyOperator          # ✅ Airflow 2.x

# ❌ Imports antigos (Airflow 1.x) — NÃO usar:
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
```

**Prevenção:**
- Sempre crie `__init__.py` em subpastas dentro de `dags/`
- Use imports absolutos a partir da raiz do `dags_folder`
- Consulte a documentação da sua versão do Airflow para os paths de import corretos
- Teste imports no container: `docker exec airflow-scheduler python -c "from airflow.operators.python import PythonOperator"`

---
## 3. Scheduler Parado ou "Not Running"

**Sintoma:**
```
# Na UI do Airflow, banner vermelho:
"The scheduler does not appear to be running. Last heartbeat was received 5 minutes ago."

# Ou DAGs não executam no horário programado
# Ou novas DAGs nunca são detectadas
```

**Causa:**
1. **Container do scheduler crashou** — Falta de memória ou erro fatal
2. **Banco de dados bloqueado** — SQLite (dev) trava com múltiplos acessos simultâneos
3. **Heartbeat expirado** — Scheduler travou em loop mas o processo ainda existe
4. **Metadatabase não inicializada** — `airflow db init` não foi executado

**Solução:**
```bash
# 1. Verificar status do container:
docker compose ps airflow-scheduler
# Se "Exited (137)" → OOM (Out of Memory)
# Se "Exited (1)"   → Erro de configuração

# 2. Ver logs do scheduler:
docker compose logs airflow-scheduler --tail=50

# 3. Reiniciar o scheduler:
docker compose restart airflow-scheduler

# 4. Se o banco está travado (SQLite), resetar:
docker exec airflow-scheduler airflow db reset --yes
# ⚠️ CUIDADO: Isso apaga todo histórico de execuções!

# 5. Se precisar reinicializar o banco sem perder dados:
docker exec airflow-scheduler airflow db upgrade

# 6. Verificar se o scheduler está realmente rodando:
docker exec airflow-scheduler ps aux | grep "airflow scheduler"

# 7. Se nada funcionar, derrubar e subir tudo novamente:
docker compose down
docker compose up -d
# Aguardar ~60 segundos para o scheduler estabilizar
```

```python
# 8. Verificar saúde do scheduler via CLI:
# docker exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname ""
```

**Prevenção:**
- Use PostgreSQL ao invés de SQLite para ambientes com múltiplos usuários
- Configure limites de memória adequados para o container do scheduler (mínimo 1GB)
- Monitore o heartbeat do scheduler — se parar por mais de 2 minutos, reinicie automaticamente
- Em Docker Compose, adicione `restart: always` ao serviço do scheduler

---
## 4. Task Fica em "Queued" Eternamente

**Sintoma:**
```
# Na UI do Airflow, a task aparece com status "queued" (cor cinza/bege)
# e nunca transiciona para "running" (verde claro)
# O log da task está vazio ou mostra apenas:
"Queued by scheduler"
# ... e nada mais acontece por minutos/horas
```

**Causa:**
1. **Executor não configurado ou parado** — O `SequentialExecutor` (padrão SQLite) executa apenas 1 task por vez
2. **Pool cheio** — A task está atribuída a um pool que atingiu o limite de slots
3. **Dependência não satisfeita** — Uma task upstream não completou com sucesso
4. **Worker indisponível** — Com `CeleryExecutor`, o worker pode ter crashado

**Solução:**
```bash
# 1. Verificar qual executor está configurado:
docker exec airflow-scheduler airflow config get-value core executor
# Se "SequentialExecutor" → só executa 1 task por vez (aguarde a anterior terminar)
# Se "LocalExecutor" → pode executar múltiplas tasks em paralelo

# 2. Verificar tasks atualmente em execução:
docker exec airflow-scheduler airflow tasks states-for-dag-run etl_vendas 2024-01-01

# 3. Ver se há tasks bloqueando a fila:
docker exec airflow-scheduler airflow dags show etl_vendas
```

```python
# 4. Verificar pools disponíveis e ocupação:
# Na UI: Admin → Pools
# Via CLI:
# docker exec airflow-scheduler airflow pools list

# 5. Se o problema é dependência, verificar o status das tasks upstream:
# Na UI: clicar na task → "Task Instance Details" → ver "Depends On Past" e "Upstream Tasks"

# 6. Configurar executor mais robusto no airflow.cfg ou variável de ambiente:
# No docker-compose.yml:
# environment:
#   AIRFLOW__CORE__EXECUTOR: LocalExecutor
#   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
```

```bash
# 7. Limpar task presa e re-agendar:
docker exec airflow-scheduler airflow tasks clear etl_vendas -t task_transform -s 2024-01-01 -e 2024-01-01 --yes

# 8. Se usando pool com limite, aumentar slots:
docker exec airflow-scheduler airflow pools set default_pool 16 "Pool padrão"
```

**Prevenção:**
- Para o ambiente do lab, use `LocalExecutor` com PostgreSQL (já configurado no docker-compose)
- Evite `depends_on_past=True` nas primeiras DAGs — isso pode bloquear a fila inteira
- Monitore pools: Admin → Pools na UI mostra slots usados vs disponíveis
- Se uma task demora muito, verifique se não está esperando recurso externo (banco, API)

---
## 5. XCom Retorna None

**Sintoma:**
```python
# Na task "transform", você tenta puxar dados da task "extract":
def transform(**context):
    dados = context['ti'].xcom_pull(task_ids='extract', key='dados_vendas')
    print(f"Dados recebidos: {dados}")
    # Output: Dados recebidos: None  ← esperava um dicionário!
```

**Causa:**
1. **`task_ids` incorreto no `xcom_pull`** — O nome não corresponde ao `task_id` real
2. **`key` errado** — A task fez push com uma key diferente da que você está puxando
3. **Task upstream não executou ainda** — A dependência não foi definida corretamente
4. **Função não retorna valor** — Para XCom implícito, a callable do PythonOperator deve ter `return`

**Solução:**
```python
# Caso 1: Verificar task_id correto (deve ser exatamente igual ao definido na task)
extract_task = PythonOperator(
    task_id='extract_vendas',  # ← Este é o task_id real
    python_callable=extract_data,
    dag=dag,
)

def transform(**context):
    # ❌ ERRADO — task_id não corresponde:
    dados = context['ti'].xcom_pull(task_ids='extract', key='return_value')
    
    # ✅ CORRETO — usar o task_id exato:
    dados = context['ti'].xcom_pull(task_ids='extract_vendas', key='return_value')


# Caso 2: Push explícito vs implícito
def extract_data(**context):
    dados = {"total": 1500, "registros": 42}
    
    # Push IMPLÍCITO (via return — key será 'return_value'):
    return dados
    
    # Push EXPLÍCITO (com key customizada):
    context['ti'].xcom_push(key='dados_vendas', value=dados)


def transform(**context):
    # Para pull do push IMPLÍCITO (return):
    dados = context['ti'].xcom_pull(task_ids='extract_vendas')  # key padrão = 'return_value'
    
    # Para pull do push EXPLÍCITO:
    dados = context['ti'].xcom_pull(task_ids='extract_vendas', key='dados_vendas')


# Caso 3: Garantir que a dependência está definida
extract_task >> transform_task  # transform só roda DEPOIS de extract


# Caso 4: Verificar se provide_context está habilitado (Airflow 1.x)
# No Airflow 2.x, **context é injetado automaticamente se a função aceita **kwargs
transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    # provide_context=True  ← necessário APENAS no Airflow 1.x
    dag=dag,
)
```

```bash
# 5. Inspecionar XComs existentes via CLI:
docker exec airflow-scheduler airflow tasks test etl_vendas extract_vendas 2024-01-01
# Depois verificar o XCom gerado:
# Na UI: Admin → XComs → filtrar por dag_id e task_id
```

**Prevenção:**
- Use sempre `return` na função para XCom implícito — é a forma mais simples
- Mantenha os `task_ids` como constantes ou variáveis para evitar typos
- Verifique XComs na UI (Admin → XComs) para confirmar que o push funcionou
- Lembre: XCom é para dados pequenos (< 48KB). Para dados grandes, salve em arquivo e passe o path

---
## 6. Template Variable Não Renderiza

**Sintoma:**
```python
# Na task, o {{ ds }} aparece como string literal ao invés do valor da data:
def processar(**context):
    print(context['templates_dict']['data'])
    # Output: {{ ds }}  ← deveria ser "2024-01-15"!

# Ou no BashOperator:
task = BashOperator(
    task_id='show_date',
    bash_command='echo "Processando data: {{ ds }}"',
    dag=dag,
)
# Log mostra: Processando data: {{ ds }}  ← não renderizou!
```

**Causa:**
1. **Template usado em campo não-templated** — Nem todos os parâmetros de operators suportam Jinja templating
2. **String passada via `op_args`/`op_kwargs`** — O PythonOperator não renderiza templates em argumentos diretos
3. **Aspas erradas** — Em `bash_command`, usar aspas simples externas impede substituição de variáveis do shell (mas templates Jinja devem funcionar)
4. **`render_template_as_native_obj` não configurado** — O template retorna string ao invés de tipo nativo

**Solução:**
```python
# Caso 1: No PythonOperator, usar templates_dict (campo templated):
task = PythonOperator(
    task_id='processar',
    python_callable=processar,
    templates_dict={
        'data': '{{ ds }}',              # ✅ Será renderizado!
        'data_formatada': '{{ ds_nodash }}',
    },
    dag=dag,
)

def processar(**context):
    data = context['templates_dict']['data']  # "2024-01-15"
    print(f"Processando: {data}")


# Caso 2: Alternativa mais simples — acessar via context diretamente:
def processar(**context):
    data = context['ds']                # ✅ Já está disponível sem template!
    logical_date = context['logical_date']  # datetime object
    print(f"Processando: {data}")


# Caso 3: No BashOperator, templates funcionam nativamente:
# ✅ CORRETO — bash_command é campo templated:
task = BashOperator(
    task_id='show_date',
    bash_command='echo "Processando: {{ ds }}" && ls /data/{{ ds_nodash }}/',
    dag=dag,
)

# ❌ ERRADO — env não é campo templated por padrão:
task = BashOperator(
    task_id='show_date',
    bash_command='echo $DATA_EXEC',
    env={'DATA_EXEC': '{{ ds }}'},  # Pode não renderizar dependendo da versão
    dag=dag,
)


# Caso 4: Templates em op_kwargs (campos templated no PythonOperator):
task = PythonOperator(
    task_id='processar',
    python_callable=processar,
    op_kwargs={
        'data': '{{ ds }}',  # ✅ op_kwargs É campo templated no Airflow 2.x
    },
    dag=dag,
)

def processar(data, **context):
    print(f"Data recebida: {data}")  # "2024-01-15"
```

**Prevenção:**
- Consulte a documentação do operator para saber quais campos são `template_fields`
- Na dúvida, acesse variáveis pelo `context` diretamente: `context['ds']`, `context['logical_date']`
- Templates mais usados: `{{ ds }}` (YYYY-MM-DD), `{{ ds_nodash }}` (YYYYMMDD), `{{ execution_date }}`
- Use `airflow tasks render dag_id task_id data` para testar renderização sem executar

---
## 7. DAG Executa Múltiplas Vezes ao Ativar (Catchup)

**Sintoma:**
```
# Você ativa a DAG pela primeira vez e de repente:
# - 30 DAG Runs aparecem simultaneamente na UI (um para cada dia desde start_date)
# - O scheduler fica sobrecarregado executando dezenas de runs em paralelo
# - Tasks falham por concorrência de recursos

# Na UI: Grid View mostra uma enxurrada de execuções:
# 2024-01-01 ✓  2024-01-02 ✓  2024-01-03 ✓  ... 2024-01-30 ●(running)
```

**Causa:** Por padrão, o Airflow tem `catchup=True`. Isso significa que ao ativar uma DAG, ele cria execuções retroativas para **todas as datas** entre `start_date` e a data atual. Se seu `start_date` é 30 dias atrás com `schedule_interval="@daily"`, o Airflow agenda 30 runs imediatamente.

**Solução:**
```python
# ✅ Desabilitar catchup na definição da DAG:
dag = DAG(
    dag_id='etl_vendas_diarias',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,  # ← Não executar datas passadas!
)

# Ou com context manager:
with DAG(
    dag_id='etl_vendas_diarias',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    pass
```

```bash
# Se o catchup já disparou dezenas de runs, limpar:
# 1. Pausar a DAG primeiro (na UI ou via CLI):
docker exec airflow-scheduler airflow dags pause etl_vendas_diarias

# 2. Limpar todas as execuções indesejadas:
docker exec airflow-scheduler airflow dags delete etl_vendas_diarias --yes

# 3. Recriar com catchup=False e reativar
docker exec airflow-scheduler airflow dags unpause etl_vendas_diarias
```

```python
# Configuração global (para todas as DAGs) no airflow.cfg:
# [scheduler]
# catchup_by_default = False

# Ou via variável de ambiente no docker-compose.yml:
# AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT: 'False'


# Limitar execuções paralelas caso use catchup intencionalmente:
dag = DAG(
    dag_id='reprocessamento_historico',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=True,  # Intencional para reprocessar histórico
    max_active_runs=3,  # ← Máximo 3 runs simultâneos
    concurrency=6,      # ← Máximo 6 tasks simultâneas na DAG
)
```

**Prevenção:**
- **Sempre** defina `catchup=False` em DAGs de laboratório e desenvolvimento
- Use `start_date` como data recente (ontem ou hoje) durante desenvolvimento
- Nunca use `start_date=datetime.now()` — isso muda a cada parse e causa comportamento errático
- Se precisar de catchup intencional, use `max_active_runs` para limitar a concorrência

---
## 8. BashOperator Retorna "Command Not Found"

**Sintoma:**
```
# Log da task mostra:
[2024-01-15 06:00:05] INFO - Running command: ['bash', '-c', 'spark-submit /opt/airflow/dags/scripts/etl.py']
[2024-01-15 06:00:05] ERROR - bash: spark-submit: command not found
[2024-01-15 06:00:05] ERROR - Subtask failed with return code 127
```

Ou:
```
bash: jq: command not found
bash: aws: command not found
bash: psql: command not found
```

**Causa:**
1. **Comando não instalado no container do Airflow** — O container do scheduler/worker é minimalista e não tem todos os binários
2. **PATH não inclui o diretório do comando** — O binário existe mas não está no `$PATH` do bash
3. **Script sem permissão de execução** — O arquivo `.sh` não tem flag executável
4. **Bash não encontra o script** — Caminho relativo não resolve corretamente dentro do container

**Solução:**
```bash
# 1. Verificar se o comando existe no container:
docker exec airflow-scheduler which spark-submit
docker exec airflow-scheduler echo $PATH

# 2. Instalar comandos faltantes (temporário — para testes):
docker exec airflow-scheduler apt-get update && apt-get install -y jq

# 3. Para instalação permanente, adicionar no Dockerfile ou docker-compose:
# No docker-compose.yml, usar imagem customizada ou instalar via entrypoint
```

```python
# 4. Usar caminho absoluto no BashOperator:
task = BashOperator(
    task_id='run_spark',
    # ❌ ERRADO — depende do PATH:
    # bash_command='spark-submit etl.py',
    
    # ✅ CORRETO — caminho absoluto:
    bash_command='/opt/spark/bin/spark-submit /opt/airflow/dags/scripts/etl.py',
    dag=dag,
)

# 5. Definir PATH explicitamente via env:
task = BashOperator(
    task_id='run_spark',
    bash_command='spark-submit /opt/airflow/dags/scripts/etl.py',
    env={'PATH': '/opt/spark/bin:/usr/local/bin:/usr/bin:/bin'},
    dag=dag,
)

# 6. Para scripts customizados, usar caminho absoluto e garantir permissão:
task = BashOperator(
    task_id='run_script',
    bash_command='chmod +x /opt/airflow/dags/scripts/processar.sh && /opt/airflow/dags/scripts/processar.sh',
    dag=dag,
)

# 7. Alternativa: usar PythonOperator com subprocess para mais controle:
import subprocess

def executar_comando(**context):
    result = subprocess.run(
        ['spark-submit', '/opt/airflow/dags/scripts/etl.py'],
        capture_output=True,
        text=True,
        env={**os.environ, 'SPARK_HOME': '/opt/spark'}
    )
    if result.returncode != 0:
        raise Exception(f"Comando falhou: {result.stderr}")
    print(result.stdout)

task = PythonOperator(
    task_id='run_spark',
    python_callable=executar_comando,
    dag=dag,
)
```

**Prevenção:**
- Sempre use caminhos absolutos em `bash_command` — nunca confie no PATH do container
- Monte scripts auxiliares como volumes no docker-compose ao invés de copiar para a imagem
- Teste comandos manualmente no container antes de usar na DAG: `docker exec airflow-scheduler <comando>`
- Para integrações complexas (Spark, AWS CLI), considere usar Operators específicos ao invés de BashOperator

---
## Quick Reference: Tabela de Diagnóstico Rápido

| Sintoma | Causa Provável | Solução Rápida |
|---------|---------------|----------------|
| DAG não aparece na UI | Arquivo fora de `dags/` ou sem objeto DAG | Verificar path e executar `python my_dag.py` |
| "DAG Import Error" na UI | Erro de sintaxe ou import quebrado | `airflow dags list-import-errors` |
| "ModuleNotFoundError" | Módulo não instalado ou `__init__.py` faltando | Instalar provider ou criar `__init__.py` |
| Import path antigo (Airflow 1.x) | Migração de versão | Usar `from airflow.operators.python import ...` |
| Scheduler "not running" | Container crashou ou DB travou | `docker compose restart airflow-scheduler` |
| Task presa em "queued" | Executor limitado ou pool cheio | Verificar executor e limites de pool |
| XCom retorna `None` | `task_ids` errado ou sem `return` | Conferir nome exato da task e usar return |
| `{{ ds }}` aparece literal | Campo não-templated ou context direto | Usar `context['ds']` no PythonOperator |
| Dezenas de runs ao ativar DAG | `catchup=True` com start_date antigo | Adicionar `catchup=False` na DAG |
| "command not found" no Bash | Binário não existe no container | Usar caminho absoluto ou instalar pacote |
| Task falha com exit code 127 | Comando não encontrado no PATH | Verificar com `docker exec ... which <cmd>` |
| DAG não executa no horário | Scheduler parado ou timezone errado | Verificar heartbeat e timezone config |

---

## Fluxo de Diagnóstico: Árvore de Decisão

```
DAG não aparece na UI?
├── Arquivo está em dags/? → Problema 1 (path errado)
├── Tem objeto DAG no escopo global? → Problema 1 (parser não encontra DAG)
├── airflow dags list-import-errors mostra erro? → Problema 2 (Import Error)
└── Scheduler está rodando? → Problema 3 (Scheduler parado)

Task não executa?
├── Status "queued" por muito tempo → Problema 4 (Executor/Pool)
├── Status "upstream_failed" → Dependência falhou (verificar task anterior)
├── Status "no_status" → DAG pausada ou scheduler parado → Problema 3
└── Executa mas falha → Ver log da task para erro específico

Task executa mas resultado errado?
├── XCom retorna None → Problema 5 (task_id/key errado)
├── Template aparece como literal → Problema 6 (campo não-templated)
├── BashOperator: "command not found" → Problema 8 (PATH/instalação)
└── Múltiplas execuções inesperadas → Problema 7 (catchup)
```

---

## Comandos Úteis para Debug

```bash
# === Status Geral ===
docker compose ps                              # Status de todos os containers
docker compose logs airflow-scheduler --tail=30  # Logs recentes do scheduler
docker compose logs airflow-webserver --tail=30  # Logs recentes da UI

# === DAGs ===
docker exec airflow-scheduler airflow dags list               # Listar todas as DAGs
docker exec airflow-scheduler airflow dags list-import-errors  # Ver erros de import
docker exec airflow-scheduler airflow dags show etl_vendas     # Ver estrutura da DAG
docker exec airflow-scheduler airflow dags reserialize         # Forçar re-parse

# === Tasks ===
docker exec airflow-scheduler airflow tasks list etl_vendas          # Listar tasks da DAG
docker exec airflow-scheduler airflow tasks test etl_vendas extract 2024-01-01  # Testar task isolada
docker exec airflow-scheduler airflow tasks render etl_vendas extract 2024-01-01  # Ver templates renderizados
docker exec airflow-scheduler airflow tasks clear etl_vendas -t extract -s 2024-01-01 -e 2024-01-01 --yes  # Limpar e re-executar

# === Configuração ===
docker exec airflow-scheduler airflow config get-value core executor       # Ver executor
docker exec airflow-scheduler airflow config get-value core dags_folder    # Ver pasta de DAGs
docker exec airflow-scheduler airflow version                               # Ver versão do Airflow
docker exec airflow-scheduler airflow providers list                        # Ver providers instalados
```

### URLs Importantes

| Serviço | URL | Função |
|---------|-----|--------|
| Airflow Webserver | http://localhost:8080 | Interface principal — DAGs, logs, XComs |
| Flower (Celery) | http://localhost:5555 | Monitorar workers (se usar CeleryExecutor) |

---

> **Marina:** "O Airflow tem uma curva de aprendizado nos primeiros dias, mas a boa notícia é que 90% dos problemas se concentram nessas 8 categorias. Minha dica principal: sempre use `airflow tasks test` para testar tasks isoladamente antes de ativar a DAG completa. E quando algo der errado, o primeiro lugar para olhar são os logs da task na UI — clique na task, depois em 'Log'. Quase sempre a resposta está ali."

