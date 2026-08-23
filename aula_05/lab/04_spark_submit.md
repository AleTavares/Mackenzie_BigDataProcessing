# Exercício 4 — Integração Airflow + Spark via SparkSubmitOperator

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, até agora nossos PythonOperators fazem o processamento pesado dentro do worker do Airflow. Mas quando o volume de dados cresce para milhões de registros, o worker não tem memória suficiente. Precisamos que o Airflow apenas **orquestre** — quem processa de verdade é o cluster Spark."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Faz todo sentido, Marina. O Airflow não deve ser o engine de processamento — ele é o **maestro da orquestra**. Vou usar o `SparkSubmitOperator` para submeter jobs PySpark ao cluster. O Airflow dispara, o Spark executa, e o Airflow monitora se deu certo ou não."

> **Marina Silva (CTO):** "Perfeito. Quero que o pipeline funcione assim: o Airflow espera o arquivo chegar (Sensor), submete o processamento ao Spark, e depois notifica a equipe. Três responsabilidades claras, cada componente fazendo o que faz melhor."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender a separação de responsabilidades: Airflow orquestra, Spark processa
- Criar um script PySpark independente para ser submetido ao cluster
- Configurar a conexão Spark no Airflow (`spark_default`)
- Usar `SparkSubmitOperator` para submeter jobs ao cluster Spark
- Passar parâmetros dinâmicos (`{{ ds }}`) ao job Spark via `application_args`
- Configurar recursos do Spark (`executor.memory`, `executor.cores`) via `conf`
- Verificar a execução do job na Spark UI
- Combinar Sensor + SparkSubmit + Notificação em um pipeline completo

## Pré-requisitos

- Exercícios 01 (Branching), 02 (FileSensor) e 03 (TaskGroups) concluídos
- Ambiente Docker com Airflow **e** Spark rodando (ver `aula_04/lab/00_setup.md`)
- Spark Master acessível em `spark://spark-master:7077`
- Spark UI acessível em http://localhost:8080
- Airflow UI acessível em http://localhost:8081

## Conceito: Por que SparkSubmitOperator?

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  SEPARAÇÃO DE RESPONSABILIDADES                                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  ❌ ANTI-PATTERN: Processar dados dentro do PythonOperator                   ║
║                                                                              ║
║     ┌───────────────────────────────────────────────┐                        ║
║     │  Airflow Worker (2GB RAM)                     │                        ║
║     │                                               │                        ║
║     │  PythonOperator:                              │                        ║
║     │    spark = SparkSession.builder.getOrCreate()  │ ← worker sobrecarrega ║
║     │    df = spark.read.parquet("1M_registros")    │                        ║
║     │    df.groupBy(...).agg(...)                   │ ← OOM!                 ║
║     │                                               │                        ║
║     └───────────────────────────────────────────────┘                        ║
║                                                                              ║
║  ✅ CORRETO: Airflow orquestra, Spark processa                               ║
║                                                                              ║
║     ┌──────────────────┐          ┌──────────────────────────────┐           ║
║     │  Airflow Worker   │  submit  │  Spark Cluster (16GB+)      │           ║
║     │  (leve, <512MB)  │─────────▶│                              │           ║
║     │                   │          │  spark-master:7077           │           ║
║     │  SparkSubmit      │  status  │    └── worker-1 (4GB)       │           ║
║     │  Operator         │◀─────────│    └── worker-2 (4GB)       │           ║
║     │                   │          │                              │           ║
║     └──────────────────┘          └──────────────────────────────┘           ║
║                                                                              ║
║  O worker do Airflow apenas ENVIA o job e MONITORA o status.                 ║
║  O processamento pesado acontece no cluster Spark.                           ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

| Abordagem | Memória do Worker | Escalabilidade | Quando usar |
|-----------|-------------------|----------------|-------------|
| `PythonOperator` com Spark local | Alta (processa tudo) | Limitada ao worker | Dados < 100MB |
| `SparkSubmitOperator` | Baixa (só orquestra) | Cluster inteiro | Dados > 100MB |

---

## Exercício 4.1: Criar o Script PySpark

### O que fazer

Crie um script PySpark **independente** que será submetido ao cluster Spark. O script deve:

1. Receber a data de execução como argumento CLI (`sys.argv[1]`)
2. Criar uma SparkSession conectando ao cluster (`spark-master:7077`)
3. Ler dados da partição correspondente à data recebida
4. Executar uma agregação (faturamento por estado)
5. Gravar o resultado particionado na camada gold
6. Imprimir métricas de execução (registros processados, tempo)

**Onde criar:** `aula_05/code/spark_jobs/processar_vendas.py`

### Dicas

1. O script é um Python puro — **não** é uma DAG Airflow. Ele roda no cluster Spark:
   ```python
   import sys
   from pyspark.sql import SparkSession

   # Argumento vindo do application_args do SparkSubmitOperator
   data_execucao = sys.argv[1]  # ex: "2024-01-15"
   ```

2. A SparkSession deve usar `.master("spark://spark-master:7077")` para conectar ao cluster:
   ```python
   spark = SparkSession.builder \
       .appName(f"DataFlow-Vendas-{data_execucao}") \
       .getOrCreate()
   # O master será definido pelo SparkSubmitOperator, não precisa hardcodar aqui
   ```

3. Construa paths dinâmicos com a data recebida:
   ```python
   path_entrada = f"/data/vendas_diarias/dt={data_execucao}/"
   path_saida = f"/data/gold/vendas_agregadas/dt={data_execucao}/"
   ```

4. O script deve ser **idempotente** — use `.mode("overwrite")` para a escrita

5. Inclua um `try/except` e `sys.exit(1)` em caso de erro para que o Airflow detecte a falha

### Critérios de Validação

- [ ] Script criado em `aula_05/code/spark_jobs/processar_vendas.py`
- [ ] Recebe data de execução via `sys.argv[1]`
- [ ] Cria SparkSession com appName dinâmico contendo a data
- [ ] Lê dados da partição correta baseada na data recebida
- [ ] Executa pelo menos uma agregação (groupBy + agg)
- [ ] Grava resultado em path particionado por data
- [ ] Usa `.mode("overwrite")` para idempotência
- [ ] Trata erros com `sys.exit(1)` para feedback ao Airflow

---

## Exercício 4.2: Configurar a Conexão Spark no Airflow

### O que fazer

Configure a conexão `spark_default` no Airflow para que o `SparkSubmitOperator` saiba onde submeter os jobs. Existem duas formas de fazer isso — escolha uma:

**Opção A — Via Airflow UI:**
- Acesse: http://localhost:8081 → Admin → Connections
- Crie uma nova Connection

**Opção B — Via CLI:**
- Use `airflow connections add` via terminal

### Dicas

1. Os parâmetros da conexão Spark:

   | Campo | Valor |
   |-------|-------|
   | Connection Id | `spark_default` |
   | Connection Type | `Spark` |
   | Host | `spark://spark-master` |
   | Port | `7077` |

2. Via CLI, o comando segue este padrão:
   ```bash
   docker exec airflow-scheduler airflow connections add \
       <conn_id> \
       --conn-type <type> \
       --conn-host <host> \
       --conn-port <port>
   ```

3. Para verificar se a conexão foi criada:
   ```bash
   docker exec airflow-scheduler airflow connections get spark_default
   ```

4. O `conn_id` é referenciado no `SparkSubmitOperator` — deve ser exatamente `spark_default`

5. Se a conexão já existir, use `--conn-uri` ou delete e recrie:
   ```bash
   docker exec airflow-scheduler airflow connections delete spark_default
   ```

### Critérios de Validação

- [ ] Conexão `spark_default` criada no Airflow
- [ ] Connection Type é `Spark`
- [ ] Host aponta para `spark://spark-master`
- [ ] Port é `7077`
- [ ] Comando `airflow connections get spark_default` retorna os dados corretos

---

## Exercício 4.3: Criar a DAG com SparkSubmitOperator

### O que fazer

Crie uma DAG que usa o `SparkSubmitOperator` para submeter o script PySpark ao cluster. A DAG deve:

1. Usar `SparkSubmitOperator` apontando para o script criado no exercício 4.1
2. Passar a data de execução (`{{ ds }}`) como argumento ao script
3. Configurar recursos do Spark (memória e cores dos executors)
4. Definir dependências adequadas

**Onde criar:** `aula_05/code/dags/dag_spark_submit.py`

### Dicas

1. Import necessário — o provider `apache-airflow-providers-apache-spark` deve estar instalado:
   ```python
   from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
   ```

2. Parâmetros essenciais do `SparkSubmitOperator`:

   | Parâmetro | Descrição | Exemplo |
   |-----------|-----------|---------|
   | `application` | Path do script PySpark no container | `"/opt/spark-jobs/processar_vendas.py"` |
   | `conn_id` | Conexão Spark configurada | `"spark_default"` |
   | `application_args` | Argumentos passados ao script | `["{{ ds }}"]` |
   | `conf` | Configurações do Spark | `{"spark.executor.memory": "2g"}` |
   | `name` | Nome do job na Spark UI | `"dataflow-vendas-{{ ds }}"` |

3. Configurações de recursos recomendadas para o lab:
   ```python
   conf = {
       "spark.executor.memory": "1g",
       "spark.executor.cores": "2",
       "spark.driver.memory": "1g",
   }
   ```

4. O `application_args` é uma **lista** — cada item vira um argumento posicional no `sys.argv`:
   ```python
   application_args=["{{ ds }}", "--mode", "full"]
   # No script: sys.argv[1] = "2024-01-15", sys.argv[2] = "--mode", sys.argv[3] = "full"
   ```

5. O `{{ ds }}` dentro do `application_args` é resolvido pelo Jinja no momento da execução — funciona com backfill!

6. Para adicionar pacotes externos (JARs), use o parâmetro `packages`:
   ```python
   packages="io.delta:delta-core_2.12:2.4.0"  # Exemplo com Delta Lake
   ```

### Critérios de Validação

- [ ] DAG criada em `aula_05/code/dags/dag_spark_submit.py`
- [ ] Usa `SparkSubmitOperator` com `conn_id="spark_default"`
- [ ] `application` aponta para o script PySpark correto
- [ ] `application_args` inclui `["{{ ds }}"]`
- [ ] `conf` define pelo menos `spark.executor.memory`
- [ ] `name` do job inclui a data para identificação na Spark UI
- [ ] DAG aparece no Airflow UI sem erros de import

---

## Exercício 4.4: Combinar Sensor → SparkSubmit → Notificação

### O que fazer

Agora que você sabe usar o `SparkSubmitOperator` isoladamente, construa um pipeline completo que combina os conceitos dos exercícios anteriores:

```
┌─────────────────────┐     ┌────────────────────────┐     ┌────────────────────┐
│  FileSensor         │────▶│  SparkSubmitOperator   │────▶│  BashOperator      │
│                     │     │                        │     │                    │
│  Espera arquivo     │     │  Submete job ao        │     │  Notifica equipe   │
│  vendas_{{ ds }}    │     │  cluster Spark         │     │  (echo/log)        │
│  chegar             │     │                        │     │                    │
└─────────────────────┘     └────────────────────────┘     └────────────────────┘
```

A DAG deve:
1. Usar `FileSensor` para aguardar a chegada do arquivo de vendas do dia
2. Submeter o processamento ao Spark via `SparkSubmitOperator`
3. Notificar a equipe com um `BashOperator` informando o resultado

### Dicas

1. Reutilize o que aprendeu no **Exercício 02** (FileSensor):
   ```python
   filepath = "/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.csv"
   ```

2. Combine os parâmetros do `FileSensor` com o `SparkSubmitOperator`:
   - Sensor com `timeout=3600` e `poke_interval=60`
   - SparkSubmit com o script e `application_args=["{{ ds }}"]`

3. A notificação final pode usar template variables no `BashOperator`:
   ```python
   bash_command='echo "✅ Job Spark concluído para {{ ds }}. Verifique: http://localhost:8080"'
   ```

4. Dependências lineares: `sensor >> spark_job >> notificacao`

5. Considere adicionar `retries=2` no `SparkSubmitOperator` — jobs Spark podem falhar por recursos temporariamente indisponíveis

6. **Bônus:** adicione um `on_failure_callback` no `SparkSubmitOperator` para logar quando o job falha:
   ```python
   def alerta_falha_spark(context):
       task_id = context["task_instance"].task_id
       ds = context["ds"]
       print(f"🚨 ALERTA: Job Spark falhou! Task: {task_id}, Data: {ds}")
   ```

### Critérios de Validação

- [ ] Pipeline com 3 tasks: Sensor → SparkSubmit → Notificação
- [ ] `FileSensor` espera arquivo com path dinâmico (`{{ ds_nodash }}`)
- [ ] `SparkSubmitOperator` submete o job ao cluster
- [ ] Dependências definidas corretamente (linear)
- [ ] `BashOperator` final notifica com a data de execução
- [ ] DAG funciona para qualquer data (sem valores hardcoded)

---

## Exercício 4.5: Executar e Verificar na Spark UI

### O que fazer

Execute a DAG e verifique na **Spark UI** (http://localhost:8080) que o job foi de fato submetido ao cluster. Isso confirma que o Airflow está apenas orquestrando — o processamento real aconteceu no Spark.

### Passos

1. **Criar o arquivo de dados** para o sensor encontrar (simule a chegada):
   ```bash
   docker exec airflow-scheduler bash -c \
       "echo 'order_id,amount,date' > /opt/airflow/data/incoming/vendas_20240115.csv"
   ```

2. **Acionar a DAG** no Airflow UI ou via CLI:
   ```bash
   docker exec airflow-scheduler airflow dags trigger \
       <sua_dag_id> --exec-date "2024-01-15"
   ```

3. **Observar no Airflow UI:**
   - Sensor deve ficar em `running` (amarelo) até encontrar o arquivo
   - SparkSubmit deve ficar em `running` enquanto o job executa
   - Notificação em `success` (verde) ao final

4. **Verificar na Spark UI** (http://localhost:8080):
   - Acesse a aba "Completed Applications"
   - Procure pelo nome do job (que você definiu com `name=`)
   - Clique para ver detalhes: duração, stages, tasks executadas

### Dicas

1. Se o job não aparecer na Spark UI, verifique:
   - A conexão `spark_default` está com host e porta corretos?
   - O container do Spark Master está rodando? (`docker ps | grep spark`)
   - O path do script está correto dentro do container?

2. Na Spark UI, observe:
   - **Duration**: quanto tempo o job levou
   - **Stages**: quantos stages o Spark criou (cada shuffle gera um stage)
   - **Tasks**: quantas tasks paralelas foram executadas

3. Compare o tempo no Spark UI com o tempo da task no Airflow — a diferença é o overhead de submissão (~5-10 segundos)

4. Se o job falhar, os logs aparecerão tanto no Airflow (aba "Log" da task) quanto na Spark UI (aba "stderr")

### Critérios de Validação

- [ ] DAG executada com sucesso (todas as tasks verdes no Airflow)
- [ ] Job aparece na Spark UI em "Completed Applications"
- [ ] Nome do job na Spark UI contém a data de execução
- [ ] Você identificou a duração e número de stages do job
- [ ] Logs do SparkSubmitOperator no Airflow mostram a submissão

---

## Resumo e Conceitos-Chave

```
╔══════════════════════════════════════════════════════════════════════╗
║   INTEGRAÇÃO AIRFLOW + SPARK: SEPARAÇÃO DE RESPONSABILIDADES         ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  🎼 Airflow = Maestro (orquestra, agenda, monitora)                  ║
║  🎵 Spark   = Músicos (processam dados pesados)                      ║
║                                                                      ║
║  ┌─────────────────────────────────────────────────────────────┐     ║
║  │  SparkSubmitOperator                                        │     ║
║  │                                                             │     ║
║  │  application      = "/path/script.py"   (o que executar)    │     ║
║  │  conn_id          = "spark_default"     (onde submeter)     │     ║
║  │  application_args = ["{{ ds }}"]        (com quais dados)   │     ║
║  │  conf             = {"memory": "2g"}   (com quais recursos) │     ║
║  │                                                             │     ║
║  └─────────────────────────────────────────────────────────────┘     ║
║                                                                      ║
║  Pipeline completo:                                                  ║
║  FileSensor → SparkSubmitOperator → BashOperator (notificação)       ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Parâmetros do SparkSubmitOperator

| Parâmetro | O que faz | Exemplo |
|-----------|-----------|---------|
| `application` | Script PySpark a executar | `"/opt/spark-jobs/etl.py"` |
| `conn_id` | Conexão Spark no Airflow | `"spark_default"` |
| `application_args` | Argumentos para `sys.argv` | `["{{ ds }}", "--mode", "full"]` |
| `conf` | Spark configs (memória, cores) | `{"spark.executor.memory": "2g"}` |
| `name` | Nome visível na Spark UI | `"etl-vendas-{{ ds }}"` |
| `packages` | Dependências Maven (JARs) | `"io.delta:delta-core_2.12:2.4.0"` |
| `verbose` | Logs detalhados | `True` |

### Anti-patterns

| ❌ Errado | ✅ Correto | Por quê |
|-----------|-----------|---------|
| Processar dados no PythonOperator | Submeter ao Spark | Worker do Airflow não tem recursos |
| `datetime.now()` no script Spark | `sys.argv[1]` com `{{ ds }}` | Impossibilita backfill |
| Hardcodar master no script | Usar `conn_id` | Portabilidade entre ambientes |
| Ignorar erros no script | `sys.exit(1)` em caso de falha | Airflow precisa saber que falhou |

---

## ✅ Checklist de Conclusão

- [ ] Entendi por que separar orquestração (Airflow) de processamento (Spark)
- [ ] Criei um script PySpark independente que recebe parâmetros via `sys.argv`
- [ ] Configurei a conexão `spark_default` no Airflow
- [ ] Criei uma DAG com `SparkSubmitOperator` funcional
- [ ] Passei `{{ ds }}` como argumento dinâmico ao job Spark
- [ ] Configurei recursos do cluster via `conf`
- [ ] Combinei Sensor + SparkSubmit + Notificação em pipeline completo
- [ ] Verifiquei o job na Spark UI (http://localhost:8080)

---

## Próximo Exercício

➡️ **Exercício 5 — Callbacks e Alertas** (`05_callbacks_alertas.md`): configurar `on_failure_callback`, `on_success_callback` e alertas automáticos para que a equipe da DataFlow saiba imediatamente quando um pipeline falha — especialmente importante agora que jobs Spark podem falhar por falta de recursos no cluster.
