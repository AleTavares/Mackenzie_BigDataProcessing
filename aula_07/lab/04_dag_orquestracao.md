# Exercício 4 — Criar DAG Airflow que Orquestra o Spark Job via SparkSubmitOperator

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, o `pipeline_vendas.py` está pronto — CLI, logging estruturado, escrita idempotente. Agora preciso que ele rode sozinho, todo dia às 6h da manhã, sem ninguém clicar nada. E se falhar, quero saber imediatamente."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Perfeito. Vou criar a DAG de produção no Airflow. O fluxo será: primeiro um `FileSensor` confirma que o arquivo de vendas do dia chegou, depois o `SparkSubmitOperator` submete o `pipeline_vendas.py` ao cluster passando `{{ ds }}` como `--data-ref`, e por fim uma notificação confirma o sucesso. Se algo falhar, o `on_failure_callback` dispara alerta."

> **Marina Silva (CTO):** "E sobre retries? Na semana passada tivemos aquele timeout por pico de rede."

> **Carlos Mendes:** "Com a idempotência que implementamos no exercício anterior, retries são seguros. Posso configurar `retries=2` com `retry_delay=5min` sem risco de duplicar dados. Se falhar 3 vezes, aí sim dispara alerta crítico."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar uma DAG de produção com fluxo: Sensor → SparkSubmit → Notificação
- Configurar `SparkSubmitOperator` com `application_args` dinâmicos
- Usar `FileSensor` para esperar a chegada de dados antes de processar
- Configurar `default_args` com retries seguros (amparado pela idempotência)
- Implementar `on_failure_callback` para alertas automáticos
- Agendar a DAG com `schedule` e `catchup=False`

## Pré-requisitos

- Exercícios 01, 02 e 03 concluídos (`pipeline_vendas.py` com CLI, logging e idempotência)
- Aula 05 (Orquestração Avançada) — conceitos de FileSensor, SparkSubmitOperator e callbacks
- Ambiente Docker rodando (Spark + Airflow)
- Conexão `spark_default` configurada no Airflow (ver Aula 05, Exercício 04)

---

## O que você vai construir

Uma DAG de produção que orquestra o pipeline de vendas diário:

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DAG: dataflow_pipeline_vendas_producao                                       │
│  Schedule: @daily (06:00 UTC) | Catchup: False                               │
│                                                                               │
│  default_args:                                                                │
│    retries=2, retry_delay=5min (seguro — pipeline é idempotente)             │
│    on_failure_callback → alerta para equipe                                   │
│                                                                               │
│  ┌──────────────────┐    ┌──────────────────────────┐    ┌────────────────┐  │
│  │  FileSensor       │───▶│  SparkSubmitOperator     │───▶│  Notificação   │  │
│  │                   │    │                          │    │                │  │
│  │  Espera arquivo:  │    │  application:            │    │  Loga sucesso  │  │
│  │  incoming/        │    │    pipeline_vendas.py    │    │  com métricas  │  │
│  │  {{ ds }}/        │    │                          │    │                │  │
│  │  vendas.parquet   │    │  application_args:       │    │                │  │
│  │                   │    │    ["--data-ref","{{ ds }}"]   │                │  │
│  │  timeout: 2h      │    │                          │    │                │  │
│  │  poke: 5min       │    │  conn_id: spark_default  │    │                │  │
│  └──────────────────┘    └──────────────────────────┘    └────────────────┘  │
│                                                                               │
│  Em caso de falha: on_failure_callback → alerta_falha_pipeline()             │
└──────────────────────────────────────────────────────────────────────────────┘
```

**Por que essa arquitetura?**

| Componente | Responsabilidade | Justificativa |
|------------|------------------|---------------|
| `FileSensor` | Esperar dados chegarem | Não processar se o arquivo não existe |
| `SparkSubmitOperator` | Submeter job ao cluster | Airflow orquestra, Spark processa |
| Notificação | Confirmar sucesso | Equipe sabe que rodou sem olhar Airflow |
| `on_failure_callback` | Alertar em falha | Equipe acorda para resolver o problema |
| `retries=2` | Resiliência a falhas transientes | Seguro pois o pipeline é idempotente |

---

## Problema

Crie o arquivo `aula_07/code/dags/dag_pipeline_vendas.py` contendo uma DAG Airflow de produção que:

1. **Agendamento:** roda diariamente às 06:00 UTC com `catchup=False`
2. **Sensor:** usa `FileSensor` para esperar o arquivo `incoming/{{ ds }}/vendas.parquet`
3. **Spark Job:** usa `SparkSubmitOperator` para submeter `pipeline_vendas.py` passando `--data-ref` e `{{ ds }}` como argumentos
4. **Notificação:** uma task final que registra o sucesso do processamento
5. **Resiliência:** `default_args` com `retries=2` e `retry_delay=timedelta(minutes=5)`
6. **Alertas:** `on_failure_callback` que loga informações da falha (task, data, exceção)

---

## Dicas

### Dica 1: Estrutura básica da DAG

A DAG precisa de imports, default_args, callback, e a definição com `@daily` schedule:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
```

O `schedule` pode ser definido com cron para controlar o horário exato:
```python
schedule="0 6 * * *"  # Todo dia às 06:00 UTC
```

### Dica 2: Callback de falha

O callback recebe o `context` do Airflow com informações da execução:

```python
def alerta_falha_pipeline(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["ds"]
    exception = context.get("exception", "Desconhecida")
    # ... logar ou enviar alerta ...
```

Coloque esse callback no `default_args` para que se aplique a **todas** as tasks:
```python
default_args = {
    "on_failure_callback": alerta_falha_pipeline,
    ...
}
```

### Dica 3: FileSensor — Path e Timeout

O `FileSensor` precisa de uma `conn_id` do tipo `File (path)` ou pode usar `fs_default`. O filepath deve usar template Jinja:

```python
filepath="data/aula_07/producao/incoming/{{ ds }}/vendas.parquet"
```

Parâmetros importantes para produção:
- `timeout`: quanto tempo esperar no máximo (em segundos). Para 2 horas: `7200`
- `poke_interval`: a cada quantos segundos verificar. Para 5 minutos: `300`
- `mode`: use `"poke"` para intervalo fixo ou `"reschedule"` para liberar worker

### Dica 4: SparkSubmitOperator — application_args

O `pipeline_vendas.py` usa `argparse` com `--data-ref`. Os argumentos são passados como **lista**:

```python
application_args=["--data-ref", "{{ ds }}"]
```

Não esqueça do `conn_id="spark_default"` e do path da `application` dentro do container.

### Dica 5: Retries seguros

Como o `pipeline_vendas.py` usa `partitionOverwriteMode=dynamic` + `mode("overwrite")` + `partitionBy("data_ref")`, retries são **garantidamente seguros**:

```python
"retries": 2,
"retry_delay": timedelta(minutes=5),
```

Se o job falhar no meio e o Airflow fizer retry, o overwrite dinâmico garante que a partição do dia é simplesmente reescrita — sem duplicação.

### Dica 6: Notificação final

A task de notificação pode ser um `PythonOperator` ou `BashOperator` simples. Em produção real, seria um Slack webhook ou email. Para o lab, basta um log:

```python
def notificar_sucesso(**context):
    ds = context["ds"]
    print(f"✅ Pipeline de vendas concluído com sucesso para {ds}")
```

---

## Critérios de Validação

Verifique se sua DAG atende a **todos** os critérios abaixo:

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | Arquivo criado em `aula_07/code/dags/dag_pipeline_vendas.py` | `ls aula_07/code/dags/` |
| 2 | DAG tem `schedule="0 6 * * *"` (diário às 06:00) | Inspecionar código |
| 3 | `catchup=False` definido | Inspecionar parâmetro da DAG |
| 4 | `default_args` com `retries=2` e `retry_delay=timedelta(minutes=5)` | Inspecionar dict |
| 5 | `on_failure_callback` definido no `default_args` | Callback logando task_id, dag_id, ds e exception |
| 6 | `FileSensor` com path dinâmico usando `{{ ds }}` | Template Jinja no `filepath` |
| 7 | `FileSensor` com `timeout` e `poke_interval` configurados | Valores razoáveis (timeout >= 1h) |
| 8 | `SparkSubmitOperator` com `conn_id="spark_default"` | Inspecionar parâmetro |
| 9 | `SparkSubmitOperator` com `application` apontando para `pipeline_vendas.py` | Path correto no container |
| 10 | `application_args=["--data-ref", "{{ ds }}"]` | Passa data dinâmica ao script |
| 11 | Task de notificação que loga sucesso com a data | PythonOperator ou BashOperator |
| 12 | Dependências: sensor >> spark_job >> notificação | Fluxo linear correto |
| 13 | DAG não tem erros de import | `python aula_07/code/dags/dag_pipeline_vendas.py` sem erro |
| 14 | Sem valores hardcoded de data (tudo via `{{ ds }}`) | Funciona para qualquer data/backfill |

---

## Teste sua DAG

Após criar o arquivo, valide com os seguintes comandos:

**1. Verificar sintaxe Python:**
```bash
python aula_07/code/dags/dag_pipeline_vendas.py
```
Se não houver output, o arquivo é válido sintaticamente.

**2. Verificar no Airflow (se o ambiente estiver rodando):**
```bash
docker exec airflow-scheduler airflow dags list | grep pipeline_vendas
```

**3. Testar rendering dos templates:**
```bash
docker exec airflow-scheduler airflow tasks render \
    dataflow_pipeline_vendas_producao spark_submit_vendas 2024-01-15
```

Isso mostra como os `{{ ds }}` serão resolvidos para a data 2024-01-15.

---

## Conceitos Consolidados

Este exercício integra conceitos de **três aulas anteriores**:

| Conceito | Aula de Origem | Aplicação Aqui |
|----------|----------------|----------------|
| SparkSubmitOperator | Aula 05 - Exercício 04 | Submeter `pipeline_vendas.py` ao cluster |
| FileSensor | Aula 05 - Exercício 02 | Esperar arquivo de vendas do dia |
| on_failure_callback | Aula 05 - Exercício 05 | Alertar equipe em caso de falha |
| Template `{{ ds }}` | Aula 04 - Exercício 04 | Parametrizar data de processamento |
| retries + idempotência | Aula 07 - Exercício 03 | Retries seguros sem duplicação |
| argparse + CLI | Aula 07 - Exercício 01 | `--data-ref` via `application_args` |

---

## Próximo Passo

No **Exercício 05**, vamos adicionar **checks de qualidade de dados** como tasks dentro desta DAG, criando um pipeline com validação automática antes de promover dados para a camada Gold — integrando o `DataQualityFramework` da Aula 06 na orquestração de produção.
