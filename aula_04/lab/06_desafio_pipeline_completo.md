# Exercício 6 — Desafio: Pipeline Diário Completo com 6+ Tasks

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, chega de pipelines improvisados. Precisamos de uma DAG definitiva que rode todo dia às 6h da manhã sem ninguém precisar tocar. Ela tem que extrair dados dos 3 parceiros em paralelo, unificar tudo, validar qualidade, carregar no data lake particionado por data, e me notificar quando terminar. Se falhar, quero saber imediatamente — e quero que tente sozinha antes de me acordar."

> **Carlos Mendes (Eng. Sênior):** "Entendido, Marina. Vou combinar tudo que construímos essa semana: scheduling com cron, templates para particionamento dinâmico, extração paralela com XComs, retry com exponential backoff, callbacks de falha, e BashOperator para notificação. Será o pipeline mais robusto que a DataFlow já teve — no mínimo 8 tasks, todas orquestradas corretamente."

## Objetivos

Ao final deste exercício, você terá demonstrado domínio de **todos** os conceitos de Airflow aprendidos nesta aula, combinando-os em uma DAG de produção completa:

- Scheduling com expressão cron e `catchup=False`
- `default_args` com retry, retry_delay e callback de falha
- Extração paralela (fan-out) com `PythonOperator` e XComs
- Unificação (fan-in) puxando XComs de múltiplas tasks
- Validação de qualidade com resultado em XCom
- Carregamento com path dinâmico usando `{{ ds }}`
- Notificação via `BashOperator` com template variables
- `execution_timeout` para proteção contra travamento
- Dependências complexas: paralelo → sequencial

## Pré-requisitos

- Exercícios 1 a 5 concluídos
- Ambiente Airflow rodando (ver `00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Domínio de: PythonOperator, BashOperator, XComs, schedule_interval, templates, retry, callbacks

## Duração Estimada

⏱️ ~20 minutos

---

## O Desafio

Crie o arquivo `dag_pipeline_completo.py` na pasta de DAGs do Airflow. Esta DAG representa o **pipeline diário definitivo** da DataFlow Analytics — o mesmo que Carlos executa manualmente toda manhã e que agora será 100% automatizado.

### Estrutura Esperada (Graph View)

```
                    ┌─────────────────────┐
                    │     start_task      │
                    │   (início visual)   │
                    └────────┬────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
   ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
   │ extract_a    │ │ extract_b    │ │ extract_c    │
   │ (Parceiro A) │ │ (Parceiro B) │ │ (Parceiro C) │
   └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
          │                │                │
          └────────────────┼────────────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   unify_data        │
                │ (consolida XComs)   │
                └─────────┬───────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │   validate_quality  │
                │ (checks de QA)      │
                └─────────┬───────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │   load_datalake     │
                │ (path com {{ ds }}) │
                └─────────┬───────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │   notify_completion │
                │ (BashOperator)      │
                └─────────┬───────────┘
                          │
                          ▼
                ┌─────────────────────┐
                │     end_task        │
                │   (fim visual)      │
                └─────────────────────┘
```

**Total mínimo: 8 tasks** (start, 3 extrações, unify, validate, load, notify — bônus: end_task = 9)

### Dependências Obrigatórias

```python
start >> [extract_a, extract_b, extract_c] >> unify >> validate >> load >> notify >> end
```

---

## Requisitos Técnicos

Sua DAG **deve** implementar todos os itens abaixo:

| # | Requisito | Conceito Exercitado |
|---|-----------|-------------------|
| 1 | `schedule_interval="0 6 * * *"` com `catchup=False` | Scheduling (Ex. 4) |
| 2 | `default_args` com `retries`, `retry_delay`, `on_failure_callback` | Error handling (Ex. 5) |
| 3 | 3 tasks de extração em **paralelo** (fan-out) usando `PythonOperator` | Paralelismo + PythonOperator (Ex. 1) |
| 4 | Cada extração pusha quantidade de registros via XCom | XComs (Ex. 2) |
| 5 | Task `unify_data` faz `xcom_pull` das 3 extrações (fan-in) | XComs multi-task (Ex. 2) |
| 6 | Task `validate_quality` verifica dados e retorna contagem válidos/inválidos | PythonOperator + XCom (Ex. 1-2) |
| 7 | Task `load_datalake` usa `{{ ds }}` no path de saída | Templates (Ex. 3-4) |
| 8 | Task `notify_completion` é `BashOperator` com template variables | BashOperator + Templates (Ex. 3) |
| 9 | Pelo menos **1 task** com `execution_timeout` | Timeout (Ex. 5) |
| 10 | Cadeia de dependências correta: paralelo → sequencial | Dependências (Ex. 1-2) |

### Bônus (opcional)

| # | Item Bônus | Descrição |
|---|-----------|-----------|
| B1 | `start_task` e `end_task` | `BashOperator` com echo simples para clareza visual no Graph View |
| B2 | `retry_exponential_backoff` | Em pelo menos uma task de extração |
| B3 | `on_success_callback` | Na task `load_datalake` para registrar métricas |
| B4 | Tags na DAG | `tags=["dataflow", "producao", "diario"]` |

---

## Critérios de Validação

Para considerar o desafio **completo**, sua DAG deve atender a todos os critérios obrigatórios:

| # | Critério | Como Verificar |
|---|----------|----------------|
| 1 | DAG aparece na UI sem erros de parsing | Lista de DAGs em http://localhost:8081 |
| 2 | Graph View mostra 8+ tasks com layout correto | Aba "Graph" na DAG |
| 3 | Trigger manual executa com sucesso (todas as tasks verdes) | Botão "Trigger DAG" |
| 4 | Extrações A, B, C rodam em **paralelo** (mesmo start time ±1s) | Aba "Gantt" na DAG |
| 5 | XComs estão preenchidos após execução (3 extrações + unify + validate) | Admin → XComs na UI |
| 6 | Task `load_datalake` usa data de execução no path (visível nos logs) | Logs da task |
| 7 | Task `notify_completion` exibe mensagem com data e contagens | Logs da task |
| 8 | `execution_timeout` configurado em pelo menos 1 task | Código da DAG |
| 9 | `on_failure_callback` definido (em default_args ou per-task) | Código da DAG |
| 10 | `schedule_interval` é `"0 6 * * *"` e `catchup=False` | Código da DAG |

### Auto-avaliação de Qualidade

| Aspecto | 🟢 Excelente | 🟡 Bom | 🔴 Revisar |
|---------|-------------|---------|-----------|
| Tasks | 9+ tasks | 8 tasks | <8 tasks |
| XComs | Todas as tasks passam dados | Apenas extrações usam XCom | Sem XCom |
| Templates | `{{ ds }}` em load + notify | `{{ ds }}` em 1 task | Hardcoded |
| Resiliência | retry + timeout + callback | retry + callback | Sem retry |
| Parallelism | 3 extrações paralelas verificadas no Gantt | Configurado mas não verificado | Tasks sequenciais |

---

## Dicas

<details>
<summary>💡 Dica 1: Estrutura básica da DAG</summary>

Comece pelo esqueleto: `DAG()` com os parâmetros obrigatórios, `default_args` completo, e as tasks vazias (apenas print) com as dependências corretas. Só depois preencha a lógica de cada task.

```python
# Esqueleto — defina a DAG e as dependências PRIMEIRO
with DAG(dag_id="...", ...) as dag:
    start = BashOperator(task_id="start_task", bash_command="echo 'Início'")
    # ... definir todas as tasks ...
    end = BashOperator(task_id="end_task", bash_command="echo 'Fim'")
    
    start >> [extract_a, extract_b, extract_c] >> unify >> validate >> load >> notify >> end
```

</details>

<details>
<summary>💡 Dica 2: Fan-out / Fan-in com XComs</summary>

Para o fan-in (unificação), use `xcom_pull` especificando `task_ids` como lista:

```python
def unificar_dados(**context):
    ti = context["ti"]
    registros_a = ti.xcom_pull(task_ids="extract_a", key="registros")
    registros_b = ti.xcom_pull(task_ids="extract_b", key="registros")
    registros_c = ti.xcom_pull(task_ids="extract_c", key="registros")
    total = registros_a + registros_b + registros_c
    # ...
```

Cada extração deve fazer `xcom_push` com a mesma key para consistência.

</details>

<details>
<summary>💡 Dica 3: Template no BashOperator de notificação</summary>

O `BashOperator` suporta Jinja templates diretamente no `bash_command`. Você pode combinar variáveis do Airflow com XComs puxados via template:

```python
notify = BashOperator(
    task_id="notify_completion",
    bash_command='echo "Pipeline {{ ds }} concluído. Registros: {{ ti.xcom_pull(task_ids=\'load_datalake\', key=\'total\') }}"',
)
```

Alternativamente, use uma string mais simples e formate apenas com `{{ ds }}`.

</details>

<details>
<summary>💡 Dica 4: execution_timeout e retry combinados</summary>

O `execution_timeout` é um `timedelta`. Se a task estourar o timeout, ela falha e entra no fluxo de retry normalmente. Coloque na task mais propensa a travar (geralmente a extração de dados externos):

```python
from datetime import timedelta

extract_a = PythonOperator(
    task_id="extract_a",
    python_callable=extrair_parceiro_a,
    execution_timeout=timedelta(minutes=5),
    retries=3,
    retry_delay=timedelta(seconds=30),
)
```

</details>

---

## Como Testar sua DAG

### Passo 1: Salvar e verificar parsing

```bash
docker exec airflow-scheduler python /opt/airflow/dags/dag_pipeline_completo.py
```

Se não houver output = sem erros de sintaxe.

### Passo 2: Listar tasks

```bash
docker exec airflow-scheduler airflow tasks list dag_pipeline_completo --tree
```

Deve mostrar a hierarquia com 8+ tasks.

### Passo 3: Trigger manual

Na UI (http://localhost:8081):
1. Encontre `dag_pipeline_completo` na lista
2. Ative o toggle (unpause)
3. Clique em "Trigger DAG" (botão play)
4. Acompanhe na **Graph View** e **Gantt View**

### Passo 4: Verificar paralelismo

Na aba **Gantt** da execução, confirme que `extract_a`, `extract_b` e `extract_c` iniciaram no mesmo momento (barras alinhadas horizontalmente).

### Passo 5: Verificar XComs

Na UI: Admin → XComs — filtre por `dag_id = dag_pipeline_completo` e confirme que as extrações, unificação e validação pusharam valores.

---

## Reflexão Final

> **Carlos Mendes:** "Esta DAG é a consolidação de tudo que aprendemos. Em produção na DataFlow, temos pipelines com 20, 30 tasks — mas a estrutura fundamental é a mesma: extração paralela, unificação, validação, carga, notificação. Se vocês dominam essa arquitetura com 8-9 tasks, escalar para 30 é questão de organização, não de complexidade conceitual. Na próxima aula, vamos elevar o nível: branching condicional, sensors que esperam arquivos aparecerem, TaskGroups para organizar visualmente, e integração direta com Spark via SparkSubmitOperator."

> **Marina Silva:** "Para mim, o mais importante é a **resiliência**. Um pipeline sem retry e sem alerta é um pipeline que vai te acordar às 3h da manhã. Com o que vocês construíram aqui — exponential backoff, execution_timeout, on_failure_callback — estamos prontos para dormir tranquilos. O Airflow cuida do resto."

---

## Referências

- [Airflow DAG Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [XComs — Cross-Communication](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Scheduling & Timetables](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/cron.html)
- [Error Handling & Retries](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#retries)
- [BashOperator Templating](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html)
