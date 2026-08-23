# Exercício 6 — Desafio: Pipeline Completo com Sensor + Branching + Spark + TaskGroups

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, você dominou cada peça individualmente: sensors, branching, TaskGroups, SparkSubmit e callbacks. Agora quero o pipeline definitivo da DataFlow Analytics — um DAG de produção que combina TUDO. Esse é o tipo de pipeline que roda em empresas sérias. Dois sensors esperando os arquivos dos parceiros, decisão inteligente de volume, processamento organizado em grupos, e alertas para qualquer problema. Se você conseguir montar isso do zero, está pronto para liderar a engenharia de dados da empresa."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Aceito o desafio! Vou precisar de tudo que aprendemos hoje: `FileSensor` para monitorar a chegada dos dados, `BranchPythonOperator` para decidir o caminho com base no volume, `TaskGroup` para organizar as fases em blocos colapsáveis, `SparkSubmitOperator` (simulado) para o caminho de alto volume, callbacks para alerta e monitoramento, `trigger_rule` para convergência após o branch, e template variables `{{ ds }}` em todo lugar. O pipeline completo da DataFlow, totalmente automatizado."

## Objetivos

Neste desafio, você vai combinar **todos** os conceitos avançados de Airflow aprendidos nos exercícios 1-5 em uma única DAG de produção:

| # | Conceito | Exercício de Origem |
|---|----------|-------------------|
| 1 | `FileSensor` — esperar chegada de arquivos | Exercício 2 |
| 2 | `BranchPythonOperator` — decisão baseada em volume | Exercício 1 |
| 3 | `TaskGroup` — organização em blocos visuais | Exercício 3 |
| 4 | `SparkSubmitOperator` (simulado) — processamento pesado | Exercício 4 |
| 5 | `on_failure_callback` / `on_success_callback` | Exercício 5 |
| 6 | `trigger_rule` — convergência após branch | Exercício 1 |
| 7 | Template variables `{{ ds }}` — paths dinâmicos | Aula 04 |

## Pré-requisitos

- Exercícios 01 a 05 desta aula concluídos
- Ambiente Airflow + Spark rodando (ver `aula_04/lab/00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Domínio completo de: FileSensor, BranchPythonOperator, TaskGroup, SparkSubmitOperator, callbacks, trigger_rule

---

## O Desafio

Crie o arquivo `dag_pipeline_avancado_completo.py` na pasta de DAGs do Airflow. Esta DAG representa o **pipeline de produção definitivo** da DataFlow — combinando TODOS os padrões avançados de orquestração em uma única DAG coesa.

### Estrutura Esperada (Graph View)

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ DAG: dataflow_pipeline_avancado_v1                                                           │
│                                                                                              │
│  ┌─── grupo_sensores ─────────────┐                                                         │
│  │                                 │                                                         │
│  │  sensor_parceiro_a (FileSensor) │                                                         │
│  │  sensor_parceiro_b (FileSensor) │                                                         │
│  │                                 │                                                         │
│  └──────────────┬──────────────────┘                                                         │
│                 │                                                                             │
│                 ▼                                                                             │
│  ┌──────────────────────────┐                                                                │
│  │  contar_registros_total  │                                                                │
│  └──────────────┬───────────┘                                                                │
│                 │                                                                             │
│                 ▼                                                                             │
│  ┌──────────────────────────┐                                                                │
│  │  decidir_processamento   │  (BranchPythonOperator)                                        │
│  └────────┬─────────┬───────┘                                                                │
│           │         │                                                                        │
│           ▼         ▼                                                                        │
│  ┌─── grupo_spark ───────┐    ┌─── grupo_python ────────┐                                   │
│  │                        │    │                          │                                   │
│  │  spark_ingestao        │    │  python_ingestao         │                                   │
│  │  spark_transformacao   │    │  python_transformacao    │                                   │
│  │  spark_persistencia    │    │  python_persistencia     │                                   │
│  │                        │    │                          │                                   │
│  └───────────┬────────────┘    └────────────┬─────────────┘                                  │
│              │                               │                                               │
│              └───────────────┬───────────────┘                                               │
│                              │                                                               │
│                              ▼  (trigger_rule: none_failed_min_one_success)                   │
│  ┌─── grupo_gold ─────────────────────────┐                                                  │
│  │                                         │                                                  │
│  │  agregar_metricas                       │                                                  │
│  │  gerar_relatorio_{{ ds }}               │                                                  │
│  │                                         │                                                  │
│  └──────────────┬──────────────────────────┘                                                  │
│                 │                                                                             │
│                 ▼                                                                             │
│  ┌──────────────────────────┐                                                                │
│  │  notificar_equipe        │  (on_success_callback)                                         │
│  └──────────────────────────┘                                                                │
│                                                                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Fluxo Resumido (dependências)

```python
grupo_sensores >> contar_registros_total >> decidir >> [grupo_spark, grupo_python] >> grupo_gold >> notificar
```

---

## Requisitos Técnicos

Sua DAG **deve** implementar todos os itens abaixo:

| # | Requisito | Conceito | Exercício |
|---|-----------|----------|-----------|
| 1 | 2 `FileSensor` aguardando arquivos dos parceiros A e B | Sensor | Ex. 2 |
| 2 | Sensors dentro de um `TaskGroup` chamado `"sensores"` | TaskGroup | Ex. 3 |
| 3 | Task `contar_registros_total` que soma registros dos dois arquivos | PythonOperator | Ex. 1 |
| 4 | `BranchPythonOperator` que decide entre `grupo_spark` e `grupo_python` | Branching | Ex. 1 |
| 5 | Limiar de decisão: volume > 1.000.000 → Spark; caso contrário → Python | Regra de negócio | Ex. 1 |
| 6 | `TaskGroup "processamento_spark"` com 3 tasks internas (ingestão, transformação, persistência) | TaskGroup + Spark | Ex. 3-4 |
| 7 | `TaskGroup "processamento_python"` com 3 tasks internas (ingestão, transformação, persistência) | TaskGroup | Ex. 3 |
| 8 | Path do `SparkSubmitOperator` (ou PythonOperator simulado) usando `{{ ds }}` | Templates | Aula 04 |
| 9 | `TaskGroup "gold"` com aggregação e relatório usando `trigger_rule="none_failed_min_one_success"` | TaskGroup + trigger_rule | Ex. 1, 3 |
| 10 | Task final `notificar_equipe` com `on_success_callback` | Callback | Ex. 5 |
| 11 | `on_failure_callback` no `default_args` (todas as tasks alertam se falharem) | Callback | Ex. 5 |
| 12 | `{{ ds }}` utilizado em pelo menos 3 locais (sensor filepath, processamento, relatório) | Templates | Aula 04 |
| 13 | `schedule_interval="0 6 * * *"` com `catchup=False` | Scheduling | Aula 04 |
| 14 | Pelo menos **10 tasks** no total | Complexidade | — |

### Bônus (opcional)

| # | Item Bônus | Descrição |
|---|-----------|-----------|
| B1 | `poke_interval=30` e `timeout=300` nos sensors | Configuração realista de sensor |
| B2 | `on_retry_callback` em uma das tasks | Monitoramento de instabilidade |
| B3 | `execution_timeout` no grupo Spark | Proteção contra travamento |
| B4 | `tags=["dataflow", "producao", "avancado", "aula05"]` | Organização na UI |
| B5 | Segundo branch: `decidir` retorna **lista** para ativar Spark + notificação extra | Branch multi-caminho |

---

## Critérios de Validação

Para considerar o desafio **completo**, sua DAG deve atender a todos os critérios obrigatórios:

| # | Critério | Como Verificar |
|---|----------|----------------|
| 1 | DAG aparece na UI sem erros de parsing | Lista de DAGs em http://localhost:8081 |
| 2 | Graph View mostra 10+ tasks organizadas em 4 grupos | Aba "Graph" — expandir/colapsar grupos |
| 3 | Os 2 sensors estão dentro do grupo `sensores` | Graph View colapsada |
| 4 | Branch leva a exatamente 2 grupos alternativos | Graph View — dois caminhos divergentes |
| 5 | Grupo `gold` executa após qualquer caminho (trigger_rule correto) | Trigger manual com ambos os caminhos |
| 6 | `on_failure_callback` dispara quando uma task falha (testar com falha proposital) | Alterar uma task para `raise Exception` e verificar logs |
| 7 | `on_success_callback` dispara na task `notificar_equipe` | Logs da task após execução bem-sucedida |
| 8 | `{{ ds }}` aparece nos logs de pelo menos 3 tasks distintas | Logs das tasks (buscar data formatada) |
| 9 | Grupo Spark usa `SparkSubmitOperator` ou PythonOperator que simula spark-submit | Código da DAG |
| 10 | Trigger manual completa com sucesso (todas as tasks verdes ou skipped no branch) | Botão "Trigger DAG" |

### Auto-avaliação de Qualidade

| Aspecto | 🟢 Excelente | 🟡 Bom | 🔴 Revisar |
|---------|-------------|---------|-----------|
| Tasks | 12+ tasks, 4 grupos | 10 tasks, 3 grupos | <10 tasks |
| Sensors | 2 sensors com `poke_interval` e `timeout` configurados | 2 sensors com defaults | 1 sensor ou nenhum |
| Branching | Branch + convergência com trigger_rule + ambos testados | Branch funcional | Sem branch |
| TaskGroups | 4 grupos: sensores, spark, python, gold | 3 grupos | <3 grupos |
| Templates | `{{ ds }}` em 3+ locais (sensor, processamento, relatório) | `{{ ds }}` em 2 locais | Sem templates |
| Callbacks | `on_failure` global + `on_success` per-task | Apenas `on_failure` | Sem callbacks |
| Spark | `SparkSubmitOperator` com `application_args={{ ds }}` | PythonOperator simulando | Sem integração |

---

## Dicas

<details>
<summary>💡 Dica 1: Estrutura do BranchPythonOperator com TaskGroups</summary>

O branch deve retornar o `task_id` da **primeira task dentro do grupo**, não o `group_id`. Com TaskGroups, o task_id inclui o prefixo do grupo:

```python
def decidir_processamento(**context):
    volume = context["ti"].xcom_pull(task_ids="contar_registros_total", key="volume")
    
    if volume > 1_000_000:
        # Retorna a primeira task DENTRO do grupo Spark
        return "processamento_spark.spark_ingestao"
    else:
        # Retorna a primeira task DENTRO do grupo Python
        return "processamento_python.python_ingestao"
```

Lembre-se: com TaskGroups, o namespace é `grupo_id.task_id`!

</details>

<details>
<summary>💡 Dica 2: trigger_rule no primeiro task do grupo gold</summary>

Como o branch pula um dos caminhos (Spark ou Python), a **primeira task do grupo gold** precisa do `trigger_rule`. As tasks subsequentes dentro do mesmo grupo herdam o fluxo normal:

```python
with TaskGroup("gold") as grupo_gold:
    agregar = PythonOperator(
        task_id="agregar_metricas",
        python_callable=agregar_metricas,
        trigger_rule="none_failed_min_one_success",  # ← Apenas aqui!
    )
    relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
        # Não precisa de trigger_rule — depende apenas de agregar
    )
    agregar >> relatorio
```

</details>

<details>
<summary>💡 Dica 3: Sensors com FileSensor e templates</summary>

Os sensors podem usar `{{ ds }}` no filepath para aguardar o arquivo do dia correto:

```python
from airflow.sensors.filesystem import FileSensor

with TaskGroup("sensores") as grupo_sensores:
    sensor_a = FileSensor(
        task_id="sensor_parceiro_a",
        filepath="/opt/airflow/data/parceiro_a/vendas_{{ ds }}.csv",
        poke_interval=30,
        timeout=300,
        mode="poke",
    )
    sensor_b = FileSensor(
        task_id="sensor_parceiro_b",
        filepath="/opt/airflow/data/parceiro_b/vendas_{{ ds }}.json",
        poke_interval=30,
        timeout=300,
        mode="poke",
    )
```

Para testar, crie os arquivos manualmente antes de triggerar:
```bash
docker exec airflow-scheduler mkdir -p /opt/airflow/data/parceiro_a /opt/airflow/data/parceiro_b
docker exec airflow-scheduler touch /opt/airflow/data/parceiro_a/vendas_2024-01-01.csv
docker exec airflow-scheduler touch /opt/airflow/data/parceiro_b/vendas_2024-01-01.json
```

</details>

<details>
<summary>💡 Dica 4: Dependências entre grupos e tasks externas</summary>

Ao definir dependências entre TaskGroups e tasks individuais, use o objeto do grupo diretamente:

```python
# Tasks fora de grupos
contar = PythonOperator(task_id="contar_registros_total", ...)
decidir = BranchPythonOperator(task_id="decidir_processamento", ...)
notificar = PythonOperator(task_id="notificar_equipe", ...)

# Montar a cadeia completa
grupo_sensores >> contar >> decidir >> [grupo_spark, grupo_python] >> grupo_gold >> notificar
```

O Airflow entende que `grupo_sensores >> contar` significa: "todas as tasks do grupo sensores devem terminar antes de `contar` executar".

</details>

---

## Como Testar sua DAG

### Passo 1: Criar os arquivos de dados para os sensors

```bash
# Criar diretórios e arquivos que os sensors esperam
docker exec airflow-scheduler mkdir -p /opt/airflow/data/parceiro_a /opt/airflow/data/parceiro_b
docker exec airflow-scheduler bash -c 'echo "id,produto,valor" > /opt/airflow/data/parceiro_a/vendas_2024-01-01.csv'
docker exec airflow-scheduler bash -c 'echo "{\"vendas\": []}" > /opt/airflow/data/parceiro_b/vendas_2024-01-01.json'
```

### Passo 2: Verificar parsing

```bash
docker exec airflow-scheduler python /opt/airflow/dags/dag_pipeline_avancado_completo.py
```

Se não houver output = sem erros de sintaxe.

### Passo 3: Listar tasks

```bash
docker exec airflow-scheduler airflow tasks list dataflow_pipeline_avancado_v1 --tree
```

Deve mostrar 10+ tasks organizadas nos 4 grupos.

### Passo 4: Trigger manual e validação

Na UI (http://localhost:8081):
1. Encontre `dataflow_pipeline_avancado_v1` na lista
2. Ative o toggle (unpause)
3. Clique em "Trigger DAG" com `execution_date = 2024-01-01`
4. Acompanhe na **Graph View** — os 4 grupos devem aparecer colapsáveis
5. Verifique que o branch escolheu UM dos caminhos e o outro ficou skipped
6. Confirme que o grupo gold executou (trigger_rule funcionou)

### Passo 5: Testar o callback de falha

Temporariamente, adicione `raise Exception("Teste de falha!")` em uma task e triggere novamente. Verifique nos logs que o `on_failure_callback` disparou a mensagem de alerta.

---

## Reflexão Final

> **Marina Silva (CTO):** "Isso é um pipeline de produção de verdade. Sensors garantem que não processamos dados antes de estarem prontos. Branching adapta o processamento ao volume do dia. TaskGroups organizam visualmente para que qualquer pessoa da equipe entenda o pipeline em segundos. SparkSubmit garante que o processamento pesado vai para o cluster certo. E callbacks nos alertam proativamente se algo deu errado. Na próxima aula, vamos adicionar a última camada que falta: **qualidade de dados**. Não adianta ter o pipeline mais robusto do mundo se os dados que passam por ele estão sujos."

> **Carlos Mendes:** "Este é o tipo de DAG que eu escrevo no dia a dia como engenheiro de dados sênior. A complexidade individual de cada componente é baixa — vocês já dominaram cada peça nos exercícios anteriores. A habilidade que este desafio testa é a **composição**: saber orquestrar múltiplas peças juntas de forma coerente, legível e resiliente. Se vocês construíram isso do zero, estão prontos para qualquer pipeline Airflow no mercado."

---

## Referências

- [BranchPythonOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#branchpythonoperator)
- [FileSensor](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/file.html)
- [TaskGroups](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskgroups.html)
- [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)
- [Callbacks](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/callbacks.html)
- [Trigger Rules](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules)
- [Template Variables Reference](https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html)
