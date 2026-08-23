# Exercício 5 — Integrar Quality Checks em DAG Airflow com Alertas

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Marina Silva (CTO):** "O sistema de quarentena que vocês construíram no exercício anterior funciona muito bem no notebook. Mas notebooks não rodam sozinhos às 6h da manhã. Quero que esses quality checks virem uma **etapa obrigatória** no pipeline Airflow diário. Se a taxa de quarentena ultrapassar 5%, o pipeline **para** e manda alerta para a equipe. Se estiver abaixo de 5%, segue para a camada Silver normalmente."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "É um caso clássico de **quality gate** — um ponto de decisão no pipeline. Vamos usar `BranchPythonOperator` como portão: ele roda os checks, calcula a taxa de quarentena, e decide se prossegue ou alerta. O relatório de qualidade vai via XCom para que a task de alerta inclua os detalhes no Slack. E vou colocar um `FileSensor` no início para só começar quando o arquivo do dia chegar."

> **Ana Rodrigues (Product Owner):** "Ontem o relatório executivo foi para o Roberto com dados errados — 15% dos registros estavam com problemas. Se tivéssemos esse gate, teríamos interceptado antes de gerar o relatório. Quero que o SLA do check seja no máximo 10 minutos — se demorar mais, algo está errado com o volume."

## Objetivos

Ao final deste exercício, você será capaz de:

- Estruturar uma DAG Airflow com quality gate como ponto de decisão
- Usar `BranchPythonOperator` para decidir entre prosseguir ou alertar com base na taxa de quarentena
- Passar métricas de qualidade entre tasks usando XCom
- Implementar `on_failure_callback` com detalhes do relatório de quarentena
- Configurar SLA em task de quality check (máx 10 min)
- Combinar `FileSensor` com lógica de branching em um pipeline coeso

## Pré-requisitos

- Ambiente Docker rodando (Spark + Airflow)
- Airflow UI acessível em http://localhost:8081
- Exercício 04 concluído (sistema de quarentena implementado)
- Familiaridade com `BranchPythonOperator`, `FileSensor`, XCom e callbacks (Aula 05)

## Duração Estimada

⏱️ ~15 minutos

---

## Problema

Crie uma DAG em `aula_06/code/dags/dag_quality_gate.py` que implementa o seguinte fluxo:

```
┌───────────────────┐     ┌───────────────────┐     ┌────────────────────┐
│  esperar_arquivo  │────▶│  executar_checks  │────▶│  quality_gate      │
│  (FileSensor)     │     │  (PythonOperator) │     │  (BranchOperator)  │
└───────────────────┘     └───────────────────┘     └────────┬───────────┘
                                                             │
                                              ┌──────────────┼──────────────┐
                                              ▼                             ▼
                                   ┌──────────────────┐          ┌──────────────────┐
                                   │ processar_silver │          │ alerta_qualidade │
                                   │ (PythonOperator) │          │ (PythonOperator) │
                                   └──────────────────┘          └──────────────────┘
```

### Regras do Quality Gate

- **Taxa de quarentena < 5%** → seguir para `processar_silver` (pipeline continua normalmente)
- **Taxa de quarentena ≥ 5%** → desviar para `alerta_qualidade` (pipeline para e alerta a equipe)

### Requisitos da DAG

| Componente | Requisito |
|-----------|-----------|
| `esperar_arquivo` | FileSensor que aguarda o arquivo de vendas do dia (`vendas_{{ ds_nodash }}.parquet`) |
| `executar_checks` | PythonOperator que roda quality checks (completude, unicidade, validade) e push do relatório via XCom |
| `quality_gate` | BranchPythonOperator que lê o relatório via XCom e decide: `processar_silver` ou `alerta_qualidade` |
| `processar_silver` | PythonOperator que processa dados válidos para camada Silver |
| `alerta_qualidade` | PythonOperator que simula envio de alerta Slack com detalhes do relatório |
| SLA | Task `executar_checks` com `sla=timedelta(minutes=10)` |
| Callback | `on_failure_callback` em `default_args` que inclui taxa de quarentena na mensagem |

---

## Estrutura Esperada da DAG

```python
# aula_06/code/dags/dag_quality_gate.py

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta

# default_args com on_failure_callback
# ...

with DAG(
    dag_id="dataflow_quality_gate_v1",
    # ... configurações ...
    schedule_interval="0 6 * * *",
    tags=["dataflow", "qualidade", "aula06"],
) as dag:
    # 5 tasks: sensor → checks → gate → [silver, alerta]
    pass
```

---

## Sua Tarefa

Implemente a DAG completa seguindo as etapas abaixo. Lembre-se: este é um exercício intermediário — use as dicas para se guiar, mas construa a solução você mesmo.

### Etapa 1: Callback de Falha com Contexto de Qualidade

Defina uma função `alerta_falha_pipeline` que:
- Extrai informações padrão do `context` (task_id, dag_id, ds, exception)
- Formata uma mensagem 🚨 simulando alerta Slack
- Inclui informações extras sobre o quality check (se disponível via XCom)

### Etapa 2: Task `executar_checks` — Rodar Quality Checks

Defina uma função `executar_quality_checks` que:
- Simula a execução dos quality checks do Exercício 04
- Calcula métricas: total de registros, registros válidos, registros em quarentena, taxa de quarentena
- Faz push do relatório via XCom (dicionário com todas as métricas)
- Retorna o relatório para uso subsequente

### Etapa 3: Task `quality_gate` — Decisão de Branching

Defina uma função `decidir_quality_gate` que:
- Faz pull do relatório de qualidade via XCom
- Avalia se `taxa_quarentena < 0.05` (5%)
- Retorna o task_id do próximo passo: `"processar_silver"` ou `"alerta_qualidade"`

### Etapa 4: Task `alerta_qualidade` — Notificação Detalhada

Defina uma função `enviar_alerta_qualidade` que:
- Faz pull do relatório via XCom
- Formata mensagem detalhada com: taxa, quantidade por motivo, recomendação de ação
- Simula envio para Slack (print formatado)

### Etapa 5: Montar a DAG e Dependências

Monte as 5 tasks com as dependências corretas e o branching.

---

## Dicas

<details>
<summary>💡 Dica 1 — Como estruturar o relatório de qualidade no XCom</summary>

O XCom funciona melhor com dicionários serializáveis. Estruture seu relatório assim:

```python
relatorio = {
    "total_registros": 51500,
    "registros_validos": 44000,
    "registros_quarentena": 7500,
    "taxa_quarentena": 0.145,  # 14.5%
    "detalhes": {
        "completude": 2500,
        "unicidade": 1500,
        "integridade": 2000,
        "validade": 1500
    },
    "execution_date": "2024-01-15"
}

# Push para XCom
context["ti"].xcom_push(key="relatorio_qualidade", value=relatorio)
```

Para simular diferentes cenários (teste com taxa < 5% e ≥ 5%), use uma variável ou random:

```python
import random
# Simula: 70% das vezes taxa é baixa, 30% das vezes é alta
taxa = random.uniform(0.01, 0.04) if random.random() > 0.3 else random.uniform(0.05, 0.20)
```

</details>

<details>
<summary>💡 Dica 2 — Como fazer branching baseado em XCom</summary>

O `BranchPythonOperator` retorna o `task_id` do próximo passo a executar:

```python
def decidir_quality_gate(**context):
    ti = context["ti"]
    relatorio = ti.xcom_pull(task_ids="executar_checks", key="relatorio_qualidade")
    
    taxa = relatorio["taxa_quarentena"]
    
    if taxa < 0.05:
        print(f"✅ Taxa de quarentena {taxa:.1%} — OK, seguindo para Silver")
        return "processar_silver"
    else:
        print(f"🚨 Taxa de quarentena {taxa:.1%} — ALTO! Alertando equipe")
        return "alerta_qualidade"
```

**Importante**: o valor retornado deve ser o `task_id` exato da task destino.

</details>

<details>
<summary>💡 Dica 3 — FileSensor para esperar dados do dia</summary>

Use template variables do Airflow para montar o caminho dinâmico:

```python
esperar_arquivo = FileSensor(
    task_id="esperar_arquivo",
    filepath="/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.parquet",
    poke_interval=30,       # Verifica a cada 30 segundos
    timeout=1800,           # Timeout de 30 minutos
    mode="poke",            # ou "reschedule" para liberar worker slot
    soft_fail=False,        # Se timeout, task falha (não skip)
)
```

`{{ ds_nodash }}` produz a data sem hífens (ex: `20240115`), ideal para nomes de arquivo.

</details>

<details>
<summary>💡 Dica 4 — Como configurar SLA na task de quality check</summary>

O SLA define o tempo máximo aceitável. Se ultrapassar, dispara alerta (mas não mata a task):

```python
executar_checks = PythonOperator(
    task_id="executar_checks",
    python_callable=executar_quality_checks,
    sla=timedelta(minutes=10),  # Se demorar >10min, algo está errado
)
```

Combine com `sla_miss_callback` na DAG para alertar:

```python
def alerta_sla_quality(dag, task_list, blocking_task_list, slas, blocking_tis):
    for sla in slas:
        print(f"⏰ SLA VIOLADO: Quality check demorou mais que 10min! task={sla.task_id}")

with DAG(
    dag_id="dataflow_quality_gate_v1",
    sla_miss_callback=alerta_sla_quality,
    ...
) as dag:
```

</details>

<details>
<summary>💡 Dica 5 — Callback que acessa XCom para incluir taxa no alerta</summary>

Dentro de um `on_failure_callback`, você pode acessar XComs de tasks anteriores:

```python
def alerta_falha_pipeline(context):
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    exception = context.get("exception", "Erro desconhecido")
    
    # Tenta puxar relatório de qualidade (pode não existir se falhou antes)
    relatorio = ti.xcom_pull(task_ids="executar_checks", key="relatorio_qualidade")
    
    taxa_info = ""
    if relatorio:
        taxa_info = f"\n  • Taxa de quarentena: {relatorio['taxa_quarentena']:.1%}"
    
    print(f"""
    🚨 ALERTA PIPELINE — DataFlow Analytics
    ════════════════════════════════════════
    • DAG: {dag_id}
    • Task: {task_id}
    • Data: {context['ds']}
    • Erro: {exception}{taxa_info}
    """)
```

</details>

<details>
<summary>💡 Dica 6 — Dependências com branching (trigger_rule)</summary>

Quando se usa `BranchPythonOperator`, as tasks que NÃO são escolhidas ficam com status `skipped`. Se você tiver tasks downstream que dependem de ambos os branches, precisa de `trigger_rule`:

```python
# Neste exercício, processar_silver e alerta_qualidade são finais (sem convergência)
# Então não precisa de trigger_rule especial

# Dependências:
esperar_arquivo >> executar_checks >> quality_gate
quality_gate >> processar_silver
quality_gate >> alerta_qualidade
```

Alternativamente, com lista:
```python
quality_gate >> [processar_silver, alerta_qualidade]
```

O `BranchPythonOperator` automaticamente faz skip das tasks não retornadas.

</details>

---

## Critérios de Validação

Seu exercício está completo quando:

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | DAG aparece no Airflow UI sem erros de import | `airflow dags list-import-errors` retorna vazio |
| 2 | FileSensor presente como primeira task | Visualizar no Graph View — task tipo sensor no início |
| 3 | BranchPythonOperator decide corretamente | Executar com taxa < 5%: `processar_silver` roda e `alerta_qualidade` fica skipped. Vice-versa com taxa ≥ 5% |
| 4 | Relatório de qualidade passado via XCom | Na aba XCom do Airflow UI, verificar que `relatorio_qualidade` existe com dicionário de métricas |
| 5 | Task de alerta formata mensagem com detalhes | Nos logs de `alerta_qualidade`, a mensagem inclui taxa, contagem por motivo |
| 6 | SLA configurado no quality check | Task `executar_checks` tem `sla=timedelta(minutes=10)` |
| 7 | on_failure_callback configurado | Em `default_args`, o callback está presente e inclui info do XCom |
| 8 | Dependências corretas | `esperar_arquivo >> executar_checks >> quality_gate >> [processar_silver, alerta_qualidade]` |

---

## Teste Rápido

Para testar sem esperar o `FileSensor`, você pode:

1. Criar o arquivo que o sensor espera:
   ```bash
   docker exec airflow-scheduler touch /opt/airflow/data/incoming/vendas_$(date +%Y%m%d).parquet
   ```

2. Ou temporariamente substituir o sensor por um `EmptyOperator` durante o desenvolvimento

3. Trigger manual:
   ```bash
   docker exec airflow-scheduler airflow dags trigger dataflow_quality_gate_v1
   ```

4. Para forçar um cenário específico no branching, ajuste a função `executar_quality_checks` para retornar uma taxa fixa (ex: 0.03 para testar path Silver, 0.10 para testar path alerta)

---

## Perguntas para Reflexão

Antes de seguir para o próximo exercício, pense:

1. **Threshold fixo vs dinâmico**: 5% é uma boa taxa? Em um pipeline real, como você definiria o threshold? (Dica: média histórica + desvio padrão)
2. **Granularidade do gate**: faz sentido ter um gate binário (ok/alerta) ou seria melhor ter 3 níveis (ok/warning/critical)?
3. **Recovery**: quando o alerta dispara, o que acontece no dia seguinte? Os dados do dia alertado precisam ser reprocessados manualmente?
4. **Custo do check**: rodar quality checks em 100% dos dados todo dia é viável? Em datasets muito grandes, amostragem seria melhor?
5. **XCom limits**: o Airflow tem limite de tamanho para XComs (~48KB por padrão no metadata DB). Como você passaria relatórios muito grandes entre tasks?

---

## Próximo Exercício

➡️ **Exercício 6 — Desafio: DataQualityFramework** (`06_framework_qualidade.md`): construir um framework de qualidade de dados reutilizável como classe Python, com interface fluente para definir checks, thresholds configuráveis e geração automática de relatório HTML.
