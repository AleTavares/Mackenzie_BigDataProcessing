# Exercício 1 — Sua Primeira DAG: Pipeline de Vendas Diárias

## Duração Estimada

⏱️ ~30 minutos

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Chegou a hora! Todo dia às 6h da manhã eu abro o terminal, rodo o script de extração de vendas, depois o de transformação, e por último o de carga no data lake. Se um passo falha, eu preciso lembrar de onde parei. Se eu esqueço de rodar, a Ana só descobre quando abre o relatório vazio no dia seguinte. Vamos acabar com isso criando nossa primeira DAG no Airflow."

> **Ana Rodrigues (Product Owner):** "Carlos, semana passada o relatório de vendas não ficou pronto até as 10h. Quando automatizamos isso? Preciso dos dados consolidados antes da reunião diária das 9h."

> **Marina Silva (CTO):** "A solução é simples: transformar o pipeline manual do Carlos em uma DAG — um grafo de tarefas com dependências. O Airflow executa automaticamente, monitora falhas e permite reprocessamento. É a diferença entre artesanato e engenharia de dados."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar um arquivo de DAG Python válido para o Airflow
- Configurar `default_args` e parâmetros da DAG (schedule, start_date, catchup)
- Definir tasks usando `PythonOperator`
- Passar dados entre tasks usando XComs
- Estabelecer dependências entre tasks com o operador `>>`
- Verificar, acionar e monitorar a DAG pelo Airflow UI

## Pré-requisitos

- Ambiente Airflow rodando (ver `00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Pasta `aula_04/code/dags/` criada e montada no container
- Login no Airflow com admin/admin

## O que vamos construir?

O pipeline diário de vendas da DataFlow em 3 etapas:

```
┌─────────────┐      ┌──────────────────┐      ┌───────────────┐
│   EXTRAIR   │─────▶│   TRANSFORMAR    │─────▶│   CARREGAR    │
│  (vendas)   │      │  (validação)     │      │  (data lake)  │
└─────────────┘      └──────────────────┘      └───────────────┘
     │                       │                        │
     ▼                       ▼                        ▼
  1500 registros       1425 válidos           Carga confirmada
  extraídos            (95% taxa)             no data lake
```

Cada etapa passa informações para a próxima usando **XComs** (cross-communication) — o mecanismo nativo do Airflow para troca de dados entre tasks.

---

## Passo 1: Entender a Estrutura de uma DAG

**Descrição:** Antes de escrever código, vamos entender os componentes que toda DAG precisa ter. Pense na DAG como a "receita" que o Airflow segue para executar seu pipeline.

**Anatomia de uma DAG:**

```
┌─────────────────────────────────────────────────────────┐
│  ARQUIVO PYTHON (.py)                                    │
│                                                          │
│  1. IMPORTS ─────────── Bibliotecas necessárias          │
│  2. DEFAULT_ARGS ────── Configurações padrão das tasks   │
│  3. DAG ─────────────── Definição do pipeline            │
│  4. FUNÇÕES ─────────── Lógica de cada task              │
│  5. TASKS ───────────── Instâncias dos operadores        │
│  6. DEPENDÊNCIAS ────── Ordem de execução                │
└─────────────────────────────────────────────────────────┘
```

**Conceitos-chave:**

| Conceito | O que é | Analogia |
|----------|---------|----------|
| **DAG** | Directed Acyclic Graph — grafo de tarefas sem ciclos | Receita com etapas em ordem |
| **Task** | Uma unidade de trabalho dentro da DAG | Um passo da receita |
| **Operator** | Template que define *como* a task executa | Tipo de ação (cozinhar, cortar, misturar) |
| **XCom** | Mecanismo de troca de dados entre tasks | Nota passada entre cozinheiros |
| **Schedule** | Frequência de execução automática | Timer da cozinha |

> **Carlos:** "O nome 'DAG' vem da teoria dos grafos: é um grafo **direcionado** (as setas têm sentido) e **acíclico** (não tem loops). Isso garante que o Airflow sempre sabe em que ordem executar. Se houvesse um ciclo (A depende de B que depende de A), o scheduler entraria em loop infinito."

---

## Passo 2: Criar o Arquivo da DAG

**Descrição:** Vamos criar o arquivo Python que contém nossa primeira DAG. Cada linha será explicada em detalhe nos passos seguintes.

**Comando:**
```bash
cat > aula_04/code/dags/dag_vendas_diarias.py << 'EOF'
"""
DAG: Pipeline Diário de Vendas da DataFlow Analytics
=====================================================
Descrição: Extrai vendas do dia anterior, valida os dados e carrega no data lake.
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Schedule: Todo dia às 6h da manhã (0 6 * * *)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ============================================================
# 1. DEFAULT ARGS — Configurações padrão para TODAS as tasks
# ============================================================
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# 2. FUNÇÕES — Lógica de negócio de cada task
# ============================================================
def extrair_dados(**context):
    """
    Extrai dados de vendas do dia anterior.
    Simula leitura de uma fonte de dados (API, banco, arquivo).
    """
    execution_date = context["ds"]
    print(f"=" * 50)
    print(f"📥 EXTRAÇÃO DE DADOS")
    print(f"=" * 50)
    print(f"Data de referência: {execution_date}")
    print(f"Conectando à fonte de dados...")
    print(f"Lendo registros de vendas...")

    # Simula a extração — em produção, seria uma query ou API call
    total_registros = 1500
    print(f"✅ Extração concluída: {total_registros} registros obtidos")

    # Envia o resultado via XCom para a próxima task
    context["ti"].xcom_push(key="registros_extraidos", value=total_registros)


def transformar_dados(**context):
    """
    Aplica transformações e validações nos dados extraídos.
    Simula limpeza: remove duplicatas, valida campos obrigatórios.
    """
    ti = context["ti"]

    # Recupera dados da task anterior via XCom
    registros_extraidos = ti.xcom_pull(
        task_ids="extrair_dados", key="registros_extraidos"
    )

    print(f"=" * 50)
    print(f"🔄 TRANSFORMAÇÃO DE DADOS")
    print(f"=" * 50)
    print(f"Registros recebidos da extração: {registros_extraidos}")
    print(f"Removendo duplicatas...")
    print(f"Validando campos obrigatórios...")
    print(f"Aplicando regras de negócio...")

    # Simula transformação — 95% dos registros passam na validação
    registros_validos = int(registros_extraidos * 0.95)
    registros_rejeitados = registros_extraidos - registros_validos

    print(f"✅ Transformação concluída:")
    print(f"   - Registros válidos: {registros_validos}")
    print(f"   - Registros rejeitados: {registros_rejeitados}")
    print(f"   - Taxa de aprovação: 95%")

    # Envia resultado para a próxima task
    ti.xcom_push(key="registros_validos", value=registros_validos)


def carregar_dados(**context):
    """
    Carrega dados validados no data lake da DataFlow.
    Simula escrita em Parquet particionado por data.
    """
    ti = context["ti"]

    # Recupera dados da task anterior via XCom
    registros_validos = ti.xcom_pull(
        task_ids="transformar_dados", key="registros_validos"
    )

    execution_date = context["ds"]

    print(f"=" * 50)
    print(f"📤 CARGA NO DATA LAKE")
    print(f"=" * 50)
    print(f"Registros a carregar: {registros_validos}")
    print(f"Destino: datalake/silver/vendas/data={execution_date}/")
    print(f"Formato: Parquet particionado")
    print(f"Escrevendo dados...")
    print(f"✅ Carga concluída com sucesso!")
    print(f"   - {registros_validos} registros salvos no data lake")
    print(f"   - Partição: data={execution_date}")


# ============================================================
# 3. DAG — Definição do pipeline
# ============================================================
with DAG(
    dag_id="dataflow_vendas_diarias_v1",
    default_args=default_args,
    description="Pipeline diário de processamento de vendas da DataFlow",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "vendas", "diario"],
) as dag:

    # ========================================================
    # 4. TASKS — Instâncias dos operadores
    # ========================================================
    extrair = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
    )

    transformar = PythonOperator(
        task_id="transformar_dados",
        python_callable=transformar_dados,
    )

    carregar = PythonOperator(
        task_id="carregar_dados",
        python_callable=carregar_dados,
    )

    # ========================================================
    # 5. DEPENDÊNCIAS — Ordem de execução
    # ========================================================
    extrair >> transformar >> carregar

EOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado com sucesso)
```

**Verificação:**
```bash
ls -la aula_04/code/dags/dag_vendas_diarias.py
```

```
-rw-r--r-- 1 user user XXXX ... dag_vendas_diarias.py
```

> **Carlos:** "Pronto! Escrevemos a DAG inteira de uma vez para você ter a visão do todo. Agora vamos destrinchar cada seção, linha por linha. Não se preocupe se parece muita coisa — cada parte tem uma função clara."

---

## Passo 3: Entendendo os Imports

**Descrição:** As duas primeiras linhas de código importam tudo que precisamos do Airflow. Vamos entender cada import.

**Código (linhas 11-13 do arquivo):**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
```

**Explicação detalhada:**

| Import | O que faz | Por que precisamos |
|--------|-----------|-------------------|
| `from airflow import DAG` | Importa a classe DAG | É o objeto principal que define nosso pipeline |
| `from airflow.operators.python import PythonOperator` | Importa o operador que executa funções Python | Cada task do nosso pipeline executa uma função Python |
| `from datetime import datetime, timedelta` | Importa classes de data/hora | Para definir `start_date` e `retry_delay` |

> **Carlos:** "O Airflow tem dezenas de operadores: `BashOperator` para comandos shell, `SparkSubmitOperator` para jobs Spark, `HttpOperator` para APIs... Hoje vamos focar no `PythonOperator` porque é o mais flexível — qualquer lógica que você escreve em Python vira uma task."

> **💡 Dica:** Se você esquecer um import, o Airflow não vai conseguir carregar a DAG. O erro aparecerá na seção "Import Errors" da interface web.

---

## Passo 4: Entendendo os Default Args

**Descrição:** Os `default_args` são configurações que se aplicam a **todas as tasks** da DAG. Funcionam como valores padrão — qualquer task pode sobrescrever individualmente.

**Código (linhas 18-23 do arquivo):**

```python
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

**Explicação de cada parâmetro:**

| Parâmetro | Valor | Significado |
|-----------|-------|-------------|
| `owner` | `"dataflow"` | Identifica quem é responsável por esta DAG. Aparece no UI e pode ser usado para filtrar |
| `depends_on_past` | `False` | Se `True`, a task de hoje só executa se a de ontem teve sucesso. Mantemos `False` para simplificar |
| `retries` | `1` | Se a task falhar, o Airflow tenta executar novamente 1 vez antes de marcar como falha definitiva |
| `retry_delay` | `timedelta(minutes=5)` | Espera 5 minutos entre a falha e a nova tentativa (dá tempo para problemas temporários se resolverem) |

> **Marina:** "Em produção, configuramos `retries=3` e `retry_delay=timedelta(minutes=10)`. Problemas de rede e timeouts são comuns — o retry automático resolve 90% dos casos sem intervenção humana. Para o lab, 1 retry é suficiente."

> **💡 Dica:** Outros parâmetros comuns em produção: `email_on_failure=True` (envia alerta), `execution_timeout=timedelta(hours=1)` (mata tasks travadas), `sla=timedelta(hours=2)` (alerta se passar do prazo).

---

## Passo 5: Entendendo a Definição da DAG

**Descrição:** O bloco `with DAG(...) as dag:` cria o objeto DAG e define seu comportamento: quando executa, com que frequência, e outras configurações. Tudo que estiver dentro do bloco `with` pertence a essa DAG.

**Código (linhas 69-78 do arquivo):**

```python
with DAG(
    dag_id="dataflow_vendas_diarias_v1",
    default_args=default_args,
    description="Pipeline diário de processamento de vendas da DataFlow",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "vendas", "diario"],
) as dag:
```

**Explicação de cada parâmetro:**

| Parâmetro | Valor | Significado |
|-----------|-------|-------------|
| `dag_id` | `"dataflow_vendas_diarias_v1"` | Identificador único da DAG. É o nome que aparece na lista do Airflow UI |
| `default_args` | `default_args` | Aplica as configurações padrão que definimos no passo anterior |
| `description` | `"Pipeline diário..."` | Texto descritivo que aparece no UI ao lado do nome |
| `schedule_interval` | `"0 6 * * *"` | Expressão cron: executa todo dia às 06:00 UTC |
| `start_date` | `datetime(2024, 1, 1)` | A partir de quando o agendamento é válido |
| `catchup` | `False` | **Importante!** Se `True`, executaria TODAS as datas desde start_date até hoje |
| `tags` | `["dataflow", "vendas", "diario"]` | Etiquetas para filtrar no UI |

**Entendendo o `schedule_interval` (cron):**

```
┌───────────── minuto (0-59)
│ ┌─────────── hora (0-23)
│ │ ┌───────── dia do mês (1-31)
│ │ │ ┌─────── mês (1-12)
│ │ │ │ ┌───── dia da semana (0-6, dom=0)
│ │ │ │ │
0 6 * * *    → "No minuto 0 da hora 6, todo dia, todo mês, todo dia da semana"
                 = Todo dia às 6h da manhã
```

**Entendendo o `catchup`:**

```
Se start_date = 1 de janeiro de 2024 e hoje é 15 de março de 2024:

catchup=True  → Airflow executa 74 vezes (uma para cada dia perdido!)
catchup=False → Airflow executa apenas a próxima data agendada ✅
```

> **Carlos:** "O `catchup=False` é essencial no lab. Se fosse `True`, ao ativar a DAG o Airflow tentaria executar centenas de vezes — uma para cada dia desde janeiro de 2024. Em produção, o catchup é útil para reprocessar dados históricos, mas é preciso planejar com cuidado."

> **⚠️ Atenção:** O `dag_id` deve ser **único** no Airflow inteiro. Se dois arquivos definirem uma DAG com o mesmo ID, o Scheduler vai ignorar um deles e mostrar um warning.

---

## Passo 6: Entendendo as Funções (Lógica de Negócio)

**Descrição:** Cada task executa uma função Python. Essas funções contêm a lógica de negócio do pipeline. O parâmetro `**context` dá acesso a metadados da execução (data, task instance, etc.).

### 6.1 — Função `extrair_dados`

**Código:**

```python
def extrair_dados(**context):
    """Extrai dados de vendas do dia anterior."""
    execution_date = context["ds"]
    print(f"📥 EXTRAÇÃO DE DADOS")
    print(f"Data de referência: {execution_date}")

    total_registros = 1500

    # Envia resultado via XCom
    context["ti"].xcom_push(key="registros_extraidos", value=total_registros)
```

**O que cada parte faz:**

- `**context` — Dicionário com metadados injetados pelo Airflow (data de execução, task instance, etc.)
- `context["ds"]` — Data de execução no formato `YYYY-MM-DD` (ex: `"2024-03-15"`)
- `context["ti"]` — Task Instance: objeto que representa esta execução específica da task
- `xcom_push(key, value)` — **Envia** um valor para o "correio interno" do Airflow (XCom)

### 6.2 — Função `transformar_dados`

**Código:**

```python
def transformar_dados(**context):
    """Aplica transformações e validações nos dados extraídos."""
    ti = context["ti"]

    # Recupera dados da task anterior
    registros_extraidos = ti.xcom_pull(
        task_ids="extrair_dados", key="registros_extraidos"
    )

    registros_validos = int(registros_extraidos * 0.95)
    ti.xcom_push(key="registros_validos", value=registros_validos)
```

**O que cada parte faz:**

- `ti.xcom_pull(task_ids="extrair_dados", key="registros_extraidos")` — **Busca** o valor que a task `extrair_dados` enviou via XCom
- O `task_ids` deve corresponder exatamente ao `task_id` da task que enviou o dado
- O `key` deve corresponder exatamente à chave usada no `xcom_push`

### 6.3 — Função `carregar_dados`

**Código:**

```python
def carregar_dados(**context):
    """Carrega dados validados no data lake."""
    ti = context["ti"]

    registros_validos = ti.xcom_pull(
        task_ids="transformar_dados", key="registros_validos"
    )
    execution_date = context["ds"]

    print(f"✅ {registros_validos} registros salvos em data={execution_date}")
```

**Fluxo de dados via XCom:**

```
extrair_dados ──push("registros_extraidos", 1500)──▶ XCom Store
                                                          │
transformar_dados ◀──pull("extrair_dados", "registros_extraidos")──┘
      │
      └──push("registros_validos", 1425)──▶ XCom Store
                                                 │
carregar_dados ◀──pull("transformar_dados", "registros_validos")──┘
```

> **Carlos:** "XComs são perfeitos para passar metadados pequenos entre tasks: contagens, status, caminhos de arquivo. Mas **nunca** passe DataFrames ou grandes volumes de dados por XCom — o limite padrão é 48KB. Para dados grandes, a task 'extrair' salva em arquivo e passa apenas o caminho para a task seguinte."

> **💡 Dica:** Em produção, a função `extrair_dados` faria algo como: `spark.read.jdbc(...)` ou `requests.get(api_url)`. As funções que escrevemos aqui simulam o comportamento com `print()` para que possamos focar na estrutura da DAG.

---

## Passo 7: Entendendo as Tasks e Dependências

**Descrição:** As tasks são instâncias de operadores que conectam as funções à DAG. As dependências definem a ordem de execução.

**Código (linhas 82-97 do arquivo):**

```python
    # Tasks — Instâncias dos operadores
    extrair = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
    )

    transformar = PythonOperator(
        task_id="transformar_dados",
        python_callable=transformar_dados,
    )

    carregar = PythonOperator(
        task_id="carregar_dados",
        python_callable=carregar_dados,
    )

    # Dependências
    extrair >> transformar >> carregar
```

**Explicação de cada task:**

| Parâmetro | Significado |
|-----------|-------------|
| `task_id` | Nome único da task dentro da DAG (aparece no grafo do UI) |
| `python_callable` | Referência à função Python que será executada |

**Explicação das dependências:**

```python
extrair >> transformar >> carregar
```

Isso é equivalente a:
```python
extrair.set_downstream(transformar)
transformar.set_downstream(carregar)
```

Ou seja:
- `transformar` só executa **depois** que `extrair` terminar com sucesso
- `carregar` só executa **depois** que `transformar` terminar com sucesso
- Se `extrair` falhar, nem `transformar` nem `carregar` serão executadas

**Operadores de dependência:**

| Operador | Significado | Exemplo |
|----------|-------------|---------|
| `>>` | "executa antes de" (downstream) | `A >> B` → A antes de B |
| `<<` | "executa depois de" (upstream) | `B << A` → B depois de A |
| `[A, B] >> C` | "ambos antes de" (fan-in) | A e B executam em paralelo, C espera ambos |
| `A >> [B, C]` | "ambos depois de" (fan-out) | Depois de A, B e C executam em paralelo |

> **Carlos:** "O operador `>>` é açúcar sintático do Python — torna o código muito mais legível que chamar `set_downstream()` explicitamente. Em DAGs complexas com 10+ tasks, essa sintaxe é indispensável para manter o código compreensível."

---

## Passo 8: Validar a DAG no Container

**Descrição:** Antes de verificar no UI, vamos confirmar que o arquivo não tem erros de sintaxe ou importação. Um erro no arquivo Python impede a DAG de ser carregada.

**Comando:**
```bash
docker exec airflow-scheduler python /opt/airflow/dags/aula_04/dag_vendas_diarias.py
```

**Resultado esperado:**
```
(nenhuma saída e nenhum erro — significa que o arquivo é válido!)
```

**Se houver erro**, você verá algo como:
```
Traceback (most recent call last):
  File "/opt/airflow/dags/aula_04/dag_vendas_diarias.py", line X
    ...
SyntaxError: invalid syntax
```

**Verificação alternativa — listar DAGs reconhecidas:**
```bash
docker exec airflow-scheduler airflow dags list 2>/dev/null | grep vendas
```

**Resultado esperado:**
```
dataflow_vendas_diarias_v1 | /opt/airflow/dags/aula_04/dag_vendas_diarias.py | dataflow | False
```

**Explicação:** O comando `airflow dags list` mostra todas as DAGs que o Scheduler reconhece. Se nossa DAG aparece na lista, significa que:
- O arquivo foi encontrado na pasta de DAGs ✅
- O Python conseguiu importar o arquivo sem erros ✅
- Um objeto DAG válido foi encontrado no escopo global ✅

> **💡 Dica:** Se a DAG não aparece na lista, verifique:
> 1. O arquivo está na pasta correta (`aula_04/code/dags/`)
> 2. O arquivo termina com `.py`
> 3. O arquivo define um objeto DAG (com `with DAG(...)` ou `dag = DAG(...)`)
> 4. Não há erros de importação (rode o comando Python acima para ver o traceback)

---

## Passo 9: Verificar a DAG no Airflow UI

**Descrição:** Agora vamos confirmar que a DAG aparece na interface web do Airflow e explorar suas informações.

**Passos no navegador:**

1. Acesse **http://localhost:8081** (já logado como admin)

2. Na página principal (lista de DAGs), você verá:

```
┌──────────────────────────────────────────────────────────────────────┐
│  DAGs                                                     🔍 Search  │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ○ dataflow_vendas_diarias_v1                                        │
│    Pipeline diário de processamento de vendas da DataFlow            │
│    Schedule: 0 6 * * *  │  Owner: dataflow                           │
│    Tags: [dataflow] [vendas] [diario]                                │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

3. Observe os elementos na lista:
   - **Toggle (○)** à esquerda — DAG está **pausada** (desligada). Círculo cinza/off
   - **Nome**: `dataflow_vendas_diarias_v1`
   - **Description**: texto que definimos no parâmetro `description`
   - **Schedule**: expressão cron `0 6 * * *`
   - **Tags**: as etiquetas que definimos

> **⚠️ Importante:** A DAG aparece **pausada** por padrão. Isso é intencional — o Airflow não quer executar DAGs novas automaticamente. Precisamos despausar manualmente (faremos isso no próximo passo ao acionar).

4. Clique no nome **"dataflow_vendas_diarias_v1"** para abrir os detalhes

5. Na página de detalhes, clique na aba **"Graph"**. Você verá:

```
┌─────────────────────────────────────────────────────────┐
│  Graph                                                   │
│                                                          │
│   ┌──────────────┐                                       │
│   │ extrair_dados│                                       │
│   └──────┬───────┘                                       │
│          │                                               │
│          ▼                                               │
│   ┌────────────────────┐                                 │
│   │ transformar_dados  │                                 │
│   └──────────┬─────────┘                                 │
│              │                                           │
│              ▼                                           │
│   ┌────────────────┐                                     │
│   │ carregar_dados │                                     │
│   └────────────────┘                                     │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

Esse é o **grafo de dependências** — a representação visual da DAG! As caixas são tasks e as setas mostram a ordem de execução.

> **Carlos:** "Essa visualização é o grande diferencial do Airflow. Em vez de ler código para entender a ordem de execução, você *vê* o pipeline como um fluxograma. Com 20 tasks e branching, isso salva horas de debugging."

---

## Passo 10: Acionar a DAG Manualmente (Trigger)

**Descrição:** Vamos executar a DAG manualmente para ver o pipeline em ação. Em produção, o Scheduler faria isso automaticamente às 6h — mas no lab, acionamos sob demanda.

**Passos no navegador:**

1. Na página principal de DAGs, **ative a DAG** clicando no toggle à esquerda do nome:

```
Antes:  ○ dataflow_vendas_diarias_v1    (toggle cinza = pausada)
Depois: ● dataflow_vendas_diarias_v1    (toggle azul = ativa)
```

2. Clique no botão **"▶ Trigger DAG"** (ícone de play) à direita da DAG:

```
┌──────────────────────────────────────────────────────────────────┐
│  ● dataflow_vendas_diarias_v1                    [▶] [⟳] [🗑️]  │
│    Pipeline diário de processamento de vendas                     │
└──────────────────────────────────────────────────────────────────┘
                                                    ↑
                                              Clique aqui!
```

3. Um popup pode aparecer perguntando a data de execução (logical date). Mantenha o padrão e clique **"Trigger"**.

4. Observe que uma nova execução aparece na coluna "Runs":

```
┌─────────────────────────────┐
│  Recent Tasks               │
│  ●●● (3 bolinhas verdes)   │
│  Runs: ●                    │
└─────────────────────────────┘
```

> **💡 O que significam as cores:**
> - 🟢 Verde (success) — Task completou com sucesso
> - 🟡 Amarelo (running) — Task está executando agora
> - 🔴 Vermelho (failed) — Task falhou
> - ⬜ Cinza claro (queued) — Task aguardando na fila
> - 🟣 Roxo (scheduled) — Task agendada pelo scheduler

**Alternativa via terminal (trigger por comando):**
```bash
docker exec airflow-scheduler airflow dags trigger dataflow_vendas_diarias_v1
```

**Resultado esperado:**
```
Created <DagRun dataflow_vendas_diarias_v1 @ 2024-xx-xx, externally triggered: True>
```

> **Carlos:** "O botão Trigger é equivalente ao comando `airflow dags trigger`. Em produção, raramente precisamos acionar manualmente — o scheduler cuida disso. Mas para desenvolvimento e reprocessamento, o trigger manual é essencial."

---

## Passo 11: Monitorar a Execução no Graph View

**Descrição:** Vamos acompanhar a execução em tempo real pela aba Graph, onde cada task muda de cor conforme avança.

**Passos no navegador:**

1. Clique no nome da DAG **"dataflow_vendas_diarias_v1"**
2. Clique na aba **"Graph"**
3. Observe as tasks mudando de estado:

**Sequência de execução (acontece em segundos):**

```
Tempo 0s:                    Tempo 1s:                    Tempo 3s:
┌──────────────┐            ┌──────────────┐            ┌──────────────┐
│ extrair_dados│ 🟡         │ extrair_dados│ 🟢         │ extrair_dados│ 🟢
└──────┬───────┘            └──────┬───────┘            └──────┬───────┘
       │                           │                           │
       ▼                           ▼                           ▼
┌────────────────┐          ┌────────────────┐          ┌────────────────┐
│transformar_dados│ ⬜       │transformar_dados│ 🟡       │transformar_dados│ 🟢
└──────┬─────────┘          └──────┬─────────┘          └──────┬─────────┘
       │                           │                           │
       ▼                           ▼                           ▼
┌──────────────┐            ┌──────────────┐            ┌──────────────┐
│carregar_dados│ ⬜          │carregar_dados│ ⬜          │carregar_dados│ 🟢
└──────────────┘            └──────────────┘            └──────────────┘

(queued → running)          (success → next runs)       (todas concluídas!)
```

4. Quando **todas as tasks estão verdes** 🟢, o pipeline executou com sucesso!

5. No topo da página, o indicador de status da DAG Run mostra:

```
┌─────────────────────────────────────────────────┐
│  DAG Run: manual__2024-xx-xx   Status: success  │
│  Duration: ~5s                                   │
└─────────────────────────────────────────────────┘
```

> **Marina:** "A execução levou apenas alguns segundos porque nossas funções são simulações com `print()`. Em produção, um pipeline ETL real pode levar minutos ou horas. A beleza é que o monitoramento visual funciona igual — você vê em tempo real qual task está executando."

---

## Passo 12: Inspecionar os Logs de Cada Task

**Descrição:** Os logs são fundamentais para debugging. Cada task gera seu próprio log com tudo que foi impresso (via `print()`) além de metadados do Airflow. Vamos inspecionar os logs das 3 tasks.

**Passos no navegador:**

1. Na aba **"Graph"**, clique na task **"extrair_dados"** (a caixa verde)
2. No painel lateral que aparece, clique em **"Log"**

**Log esperado para `extrair_dados`:**

```
[2024-xx-xx 06:00:01,234] {taskinstance.py} INFO - Starting attempt 1 of 2
[2024-xx-xx 06:00:01,250] {taskinstance.py} INFO - Executing <Task(PythonOperator): extrair_dados>
[2024-xx-xx 06:00:01,260] {python.py} INFO - Exporting env vars...
==================================================
📥 EXTRAÇÃO DE DADOS
==================================================
Data de referência: 2024-xx-xx
Conectando à fonte de dados...
Lendo registros de vendas...
✅ Extração concluída: 1500 registros obtidos
[2024-xx-xx 06:00:01,280] {taskinstance.py} INFO - Marking task as SUCCESS
```

3. Volte ao Graph e clique na task **"transformar_dados"**, depois em "Log":

**Log esperado para `transformar_dados`:**

```
[2024-xx-xx 06:00:02,100] {taskinstance.py} INFO - Starting attempt 1 of 2
[2024-xx-xx 06:00:02,120] {python.py} INFO - Exporting env vars...
==================================================
🔄 TRANSFORMAÇÃO DE DADOS
==================================================
Registros recebidos da extração: 1500
Removendo duplicatas...
Validando campos obrigatórios...
Aplicando regras de negócio...
✅ Transformação concluída:
   - Registros válidos: 1425
   - Registros rejeitados: 75
   - Taxa de aprovação: 95%
[2024-xx-xx 06:00:02,150] {taskinstance.py} INFO - Marking task as SUCCESS
```

4. Clique na task **"carregar_dados"**, depois em "Log":

**Log esperado para `carregar_dados`:**

```
[2024-xx-xx 06:00:03,050] {taskinstance.py} INFO - Starting attempt 1 of 2
[2024-xx-xx 06:00:03,070] {python.py} INFO - Exporting env vars...
==================================================
📤 CARGA NO DATA LAKE
==================================================
Registros a carregar: 1425
Destino: datalake/silver/vendas/data=2024-xx-xx/
Formato: Parquet particionado
Escrevendo dados...
✅ Carga concluída com sucesso!
   - 1425 registros salvos no data lake
   - Partição: data=2024-xx-xx
[2024-xx-xx 06:00:03,090] {taskinstance.py} INFO - Marking task as SUCCESS
```

**Observações importantes nos logs:**

| Elemento | Significado |
|----------|-------------|
| `Starting attempt 1 of 2` | Primeira tentativa (de 2 possíveis, por causa do `retries=1`) |
| Mensagens com `print()` | Tudo que nossas funções imprimem aparece aqui |
| `Marking task as SUCCESS` | O Airflow confirma que a task finalizou sem erro |
| Timestamps `[2024-xx-xx]` | Horário exato de cada evento (útil para medir performance) |

**Acessar logs via terminal (alternativa):**
```bash
docker exec airflow-scheduler airflow tasks test dataflow_vendas_diarias_v1 extrair_dados 2024-01-15
```

> **Carlos:** "Os logs são seu melhor amigo no debugging. Quando uma task falha em produção às 3h da manhã, é nos logs que você descobre o que aconteceu. Por isso, sempre inclua `print()` descritivos nas funções — eles viram documentação viva do que o pipeline está fazendo."

---

## Passo 13: Verificar XComs na Interface

**Descrição:** Vamos confirmar que os XComs foram criados corretamente. O Airflow armazena todos os valores trocados via XCom e permite visualizá-los pela interface web.

**Passos no navegador:**

1. No menu superior, vá em **Admin → XComs**

2. Você verá uma tabela com todos os XComs gerados pela execução:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│  XCom                                                                                │
├───────┬───────────────────────┬───────────────────┬────────────────────┬─────────────┤
│  Key  │       Task ID         │      DAG ID       │      Value         │  Exec Date  │
├───────┼───────────────────────┼───────────────────┼────────────────────┼─────────────┤
│ registros_extraidos  │ extrair_dados      │ dataflow_vendas_.. │ 1500        │ 2024-xx-xx  │
│ registros_validos    │ transformar_dados  │ dataflow_vendas_.. │ 1425        │ 2024-xx-xx  │
│ return_value         │ extrair_dados      │ dataflow_vendas_.. │ None        │ 2024-xx-xx  │
│ return_value         │ transformar_dados  │ dataflow_vendas_.. │ None        │ 2024-xx-xx  │
│ return_value         │ carregar_dados     │ dataflow_vendas_.. │ None        │ 2024-xx-xx  │
└───────┴───────────────────────┴───────────────────┴────────────────────┴─────────────┘
```

3. Observe os dois XComs que nós criamos explicitamente:
   - **`registros_extraidos`** com valor **1500** (criado por `extrair_dados`)
   - **`registros_validos`** com valor **1425** (criado por `transformar_dados`)

4. Note os `return_value` com `None` — o Airflow cria automaticamente um XCom `return_value` com o retorno da função. Como nossas funções não têm `return`, o valor é `None`.

**Verificar XComs via terminal:**
```bash
docker exec airflow-scheduler airflow tasks test dataflow_vendas_diarias_v1 extrair_dados 2024-01-15 2>&1 | grep -i xcom
```

**Explicação sobre XComs:**

| Aspecto | Detalhe |
|---------|---------|
| **Onde ficam armazenados** | No banco de metadados do Airflow (SQLite no lab, PostgreSQL em produção) |
| **Limite de tamanho** | ~48KB por valor no SQLite (suficiente para metadados, não para dados) |
| **Quando são limpos** | Quando a DAG Run é deletada ou manualmente via UI |
| **Alternativa ao xcom_push** | Retornar valor na função: `return 1500` equivale a `xcom_push(key="return_value", value=1500)` |

> **Marina:** "Os XComs são uma ferramenta de comunicação leve entre tasks. Para datasets grandes, use o padrão: a task de extração salva os dados em um arquivo (S3, data lake) e passa apenas o **caminho** do arquivo via XCom para a task seguinte. Assim você combina a robustez do storage com a flexibilidade dos XComs."

> **💡 Dica:** Se uma função retorna um valor diretamente (`return 1500`), esse valor é automaticamente salvo como XCom com key `"return_value"`. Você pode ler com `ti.xcom_pull(task_ids="minha_task")` (sem especificar key).

---

## Passo 14: Explorar a Aba "Grid" (Histórico de Execuções)

**Descrição:** A aba Grid (anteriormente chamada Tree View) mostra o histórico de todas as execuções da DAG em formato de grade. Cada coluna é uma execução e cada linha é uma task.

**Passos no navegador:**

1. Na página da DAG, clique na aba **"Grid"**

2. Você verá uma grade como esta (com apenas 1 execução, já que acionamos apenas uma vez):

```
┌─────────────────────────────────────────────────────┐
│  Grid View                                           │
│                                                      │
│  Task               │ Run 1 (manual)                 │
│  ───────────────────┼────────────────                │
│  extrair_dados      │    🟢                          │
│  transformar_dados  │    🟢                          │
│  carregar_dados     │    🟢                          │
│                                                      │
│  DAG Run Status:        🟢 success                   │
└─────────────────────────────────────────────────────┘
```

3. Se executarmos a DAG mais vezes, novas colunas aparecem:

```
│  Task               │ Run 1  │ Run 2  │ Run 3  │
│  ───────────────────┼────────┼────────┼────────│
│  extrair_dados      │  🟢    │  🟢    │  🔴    │
│  transformar_dados  │  🟢    │  🟢    │  ⬜    │
│  carregar_dados     │  🟢    │  🟢    │  ⬜    │
```

No exemplo acima, Run 3 teve uma falha na extração — as tasks seguintes nem executaram (ficaram em cinza/queued).

> **Carlos:** "O Grid View é fantástico para detectar padrões. Se toda sexta-feira a task de extração falha, você sabe que tem um problema relacionado ao dia da semana — talvez o sistema fonte faça backup às sextas. Sem histórico visual, esse tipo de padrão passa despercebido."

---

## Passo 15: Executar uma Segunda Vez (Verificar Consistência)

**Descrição:** Vamos acionar a DAG uma segunda vez para confirmar que ela é **idempotente** — executa corretamente múltiplas vezes sem efeitos colaterais inesperados.

**Passos no navegador:**

1. Volte à página principal de DAGs
2. Clique no botão **"▶ Trigger DAG"** novamente
3. Aguarde a execução concluir (todos os 3 quadradinhos ficarem verdes)
4. Vá na aba **Grid** — agora há 2 colunas (2 execuções)

**Verificação via terminal:**
```bash
docker exec airflow-scheduler airflow dags list-runs -d dataflow_vendas_diarias_v1 2>/dev/null
```

**Resultado esperado:**
```
dag_id                      | run_id                           | state   | execution_date
============================+==================================+=========+====================
dataflow_vendas_diarias_v1  | manual__2024-xx-xxTxx:xx:xx+...  | success | 2024-xx-xx ...
dataflow_vendas_diarias_v1  | manual__2024-xx-xxTxx:xx:xx+...  | success | 2024-xx-xx ...
```

> **Marina:** "Cada execução recebe um `run_id` único. O tipo 'manual__' indica que foi acionada manualmente. Execuções automáticas têm tipo 'scheduled__'. Isso é importante para auditoria — você sabe exatamente quem ou o quê disparou cada execução."

---

## Resumo do Exercício

Neste exercício você construiu sua primeira DAG completa no Apache Airflow! Aqui está o que cada componente faz:

| Componente | Função | Analogia |
|------------|--------|----------|
| `default_args` | Configurações comuns a todas as tasks | "Regras da casa" |
| `DAG(...)` | Define o pipeline (nome, schedule, tags) | "Cabeçalho da receita" |
| Funções Python | Lógica de negócio de cada etapa | "Instruções de cada passo" |
| `PythonOperator` | Conecta a função à DAG como task | "Atribuir tarefa a um cozinheiro" |
| `>>` | Define ordem de execução | "Depois de X, faça Y" |
| `xcom_push/pull` | Passa dados entre tasks | "Nota passada ao colega" |

### Fluxo Completo

```
Arquivo .py na pasta dags/
        │
        ▼
Scheduler detecta (< 30s)
        │
        ▼
DAG aparece no UI (pausada)
        │
        ▼
Ativação manual (toggle)
        │
        ▼
Trigger (manual ou scheduled)
        │
        ▼
extrair_dados ──XCom──▶ transformar_dados ──XCom──▶ carregar_dados
        │                       │                        │
        ▼                       ▼                        ▼
    Log + XCom              Log + XCom               Log + XCom
```

### Conceitos-chave Aprendidos

1. **DAG** é um arquivo Python regular — qualquer lógica Python funciona dentro das funções
2. **PythonOperator** executa qualquer função Python como uma task do Airflow
3. **XComs** permitem troca de metadados entre tasks (use para dados pequenos!)
4. **`>>`** define a ordem — tasks downstream só executam se a upstream teve sucesso
5. **`catchup=False`** evita execuções retroativas indesejadas
6. **Logs** capturam tudo que suas funções imprimem com `print()`
7. **Graph View** mostra a DAG como fluxograma visual
8. **Grid View** mostra histórico de execuções para detectar padrões

### Arquivo Criado

| Arquivo | Localização | Função |
|---------|-------------|--------|
| `dag_vendas_diarias.py` | `aula_04/code/dags/` | Primeira DAG — ETL de vendas |

> **Carlos:** "Parabéns! Você acabou de transformar um pipeline manual em um pipeline automatizado. A Ana não vai mais reclamar de relatórios atrasados — o Airflow vai executar religiosamente às 6h da manhã, todo dia, sem depender de ninguém lembrar. Se falhar, temos retry automático e logs detalhados para investigar. Isso é engenharia de dados."

> **Ana:** "Finalmente! Agora preciso que vocês adicionem uma notificação quando o pipeline terminar. E que tal incluir validações mais sofisticadas? E se pudéssemos..."

> **Carlos:** "Calma, Ana! Uma coisa de cada vez. No próximo exercício vamos adicionar dependências mais complexas e um BashOperator para notificações. Passo a passo."

---

## Próximo Exercício

➡️ **Exercício 2 — Dependências e XComs Avançados** (`02_dependencias_xcoms.md`): explorar padrões de dependência (fan-in, fan-out), BashOperator para notificações, e template variables como `{{ ds }}`.
