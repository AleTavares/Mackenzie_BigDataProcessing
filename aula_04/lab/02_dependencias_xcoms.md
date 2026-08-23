# Exercício 2 — Dependências entre Tasks e XComs Avançados

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Ana Rodrigues (Product Owner):** "Carlos, temos um gargalo no pipeline. Estamos processando os dados dos parceiros A, B e C sequencialmente — um depois do outro. Cada extração leva cerca de 5 minutos. São 15 minutos só esperando! Não dá para extrair tudo ao mesmo tempo?"

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Claro que dá, Ana. No Airflow, podemos definir tasks que rodam em paralelo — é o padrão fan-out. As três extrações disparam ao mesmo tempo, e depois convergem para uma task de unificação — o padrão fan-in. O tempo total cai de 15 para 5 minutos."

> **Marina Silva (CTO):** "Esse é um padrão clássico de orquestração. Além disso, Carlos, aproveita para mostrar como usar o `return` das funções como XCom — é mais limpo que o `xcom_push` explícito. E adiciona um BashOperator no final para simular uma notificação."

## Objetivos

Ao final deste exercício, você será capaz de:

- Definir tasks que executam em **paralelo** (fan-out)
- Usar a sintaxe `[task_a, task_b, task_c] >> task_d` (fan-in)
- Usar `return` como alternativa ao `xcom_push` explícito
- Puxar XComs de **múltiplas** tasks upstream
- Adicionar um `BashOperator` para comandos shell
- Observar execução paralela no Graph View do Airflow

## Pré-requisitos

- Exercício 1 concluído (`dag_vendas_diarias.py` funcionando)
- Ambiente Airflow rodando (ver `00_setup.md`)
- Airflow UI acessível em http://localhost:8081

## O que vamos construir?

O pipeline de vendas paralelo da DataFlow — processando dados de 3 parceiros simultaneamente:

```
┌─────────────┐
│ extrair_a   │─┐
└─────────────┘ │
┌─────────────┐ │     ┌───────────┐     ┌──────────┐     ┌──────────┐     ┌───────────┐
│ extrair_b   │─┼────▶│ unificar  │────▶│ validar  │────▶│ carregar │────▶│ notificar │
└─────────────┘ │     └───────────┘     └──────────┘     └──────────┘     └───────────┘
┌─────────────┐ │
│ extrair_c   │─┘
└─────────────┘
```

**Padrões utilizados:**

| Padrão | Onde | Significado |
|--------|------|-------------|
| **Fan-out** | Início → 3 extrações | Uma origem dispara múltiplas tasks em paralelo |
| **Fan-in** | 3 extrações → unificar | Múltiplas tasks convergem para uma única |
| **Sequencial** | unificar → validar → carregar → notificar | Cada task depende da anterior |

**Diferenças em relação ao Exercício 1:**

| Exercício 1 | Exercício 2 |
|-------------|-------------|
| 3 tasks sequenciais | 7 tasks (3 paralelas + 4 sequenciais) |
| `xcom_push()` explícito | `return` como XCom automático |
| Apenas PythonOperator | PythonOperator + BashOperator |
| `A >> B >> C` | `[A, B, C] >> D >> E >> F >> G` |

---

## Passo 1: Entender o Padrão Fan-Out / Fan-In

**Descrição:** Antes de codar, vamos entender visualmente como o Airflow lida com paralelismo. O fan-out/fan-in é o padrão mais comum em pipelines de dados reais.

**Fan-Out (divergência):**

```
         ┌──▶ Task B
Task A ──┼──▶ Task C
         └──▶ Task D
```

Após Task A concluir, B, C e D iniciam **simultaneamente**. O Airflow sabe que não há dependência entre elas — podem rodar em paralelo.

**Fan-In (convergência):**

```
Task B ──┐
Task C ──┼──▶ Task E
Task D ──┘
```

Task E só inicia quando **todas** as tasks upstream (B, C, D) terminarem com sucesso. Se qualquer uma falhar, E não executa.

**Sintaxe no Airflow:**

```python
# Fan-out: uma task dispara várias
task_a >> [task_b, task_c, task_d]

# Fan-in: várias tasks convergem para uma
[task_b, task_c, task_d] >> task_e

# Combinado (nosso caso):
[extrair_a, extrair_b, extrair_c] >> unificar >> validar >> carregar >> notificar
```

> **Carlos:** "Na prática, o Airflow usa o conceito de 'trigger_rule' para decidir quando uma task downstream deve iniciar. O padrão é `all_success` — ou seja, TODAS as tasks anteriores precisam ter sucesso. Existem outras regras como `one_success` (basta uma) ou `none_failed` (nenhuma falhou), mas vamos manter o padrão por agora."

---

## Passo 2: Entender XCom com `return` (return_value)

**Descrição:** No Exercício 1, usamos `xcom_push()` e `xcom_pull()` explicitamente. Existe uma alternativa mais limpa: quando sua função **retorna** um valor, o Airflow automaticamente salva esse valor como XCom com a key `"return_value"`.

**Comparação:**

```python
# Exercício 1 — XCom explícito (push/pull)
def extrair_dados(**context):
    total = 1500
    context["ti"].xcom_push(key="registros_extraidos", value=total)

def transformar_dados(**context):
    total = context["ti"].xcom_pull(task_ids="extrair_dados", key="registros_extraidos")
```

```python
# Exercício 2 — XCom implícito (return)
def extrair_dados(**context):
    total = 1500
    return total  # Airflow salva como XCom automaticamente!

def transformar_dados(**context):
    # Sem key — pega o return_value por padrão
    total = context["ti"].xcom_pull(task_ids="extrair_dados")
```

**Regras do XCom com `return`:**

| Aspecto | Comportamento |
|---------|--------------|
| Key usada | `"return_value"` (automática) |
| Como puxar | `xcom_pull(task_ids="nome_task")` — sem parâmetro `key` |
| Tipos suportados | Qualquer coisa serializável em JSON (int, str, list, dict) |
| Quando usar `return` | Para o valor **principal** da task (o resultado mais importante) |
| Quando usar `xcom_push` | Quando precisa enviar **múltiplos** valores com keys diferentes |

> **Marina:** "Usar `return` é mais Pythônico e torna o código mais limpo. A regra é simples: se sua task produz um único resultado, use `return`. Se produz vários dados diferentes, use `xcom_push` com keys descritivas."

---

## Passo 3: Entender o BashOperator

**Descrição:** O `BashOperator` executa comandos shell dentro da task. É útil para notificações, limpeza de arquivos, chamadas a ferramentas de linha de comando e scripts auxiliares.

**Sintaxe básica:**

```python
from airflow.operators.bash import BashOperator

notificar = BashOperator(
    task_id="notificar",
    bash_command='echo "Pipeline concluído em {{ ds }}"',
)
```

**Pontos importantes:**

| Aspecto | Detalhe |
|---------|---------|
| `bash_command` | String com o comando shell a executar |
| Template variables | `{{ ds }}`, `{{ ts }}`, `{{ dag_run.run_id }}` são substituídas pelo Airflow |
| Código de saída | Se o comando retornar código != 0, a task falha |
| Múltiplos comandos | Use `&&` para encadear: `'cmd1 && cmd2'` |
| Scripts | Aponte para um arquivo: `bash_command='/path/script.sh'` |

**Template variables comuns:**

| Variável | Exemplo de valor | Significado |
|----------|------------------|-------------|
| `{{ ds }}` | `2024-03-15` | Data de execução (YYYY-MM-DD) |
| `{{ ds_nodash }}` | `20240315` | Data sem hífens |
| `{{ ts }}` | `2024-03-15T06:00:00+00:00` | Timestamp completo |
| `{{ dag_run.run_id }}` | `manual__2024-03-15T...` | ID único da execução |

> **Carlos:** "O BashOperator é o canivete suíço do Airflow. Qualquer comando que você rodaria no terminal pode virar uma task. Em produção, usamos para enviar notificações via `curl` para o Slack, compactar arquivos com `gzip`, ou limpar pastas temporárias."

---

## Passo 4: Criar a DAG com Paralelismo

**Descrição:** Vamos criar o arquivo completo da nova DAG. Ela extrai dados de 3 parceiros em paralelo, unifica os resultados, valida, carrega no data lake e envia uma notificação.

**Comando:** Execute o bloco completo abaixo — é um único `cat` que cria o arquivo inteiro:

```bash
cat > aula_04/code/dags/dag_vendas_paralelo.py << 'EOF'
"""
DAG: Pipeline de Vendas com Processamento Paralelo
====================================================
Descrição: Extrai dados de 3 parceiros em paralelo, unifica, valida e carrega.
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrões: Fan-out (extrações paralelas), Fan-in (convergência para unificação)
Schedule: Todo dia às 6h30 (após o pipeline básico)

Estrutura:
    extrair_a ─┐
    extrair_b ─┼──▶ unificar ──▶ validar ──▶ carregar ──▶ notificar
    extrair_c ─┘
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import time


# ============================================================
# 1. DEFAULT ARGS
# ============================================================
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# 2. FUNÇÕES DE EXTRAÇÃO (uma por parceiro)
#    Cada função RETORNA um dict — o Airflow salva como XCom
#    automaticamente com key "return_value"
# ============================================================
def extrair_parceiro_a(**context):
    """
    Extrai dados do Parceiro A (sistema legado CSV).
    Retorna dict com metadados da extração via return (XCom automático).
    """
    execution_date = context["ds"]
    print(f"=" * 50)
    print(f"📥 EXTRAÇÃO — PARCEIRO A (CSV Legado)")
    print(f"=" * 50)
    print(f"Data de referência: {execution_date}")
    print(f"Conectando ao sistema legado...")
    print(f"Encoding: ISO-8859-1 | Separador: ;")

    # Simula tempo de extração (2 segundos)
    time.sleep(2)

    registros = 850
    print(f"✅ Parceiro A: {registros} registros extraídos")

    # RETURN automático como XCom!
    return {"parceiro": "A", "registros": registros, "formato": "csv"}


def extrair_parceiro_b(**context):
    """
    Extrai dados do Parceiro B (API JSON).
    """
    execution_date = context["ds"]
    print(f"=" * 50)
    print(f"📥 EXTRAÇÃO — PARCEIRO B (API JSON)")
    print(f"=" * 50)
    print(f"Data de referência: {execution_date}")
    print(f"Chamando API REST do parceiro B...")
    print(f"Paginando resultados...")

    # Simula tempo de extração (2 segundos)
    time.sleep(2)

    registros = 1200
    print(f"✅ Parceiro B: {registros} registros extraídos")

    return {"parceiro": "B", "registros": registros, "formato": "json"}


def extrair_parceiro_c(**context):
    """
    Extrai dados do Parceiro C (Data Lake Parquet).
    """
    execution_date = context["ds"]
    print(f"=" * 50)
    print(f"📥 EXTRAÇÃO — PARCEIRO C (Data Lake Parquet)")
    print(f"=" * 50)
    print(f"Data de referência: {execution_date}")
    print(f"Lendo partição do data lake...")
    print(f"Formato: Parquet columnar")

    # Simula tempo de extração (2 segundos)
    time.sleep(2)

    registros = 2100
    print(f"✅ Parceiro C: {registros} registros extraídos")

    return {"parceiro": "C", "registros": registros, "formato": "parquet"}


# ============================================================
# 3. FUNÇÕES DE PROCESSAMENTO
#    Demonstram como puxar XComs de MÚLTIPLAS tasks upstream
# ============================================================
def unificar_dados(**context):
    """
    Unifica dados dos 3 parceiros.
    Puxa XComs de múltiplas tasks usando xcom_pull com lista de task_ids.
    """
    ti = context["ti"]

    # Puxar XCom de cada task de extração (return_value)
    dados_a = ti.xcom_pull(task_ids="extrair_parceiro_a")
    dados_b = ti.xcom_pull(task_ids="extrair_parceiro_b")
    dados_c = ti.xcom_pull(task_ids="extrair_parceiro_c")

    print(f"=" * 50)
    print(f"🔗 UNIFICAÇÃO DE DADOS")
    print(f"=" * 50)
    print(f"Parceiro A: {dados_a['registros']} registros ({dados_a['formato']})")
    print(f"Parceiro B: {dados_b['registros']} registros ({dados_b['formato']})")
    print(f"Parceiro C: {dados_c['registros']} registros ({dados_c['formato']})")

    total = dados_a["registros"] + dados_b["registros"] + dados_c["registros"]
    print(f"")
    print(f"Normalizando schemas...")
    print(f"Aplicando schema unificado de vendas...")
    print(f"✅ Unificação concluída: {total} registros totais")

    return {"total_registros": total, "parceiros_processados": 3}


def validar_qualidade(**context):
    """
    Executa validações de qualidade nos dados unificados.
    """
    ti = context["ti"]

    dados_unificados = ti.xcom_pull(task_ids="unificar_dados")
    total = dados_unificados["total_registros"]

    print(f"=" * 50)
    print(f"🔍 VALIDAÇÃO DE QUALIDADE")
    print(f"=" * 50)
    print(f"Registros a validar: {total}")
    print(f"Check 1: Campos obrigatórios não-nulos... ✅")
    print(f"Check 2: order_id únicos (sem duplicatas)... ✅")
    print(f"Check 3: total_amount > 0... ✅")
    print(f"Check 4: order_date não futura... ✅")

    # Simula 2% de rejeição
    rejeitados = int(total * 0.02)
    aprovados = total - rejeitados

    print(f"")
    print(f"✅ Validação concluída:")
    print(f"   - Aprovados: {aprovados} ({100 - 2}%)")
    print(f"   - Rejeitados: {rejeitados} (2%) → quarentena")

    return {"aprovados": aprovados, "rejeitados": rejeitados}


def carregar_datalake(**context):
    """
    Carrega dados validados no data lake (camada Silver).
    """
    ti = context["ti"]
    execution_date = context["ds"]

    resultado_validacao = ti.xcom_pull(task_ids="validar_qualidade")
    aprovados = resultado_validacao["aprovados"]

    print(f"=" * 50)
    print(f"📤 CARGA NO DATA LAKE")
    print(f"=" * 50)
    print(f"Registros a carregar: {aprovados}")
    print(f"Destino: datalake/silver/vendas_unificadas/data={execution_date}/")
    print(f"Formato: Parquet particionado por data")
    print(f"Mode: overwrite (idempotente)")
    print(f"Escrevendo...")
    print(f"✅ Carga concluída: {aprovados} registros no data lake")

    return {"registros_carregados": aprovados, "particao": execution_date}


# ============================================================
# 4. DAG — Definição do pipeline com paralelismo
# ============================================================
with DAG(
    dag_id="dataflow_vendas_paralelo_v1",
    default_args=default_args,
    description="Pipeline paralelo: 3 parceiros simultâneos com unificação",
    schedule_interval="30 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "vendas", "paralelo", "fan-in"],
) as dag:

    # ========================================================
    # 5. TASKS DE EXTRAÇÃO (executam em paralelo!)
    # ========================================================
    extrair_a = PythonOperator(
        task_id="extrair_parceiro_a",
        python_callable=extrair_parceiro_a,
    )

    extrair_b = PythonOperator(
        task_id="extrair_parceiro_b",
        python_callable=extrair_parceiro_b,
    )

    extrair_c = PythonOperator(
        task_id="extrair_parceiro_c",
        python_callable=extrair_parceiro_c,
    )

    # ========================================================
    # 6. TASKS DE PROCESSAMENTO (executam em sequência)
    # ========================================================
    unificar = PythonOperator(
        task_id="unificar_dados",
        python_callable=unificar_dados,
    )

    validar = PythonOperator(
        task_id="validar_qualidade",
        python_callable=validar_qualidade,
    )

    carregar = PythonOperator(
        task_id="carregar_datalake",
        python_callable=carregar_datalake,
    )

    # ========================================================
    # 7. TASK DE NOTIFICAÇÃO (BashOperator)
    # ========================================================
    notificar = BashOperator(
        task_id="notificar_conclusao",
        bash_command="""
            echo "=============================================="
            echo "📧 NOTIFICAÇÃO — Pipeline Concluído"
            echo "=============================================="
            echo "DAG: dataflow_vendas_paralelo_v1"
            echo "Data de execução: {{ ds }}"
            echo "Run ID: {{ run_id }}"
            echo "Horário: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "Status: SUCESSO ✅"
            echo "=============================================="
            echo "Enviando alerta para equipe..."
            echo "✅ Notificação enviada com sucesso!"
        """,
    )

    # ========================================================
    # 8. DEPENDÊNCIAS — O coração do paralelismo!
    # ========================================================
    # Fan-in: as 3 extrações convergem para a unificação
    [extrair_a, extrair_b, extrair_c] >> unificar

    # Sequencial: unificar → validar → carregar → notificar
    unificar >> validar >> carregar >> notificar

EOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado com sucesso)
```

**Verificação:**
```bash
ls -la aula_04/code/dags/dag_vendas_paralelo.py
```

```
-rw-r--r-- 1 user user XXXX ... dag_vendas_paralelo.py
```

> **Carlos:** "Perceba a linha-chave: `[extrair_a, extrair_b, extrair_c] >> unificar`. Essa lista diz ao Airflow: 'essas 3 tasks não dependem uma da outra (podem rodar em paralelo), MAS a task `unificar` depende de TODAS elas'. É elegante e legível."

---

## Passo 5: Analisando as Decisões de Design

**Descrição:** Vamos revisitar as partes mais importantes do código e entender **por que** cada escolha foi feita.

### 5.1 — Por que `return` em vez de `xcom_push`?

```python
def extrair_parceiro_a(**context):
    registros = 850
    return {"parceiro": "A", "registros": registros, "formato": "csv"}
```

Usamos `return` porque cada função de extração tem **um único resultado principal** — os metadados da extração. É mais limpo que:

```python
# Alternativa mais verbosa (funciona, mas desnecessária aqui):
context["ti"].xcom_push(key="dados_parceiro_a", value={"parceiro": "A", ...})
```

### 5.2 — Por que retornamos um `dict` e não apenas o número?

```python
# ✅ Dict — informativo e extensível
return {"parceiro": "A", "registros": 850, "formato": "csv"}

# ❌ Apenas o número — limitado
return 850
```

Com um dict, a task `unificar_dados` sabe **de onde** vieram os registros e em qual formato. Se amanhã precisarmos adicionar mais informações (timestamp, caminho do arquivo), basta adicionar uma key — sem alterar a assinatura.

### 5.3 — Por que `time.sleep(2)` nas extrações?

```python
time.sleep(2)  # Simula tempo de extração
```

É apenas para **visualização no lab**! Sem o sleep, as 3 tasks terminam instantaneamente e é difícil observar o paralelismo no Graph View. Com 2 segundos de espera, você consegue ver as 3 tasks amarelas (running) ao mesmo tempo.

> **Marina:** "Em produção, NUNCA use `time.sleep()` — é desperdício de recursos. O tempo real de processamento (queries, API calls, I/O) é natural."

### 5.4 — Por que o BashOperator para notificação?

```python
notificar = BashOperator(
    task_id="notificar_conclusao",
    bash_command='echo "Pipeline concluído em {{ ds }}"',
)
```

Poderíamos usar PythonOperator, mas o BashOperator demonstra:
- Como usar um operador diferente na mesma DAG
- Como as template variables (`{{ ds }}`) funcionam
- Como simular envio de notificação (em produção: `curl` para Slack/Teams)

---

## Passo 6: Validar a DAG no Container

**Descrição:** Confirmar que o arquivo está sintaticamente correto e que o Airflow consegue carregá-lo.

**Comando:**
```bash
docker exec airflow-scheduler python /opt/airflow/dags/aula_04/dag_vendas_paralelo.py
```

**Resultado esperado:**
```
(nenhuma saída e nenhum erro — arquivo válido!)
```

**Verificação — listar DAGs:**
```bash
docker exec airflow-scheduler airflow dags list 2>/dev/null | grep paralelo
```

**Resultado esperado:**
```
dataflow_vendas_paralelo_v1 | /opt/airflow/dags/aula_04/dag_vendas_paralelo.py | dataflow | False
```

**Se der erro de importação**, verifique:
1. O `import time` está presente no topo
2. O `from airflow.operators.bash import BashOperator` está correto
3. Não há indentação misturada (tabs vs espaços)

> **Carlos:** "Se a DAG não aparece após 30 segundos, force um re-scan com `docker exec airflow-scheduler airflow dags reserialize`. Mas normalmente o Scheduler detecta arquivos novos automaticamente."

---

## Passo 7: Verificar o Grafo no Airflow UI

**Descrição:** Vamos confirmar que o Airflow entendeu corretamente o padrão de paralelismo que definimos.

**Passos no navegador:**

1. Acesse **http://localhost:8081**
2. Localize **"dataflow_vendas_paralelo_v1"** na lista de DAGs
3. Clique no nome da DAG para abrir os detalhes
4. Clique na aba **"Graph"**

**Resultado esperado — Graph View:**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Graph                                                                       │
│                                                                              │
│   ┌──────────────────┐                                                       │
│   │ extrair_parceiro_a│─┐                                                    │
│   └──────────────────┘  │                                                    │
│                          │     ┌───────────────┐     ┌────────────────────┐  │
│   ┌──────────────────┐  ├────▶│ unificar_dados│────▶│ validar_qualidade  │  │
│   │ extrair_parceiro_b│─┤     └───────────────┘     └────────┬───────────┘  │
│   └──────────────────┘  │                                     │              │
│                          │                                     ▼              │
│   ┌──────────────────┐  │                          ┌──────────────────────┐  │
│   │ extrair_parceiro_c│─┘                          │ carregar_datalake    │  │
│   └──────────────────┘                             └──────────┬───────────┘  │
│                                                                │              │
│                                                                ▼              │
│                                                    ┌──────────────────────┐  │
│                                                    │ notificar_conclusao  │  │
│                                                    └──────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**O que observar:**

- As 3 tasks de extração estão **na mesma linha vertical** (sem setas entre elas) — isso indica que são independentes
- Todas as 3 têm setas apontando para `unificar_dados` — o Airflow sabe que precisa esperar as 3
- Depois de `unificar_dados`, o fluxo é sequencial

> **Ana:** "Agora sim! Visualmente fica claro que as extrações rodam ao mesmo tempo. Não preciso ser engenheira para entender esse diagrama."

---

## Passo 8: Acionar a DAG e Observar o Paralelismo

**Descrição:** Vamos executar a DAG e observar as 3 tasks de extração rodando simultaneamente. O `time.sleep(2)` que adicionamos ajuda a visualizar esse comportamento.

**Passos no navegador:**

1. Na lista de DAGs, **ative** a DAG clicando no toggle à esquerda:

```
Antes:  ○ dataflow_vendas_paralelo_v1     (pausada)
Depois: ● dataflow_vendas_paralelo_v1     (ativa)
```

2. Clique no botão **"▶ Trigger DAG"** (ícone de play)

3. **Rapidamente** clique no nome da DAG e vá para a aba **"Graph"**

4. Observe as tasks mudando de estado:

**Sequência de execução (observe o paralelismo!):**

```
Tempo 0s:                           Tempo 1s (PARALELO!):
┌──────────────────┐               ┌──────────────────┐
│ extrair_parceiro_a│ ⬜            │ extrair_parceiro_a│ 🟡 ← Rodando!
└──────────────────┘               └──────────────────┘
┌──────────────────┐               ┌──────────────────┐
│ extrair_parceiro_b│ ⬜            │ extrair_parceiro_b│ 🟡 ← Rodando!
└──────────────────┘               └──────────────────┘
┌──────────────────┐               ┌──────────────────┐
│ extrair_parceiro_c│ ⬜            │ extrair_parceiro_c│ 🟡 ← Rodando!
└──────────────────┘               └──────────────────┘
┌───────────────┐                  ┌───────────────┐
│ unificar_dados│ ⬜                │ unificar_dados│ ⬜ ← Esperando!
└───────────────┘                  └───────────────┘
```

```
Tempo 3s:                           Tempo 5s:
┌──────────────────┐               ┌──────────────────┐
│ extrair_parceiro_a│ 🟢            │ extrair_parceiro_a│ 🟢
└──────────────────┘               └──────────────────┘
┌──────────────────┐               ┌──────────────────┐
│ extrair_parceiro_b│ 🟢            │ extrair_parceiro_b│ 🟢
└──────────────────┘               └──────────────────┘
┌──────────────────┐               ┌──────────────────┐
│ extrair_parceiro_c│ 🟢            │ extrair_parceiro_c│ 🟢
└──────────────────┘               └──────────────────┘
┌───────────────┐                  ┌───────────────┐
│ unificar_dados│ 🟡 ← Agora sim! │ unificar_dados│ 🟢
└───────────────┘                  └───────────────┘
┌────────────────────┐             ┌────────────────────┐
│ validar_qualidade  │ ⬜           │ validar_qualidade  │ 🟢
└────────────────────┘             └────────────────────┘
┌──────────────────────┐           ┌──────────────────────┐
│ carregar_datalake    │ ⬜         │ carregar_datalake    │ 🟢
└──────────────────────┘           └──────────────────────┘
┌──────────────────────┐           ┌──────────────────────┐
│ notificar_conclusao  │ ⬜         │ notificar_conclusao  │ 🟢
└──────────────────────┘           └──────────────────────┘
```

5. **Ponto-chave**: As 3 extrações ficam amarelas (🟡 running) **ao mesmo tempo**! Isso prova o paralelismo.

**Alternativa via terminal (trigger):**
```bash
docker exec airflow-scheduler airflow dags trigger dataflow_vendas_paralelo_v1
```

> **Carlos:** "Viu as 3 tasks amarelas ao mesmo tempo? É isso que economiza os 10 minutos que a Ana reclamava. Em vez de esperar cada parceiro sequencialmente (5+5+5=15 min), todos rodam juntos (max(5,5,5)=5 min). A task `unificar_dados` só sai do 'queued' quando as 3 ficam verdes."

---

## Passo 9: Verificar Timing de Execução

**Descrição:** Vamos confirmar que as 3 extrações realmente executaram em paralelo olhando os timestamps nos detalhes das tasks.

**Passos no navegador:**

1. Na aba **"Graph"** (com todas as tasks verdes), clique em **"extrair_parceiro_a"**
2. No painel lateral, observe os campos **"Started"** e **"Duration"**
3. Repita para **"extrair_parceiro_b"** e **"extrair_parceiro_c"**

**Resultado esperado:**

| Task | Started | Duration |
|------|---------|----------|
| extrair_parceiro_a | 2024-xx-xx **06:30:01** | ~2s |
| extrair_parceiro_b | 2024-xx-xx **06:30:01** | ~2s |
| extrair_parceiro_c | 2024-xx-xx **06:30:01** | ~2s |
| unificar_dados | 2024-xx-xx **06:30:03** | <1s |

**O que isso prova:**

- As 3 extrações iniciaram **no mesmo segundo** (06:30:01) → paralelismo confirmado!
- A unificação iniciou **2 segundos depois** (06:30:03) → esperou as extrações terminarem
- Duração total do fan-out: ~2 segundos (não 6 segundos como seria sequencial)

**Comparação de tempo:**

```
Sequencial:     extrair_a (2s) → extrair_b (2s) → extrair_c (2s) = 6 segundos
Paralelo:       extrair_a (2s) ┐
                extrair_b (2s) ┼ = 2 segundos (max dos 3)
                extrair_c (2s) ┘
                
Economia: 67% do tempo! (4 segundos salvos)
```

> **Ana:** "Em produção, cada extração leva 5 minutos. São 10 minutos economizados por execução — 300 minutos por mês! Isso é quase meio dia de processamento."

---

## Passo 10: Inspecionar os XComs Gerados

**Descrição:** Vamos verificar que os `return` das funções foram corretamente salvos como XComs, e que a função `unificar_dados` conseguiu puxar dados de múltiplas tasks.

**Passos no navegador:**

1. Vá em **Admin → XComs**
2. Filtre por DAG: `dataflow_vendas_paralelo_v1`

**Resultado esperado:**

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│  Key           │ Task ID              │ Value                                                │
├────────────────┼──────────────────────┼──────────────────────────────────────────────────────┤
│ return_value   │ extrair_parceiro_a   │ {"parceiro": "A", "registros": 850, "formato": "csv"}│
│ return_value   │ extrair_parceiro_b   │ {"parceiro": "B", "registros": 1200, "formato":"json"}│
│ return_value   │ extrair_parceiro_c   │ {"parceiro": "C", "registros": 2100, "formato":"parquet"}│
│ return_value   │ unificar_dados       │ {"total_registros": 4150, "parceiros_processados": 3}│
│ return_value   │ validar_qualidade    │ {"aprovados": 4067, "rejeitados": 83}                │
│ return_value   │ carregar_datalake    │ {"registros_carregados": 4067, "particao":"2024-xx-xx"}│
└────────────────┴──────────────────────┴──────────────────────────────────────────────────────┘
```

**O que observar:**

- Todos os XComs têm key `"return_value"` — foram criados pelo `return` das funções
- Cada XCom contém um **dict** completo com metadados descritivos
- A soma confere: 850 + 1200 + 2100 = 4150 registros unificados
- Após validação: 4150 × 0.98 = 4067 aprovados

**Verificar via terminal:**
```bash
docker exec airflow-scheduler airflow tasks test dataflow_vendas_paralelo_v1 unificar_dados 2024-01-15
```

**Resultado parcial esperado:**
```
...
🔗 UNIFICAÇÃO DE DADOS
==================================================
Parceiro A: 850 registros (csv)
Parceiro B: 1200 registros (json)
Parceiro C: 2100 registros (parquet)

Normalizando schemas...
✅ Unificação concluída: 4150 registros totais
```

> **Marina:** "Veja como o `unificar_dados` consegue orquestrar os resultados de 3 tasks independentes. Esse padrão é a base de qualquer pipeline de integração de dados — coletar de múltiplas fontes e consolidar em uma visão única."

---

## Passo 11: Verificar os Logs do BashOperator

**Descrição:** Vamos confirmar que o BashOperator executou corretamente e que as template variables (`{{ ds }}`, `{{ run_id }}`) foram substituídas pelos valores reais.

**Passos no navegador:**

1. Na aba **"Graph"**, clique na task **"notificar_conclusao"**
2. Clique em **"Log"**

**Log esperado:**

```
[2024-xx-xx] {taskinstance.py} INFO - Starting attempt 1 of 2
[2024-xx-xx] {subprocess.py} INFO - Running command: ['bash', '-c', ...]
[2024-xx-xx] {subprocess.py} INFO - Output:
==============================================
📧 NOTIFICAÇÃO — Pipeline Concluído
==============================================
DAG: dataflow_vendas_paralelo_v1
Data de execução: 2024-03-15
Run ID: manual__2024-03-15T10:30:00+00:00
Horário: 2024-03-15 10:30:05
Status: SUCESSO ✅
==============================================
Enviando alerta para equipe...
✅ Notificação enviada com sucesso!
[2024-xx-xx] {taskinstance.py} INFO - Marking task as SUCCESS
```

**O que observar:**

- `{{ ds }}` foi substituído por `2024-03-15` (a data de execução real)
- `{{ run_id }}` foi substituído pelo ID único da execução
- `$(date ...)` é um comando bash real — executou no momento da task
- O BashOperator retornou código 0 (sucesso) — por isso a task ficou verde

> **Carlos:** "Em produção, substituiríamos o `echo` por algo como `curl -X POST https://hooks.slack.com/... -d '{"text": "Pipeline concluído"}'` para enviar uma mensagem real no canal da equipe. A estrutura é a mesma — só muda o comando."

---

## Passo 12: Explorar Variações de Dependência

**Descrição:** Agora que você entendeu o básico, vamos ver outras formas de expressar dependências no Airflow. Não precisa executar — é para referência.

**Variações de sintaxe:**

```python
# O que usamos (mais comum):
[extrair_a, extrair_b, extrair_c] >> unificar

# Equivalente expandido:
extrair_a >> unificar
extrair_b >> unificar
extrair_c >> unificar

# Usando set_downstream:
extrair_a.set_downstream(unificar)
extrair_b.set_downstream(unificar)
extrair_c.set_downstream(unificar)

# Fan-out (uma task dispara várias):
inicio >> [extrair_a, extrair_b, extrair_c]

# Combinando fan-out e fan-in em uma linha:
inicio >> [extrair_a, extrair_b, extrair_c] >> unificar
```

**Padrões avançados (preview — veremos na Aula 5):**

```python
# Dependência condicional (apenas uma branch executa):
branch_task >> [caminho_a, caminho_b]  # BranchPythonOperator decide qual

# Trigger rules (task executa mesmo se upstream falhou):
task_cleanup = PythonOperator(
    task_id="cleanup",
    python_callable=limpar,
    trigger_rule="all_done",  # Executa independente de sucesso/falha
)

# Cross-dependencies entre listas:
from airflow.models.baseoperator import cross_downstream
cross_downstream([t1, t2], [t3, t4])  # t1,t2 antes de t3,t4
```

> **Marina:** "A sintaxe com listas (`[]`) e `>>` é a forma mais Pythônica e legível. Use sempre que possível. Os métodos `set_downstream` e `set_upstream` existem para casos muito específicos ou geração dinâmica de DAGs."

---

## Resumo do Exercício

Neste exercício você expandiu suas habilidades com Airflow, implementando paralelismo e comunicação avançada entre tasks.

### Conceitos Novos Aprendidos

| Conceito | Descrição | Sintaxe |
|----------|-----------|---------|
| **Fan-out** | Uma task dispara várias em paralelo | `A >> [B, C, D]` |
| **Fan-in** | Múltiplas tasks convergem para uma | `[B, C, D] >> E` |
| **XCom com return** | Retorno da função salvo automaticamente | `return {"key": "value"}` |
| **xcom_pull múltiplo** | Buscar dados de várias tasks | `ti.xcom_pull(task_ids="task_x")` |
| **BashOperator** | Executar comando shell como task | `BashOperator(bash_command=...)` |
| **Template variables** | Variáveis dinâmicas do Airflow | `{{ ds }}`, `{{ run_id }}` |

### Comparação: Exercício 1 vs Exercício 2

| Aspecto | Exercício 1 | Exercício 2 |
|---------|-------------|-------------|
| Tasks | 3 (sequenciais) | 7 (3 paralelas + 4 sequenciais) |
| Padrão | Linear (A→B→C) | Fan-in + Linear |
| XCom | Push/Pull explícito | Return automático |
| Operadores | PythonOperator | PythonOperator + BashOperator |
| Tempo simulado | Instantâneo | ~2s por extração (paralelo) |
| DAG ID | `dataflow_vendas_diarias_v1` | `dataflow_vendas_paralelo_v1` |

### Fluxo de Dados Completo

```
extrair_parceiro_a ──return {"A", 850}──┐
                                         │
extrair_parceiro_b ──return {"B", 1200}──┼──▶ unificar_dados (pull de 3 tasks)
                                         │         │
extrair_parceiro_c ──return {"C", 2100}──┘         │ return {total: 4150}
                                                    ▼
                                            validar_qualidade
                                                    │ return {aprovados: 4067}
                                                    ▼
                                            carregar_datalake
                                                    │ return {carregados: 4067}
                                                    ▼
                                            notificar_conclusao (BashOperator)
                                                    │ echo "{{ ds }}"
                                                    ▼
                                                  FIM ✅
```

### Arquivo Criado

| Arquivo | Localização | Função |
|---------|-------------|--------|
| `dag_vendas_paralelo.py` | `aula_04/code/dags/` | DAG com paralelismo e XComs avançados |

### Boas Práticas Demonstradas

1. **Use `return` para XComs simples** — mais limpo que `xcom_push` explícito
2. **Retorne dicts descritivos** — facilita debugging e extensibilidade
3. **Adicione `time.sleep()` apenas em labs** — nunca em produção
4. **Combine operadores** — PythonOperator para lógica, BashOperator para comandos shell
5. **Nomeie tasks de forma clara** — `extrair_parceiro_a` é melhor que `task_1`
6. **Docstring nas funções** — aparecem como tooltip no Airflow UI

> **Carlos:** "Com fan-out/fan-in e XComs por return, você tem as ferramentas para modelar a maioria dos pipelines de dados reais. A DataFlow processa dados de dezenas de parceiros usando exatamente esse padrão — cada parceiro tem sua task de extração, todas convergem para uma unificação central."

> **Ana:** "Perfeito! Os relatórios agora chegam 10 minutos mais cedo toda manhã. Próximo passo: quero que o pipeline seja inteligente o suficiente para lidar com feriados e dias sem dados. Dá para fazer?"

> **Carlos:** "Dá sim — com schedule_interval dinâmico e template variables. Mas isso fica para o próximo exercício!"

---

## Próximo Exercício

➡️ **Exercício 3 — BashOperator e Template Variables** (`03_bashoperator_templates.md`): explorar BashOperator para tarefas auxiliares, schedule_interval com expressões cron, e template variables (`{{ ds }}`, `{{ macros }}`) para DAGs conscientes de data.
