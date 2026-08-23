# Exercício 3 — TaskGroups: Organização Visual de DAGs Complexas

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, nosso pipeline cresceu para 15 tasks espalhadas por múltiplas fontes de dados. Quando abro a Graph View no Airflow, parece um prato de espaguete. Precisamos organizar isso visualmente — qualquer pessoa da equipe deve entender o pipeline em 5 segundos."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Marina, o Airflow tem exatamente o que precisamos: **TaskGroups**. Eles agrupam tasks relacionadas em blocos colapsáveis na interface gráfica. É como criar pastas para organizar arquivos — a lógica não muda, mas a visualização fica muito mais clara."

> **Marina Silva (CTO):** "Perfeito. Quero ver três grupos claros: ingestão, transformação e camada gold. E se conseguirmos reaproveitar o padrão para cada parceiro novo, melhor ainda."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender por que TaskGroups existem e quando usá-los
- Criar TaskGroups com a sintaxe `with TaskGroup(...) as grupo:`
- Organizar uma DAG com 3 grupos: ingestão, transformação e gold
- Definir dependências entre grupos inteiros
- Entender o namespacing de task_ids (`grupo.task`)
- Visualizar grupos colapsáveis na Graph View do Airflow
- Criar uma factory function para gerar TaskGroups reutilizáveis

## Pré-requisitos

- Exercícios 01 (BranchPythonOperator) e 02 (FileSensor) concluídos
- Ambiente Airflow rodando (ver `aula_04/lab/00_setup.md`)
- Airflow UI acessível em http://localhost:8081

## O que vamos construir?

Um pipeline multi-fonte organizado em 3 TaskGroups que reflete a arquitetura Medallion:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│ DAG: dataflow_taskgroups_multi_fonte_v1                                              │
│                                                                                      │
│  ┌─── ingestao ────────────┐    ┌─── transformacao ──────┐    ┌─── gold ──────────┐ │
│  │                          │    │                        │    │                    │ │
│  │  ingerir_parceiro_a      │    │  limpar_dados          │    │  agregar_vendas    │ │
│  │  ingerir_parceiro_b      │───▶│  normalizar_schema     │───▶│  gerar_relatorio   │ │
│  │  ingerir_parceiro_c      │    │  deduplicar            │    │                    │ │
│  │                          │    │                        │    │                    │ │
│  └──────────────────────────┘    └────────────────────────┘    └────────────────────┘ │
│                                                                                      │
│  ▶ Clique em um grupo para expandir/colapsar na Graph View                           │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Conceitos-chave deste exercício:**

| Conceito | O que é | Analogia |
|----------|---------|----------|
| **TaskGroup** | Agrupamento visual e lógico de tasks | Pasta de arquivos |
| **group_id** | Identificador único do grupo | Nome da pasta |
| **Namespacing** | task_ids prefixados com grupo: `ingestao.parceiro_a` | Caminho completo |
| **Dependência entre grupos** | `grupo_a >> grupo_b` (todas de A antes de todas de B) | Fila de etapas |
| **Factory pattern** | Função que cria TaskGroups reutilizáveis | Template de pasta |

---

## Passo 1: Entender o Problema — DAGs sem Organização

**Descrição:** Quando um pipeline cresce para 10+ tasks, a Graph View do Airflow se torna confusa. TaskGroups resolvem isso agrupando tasks relacionadas em blocos colapsáveis.

**Sem TaskGroups (15 tasks visíveis simultaneamente):**

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ ingerir_a    │  │ ingerir_b    │  │ ingerir_c    │  │ limpar       │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │                 │
       └─────────────────┼─────────────────┘                 │
                         ▼                                   ▼
                  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
                  │ normalizar   │  │ deduplicar   │  │ agregar      │
                  └──────────────┘  └──────────────┘  └──────────────┘
                         ... (mais 8 tasks espalhadas) ...

⚠️ Resultado: confuso, difícil de entender o fluxo geral
```

**Com TaskGroups (3 blocos organizados):**

```
┌─── ingestao ───┐    ┌─── transformacao ───┐    ┌─── gold ───┐
│  (3 tasks)     │───▶│  (3 tasks)          │───▶│  (2 tasks) │
└────────────────┘    └─────────────────────┘    └────────────┘

✅ Resultado: claro, hierárquico, qualquer pessoa entende em 5 segundos
```

**Quando usar TaskGroups:**

| Cenário | Usar TaskGroup? | Por quê |
|---------|----------------|---------|
| DAG com 3-4 tasks simples | ❌ Não | Overhead desnecessário |
| DAG com 8+ tasks em etapas distintas | ✅ Sim | Organização visual |
| Tasks de diferentes fases (ETL) | ✅ Sim | Separação lógica clara |
| Padrão repetitivo por parceiro/fonte | ✅ Sim | Reutilização com factory |

> **Carlos:** "TaskGroups não mudam a lógica de execução — as tasks rodam da mesma forma. A diferença é puramente visual e organizacional. É como refatorar código em funções: o comportamento é igual, mas a legibilidade melhora drasticamente."

---

## Passo 2: Sintaxe Básica do TaskGroup

**Descrição:** O `TaskGroup` usa um context manager (`with`) para agrupar tasks. Qualquer task definida dentro do bloco `with` pertence ao grupo.

**Sintaxe:**

```python
from airflow.utils.task_group import TaskGroup

with DAG(...) as dag:

    # Criar um TaskGroup
    with TaskGroup(group_id="meu_grupo") as grupo:
        # Todas as tasks aqui dentro pertencem ao grupo
        task_a = PythonOperator(task_id="task_a", ...)
        task_b = PythonOperator(task_id="task_b", ...)
        
        # Dependências DENTRO do grupo
        task_a >> task_b

    # Dependências ENTRE grupos
    grupo >> outra_task
```

**Parâmetros do TaskGroup:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `group_id` | str | Identificador único do grupo (obrigatório) |
| `prefix_group_id` | bool | Se True (padrão), prefixa task_ids com o group_id |
| `tooltip` | str | Texto exibido ao passar o mouse sobre o grupo no UI |

**Namespacing automático:**

Quando `prefix_group_id=True` (padrão), os task_ids são automaticamente prefixados:

```python
with TaskGroup(group_id="ingestao") as grupo_ingestao:
    # task_id real: "ingestao.parceiro_a" (não apenas "parceiro_a")
    parceiro_a = PythonOperator(task_id="parceiro_a", ...)

# Para referenciar em XComs ou trigger_rules:
# ti.xcom_pull(task_ids="ingestao.parceiro_a")
```

> **Marina:** "O namespacing é essencial quando você tem tasks com nomes similares em grupos diferentes. Posso ter `ingestao.validar` e `transformacao.validar` sem conflito."

---

## Passo 3: Criar a DAG Completa com TaskGroups

**Descrição:** Vamos criar uma DAG que organiza o pipeline multi-fonte da DataFlow em 3 grupos claros: ingestão (bronze), transformação (silver) e gold.

**Comando — crie o arquivo da DAG:**

```bash
cat > aula_05/code/dags/dag_taskgroups_multi_fonte.py << 'EOF'
"""
DAG: Pipeline Multi-Fonte com TaskGroups — DataFlow Analytics
=============================================================
Descrição: Pipeline organizado em 3 TaskGroups representando
           as camadas da arquitetura Medallion (Bronze/Silver/Gold).
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: Ingestão (3 fontes) → Transformação → Gold (agregações)

Cenário de Negócio:
    - DataFlow recebe dados de 3 parceiros (A, B, C)
    - Cada parceiro envia em formato diferente
    - Pipeline deve ingerir, transformar e agregar
    - TaskGroups organizam visualmente as 8 tasks em 3 blocos
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


# ============================================================
# 1. DEFAULT ARGS
# ============================================================
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# 2. FUNÇÕES DE NEGÓCIO
# ============================================================
def ingerir_fonte(fonte: str, formato: str, **context):
    """Simula ingestão de dados de um parceiro específico."""
    ds = context["ds"]
    print(f"{'=' * 50}")
    print(f"📥 INGESTÃO — {fonte.upper()}")
    print(f"{'=' * 50}")
    print(f"  Fonte: {fonte}")
    print(f"  Formato: {formato}")
    print(f"  Data referência: {ds}")
    print(f"  Destino: datalake/bronze/{fonte}/{ds}/")
    print(f"  ✅ {fonte} ingerido com sucesso!")
    context["ti"].xcom_push(key="registros", value=50000)
EOF
```

**Resultado esperado:**
```
(nenhuma saída — primeira parte do arquivo criada)
```

Agora vamos completar o arquivo com as funções de transformação, gold e a definição da DAG:

```bash
cat >> aula_05/code/dags/dag_taskgroups_multi_fonte.py << 'EOF'


def limpar_dados(**context):
    """Remove nulls e valores inválidos."""
    print("🧹 Limpando dados: removendo nulls e valores negativos...")
    print("   Registros removidos: 2.847 (2.3%)")
    print("   ✅ Dados limpos!")


def normalizar_schema(**context):
    """Normaliza schemas de diferentes fontes para formato único."""
    print("🔄 Normalizando schemas dos 3 parceiros...")
    print("   Colunas padronizadas: order_id, customer_id, amount, date")
    print("   ✅ Schema unificado!")


def deduplicar(**context):
    """Remove registros duplicados."""
    print("🔍 Deduplicando registros...")
    print("   Duplicatas encontradas: 1.523 (1.0%)")
    print("   ✅ Dados deduplicados!")


def agregar_vendas(**context):
    """Agrega vendas por estado e categoria para camada Gold."""
    ds = context["ds"]
    print(f"📊 Agregando vendas para {ds}...")
    print("   Dimensões: estado, categoria, payment_method")
    print("   Destino: datalake/gold/vendas_agregadas/")
    print("   ✅ Camada Gold atualizada!")


def gerar_relatorio(**context):
    """Gera relatório executivo diário."""
    ds = context["ds"]
    print(f"📋 Gerando relatório executivo de {ds}...")
    print("   Faturamento total: R$ 1.247.890,00")
    print("   Top estado: SP (34%)")
    print("   ✅ Relatório disponível em /reports/")


# ============================================================
# 3. DAG COM TASKGROUPS
# ============================================================
with DAG(
    dag_id="dataflow_taskgroups_multi_fonte_v1",
    default_args=default_args,
    description="Pipeline multi-fonte com TaskGroups (Bronze/Silver/Gold)",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "taskgroups", "medallion", "aula05"],
) as dag:

    # ========================================================
    # GRUPO 1: INGESTÃO (Camada Bronze)
    # ========================================================
    with TaskGroup(
        group_id="ingestao",
        tooltip="Ingestão de dados dos 3 parceiros → Bronze"
    ) as grupo_ingestao:

        parceiro_a = PythonOperator(
            task_id="parceiro_a",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": "parceiro_a", "formato": "CSV"},
        )

        parceiro_b = PythonOperator(
            task_id="parceiro_b",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": "parceiro_b", "formato": "JSON"},
        )

        parceiro_c = PythonOperator(
            task_id="parceiro_c",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": "parceiro_c", "formato": "Parquet"},
        )

        # Tasks dentro do grupo são PARALELAS (sem dependência entre si)
        # Os 3 parceiros são ingeridos simultaneamente!

    # ========================================================
    # GRUPO 2: TRANSFORMAÇÃO (Camada Silver)
    # ========================================================
    with TaskGroup(
        group_id="transformacao",
        tooltip="Limpeza, normalização e deduplicação → Silver"
    ) as grupo_transformacao:

        limpar = PythonOperator(
            task_id="limpar",
            python_callable=limpar_dados,
        )

        normalizar = PythonOperator(
            task_id="normalizar",
            python_callable=normalizar_schema,
        )

        dedup = PythonOperator(
            task_id="deduplicar",
            python_callable=deduplicar,
        )

        # Dentro do grupo: dependências sequenciais
        limpar >> normalizar >> dedup

    # ========================================================
    # GRUPO 3: GOLD (Camada Gold)
    # ========================================================
    with TaskGroup(
        group_id="gold",
        tooltip="Agregações de negócio e relatórios → Gold"
    ) as grupo_gold:

        agregar = PythonOperator(
            task_id="agregar_vendas",
            python_callable=agregar_vendas,
        )

        relatorio = PythonOperator(
            task_id="gerar_relatorio",
            python_callable=gerar_relatorio,
        )

        agregar >> relatorio

    # ========================================================
    # DEPENDÊNCIAS ENTRE GRUPOS
    # ========================================================
    # Toda a ingestão deve terminar antes da transformação começar
    # Toda a transformação deve terminar antes do gold começar
    grupo_ingestao >> grupo_transformacao >> grupo_gold

EOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo completo criado)
```

**Verificação:**
```bash
ls -la aula_05/code/dags/dag_taskgroups_multi_fonte.py
```

**O que observar no código:**

1. **3 blocos `with TaskGroup(...)`** — cada um agrupa tasks relacionadas
2. **Dependências internas** — dentro de `transformacao`, as tasks são sequenciais (`limpar >> normalizar >> dedup`)
3. **Tasks paralelas** — dentro de `ingestao`, os 3 parceiros não têm dependência entre si (rodam em paralelo)
4. **Dependência entre grupos** — `grupo_ingestao >> grupo_transformacao >> grupo_gold` encadeia os blocos

---

## Passo 4: Validar e Visualizar no Airflow

**Descrição:** Vamos validar a DAG e observar os TaskGroups na Graph View.

**Validar sintaxe:**

```bash
docker exec airflow-scheduler python /opt/airflow/dags/aula_05/dag_taskgroups_multi_fonte.py
```

**Resultado esperado:**
```
(nenhuma saída e nenhum erro — DAG válida!)
```

**Verificar listagem:**
```bash
docker exec airflow-scheduler airflow dags list 2>/dev/null | grep taskgroups
```

**Resultado esperado:**
```
dataflow_taskgroups_multi_fonte_v1 | /opt/airflow/dags/aula_05/dag_taskgroups_multi_fonte.py | dataflow | False
```

**Visualizar na Graph View:**

1. Acesse **http://localhost:8081**
2. Localize **"dataflow_taskgroups_multi_fonte_v1"**
3. Ative a DAG (toggle)
4. Clique no nome da DAG → aba **"Graph"**

**O que você verá na Graph View (grupos colapsados):**

```
┌─────────────────┐         ┌─────────────────────┐         ┌──────────────────┐
│  ▶ ingestao     │────────▶│  ▶ transformacao     │────────▶│  ▶ gold          │
│    (3 tasks)    │         │    (3 tasks)         │         │    (2 tasks)     │
└─────────────────┘         └─────────────────────┘         └──────────────────┘
```

**Ao clicar em "ingestao" para expandir:**

```
┌─────────────────────────────────────┐
│  ▼ ingestao                          │
│                                      │
│  ┌─────────────┐                     │
│  │ parceiro_a  │                     │         ┌─────────────────────┐
│  └─────────────┘                     │────────▶│  ▶ transformacao     │──▶ ...
│  ┌─────────────┐                     │         └─────────────────────┘
│  │ parceiro_b  │                     │
│  └─────────────┘                     │
│  ┌─────────────┐                     │
│  │ parceiro_c  │                     │
│  └─────────────┘                     │
└─────────────────────────────────────┘
```

> **Marina:** "Agora sim! Em vez de 8 caixas espalhadas, vejo 3 blocos claros: ingestão, transformação, gold. Qualquer novo membro da equipe entende o pipeline imediatamente. E posso expandir qualquer grupo para ver os detalhes."

---

## Passo 5: Entender o Namespacing de Task IDs

**Descrição:** Com `prefix_group_id=True` (padrão), os task_ids são prefixados automaticamente com o `group_id`. Isso é importante para referências em XComs, trigger_rules e logs.

**Task IDs reais na DAG:**

| Definição no código | task_id real (com namespace) |
|--------------------|-----------------------------|
| `task_id="parceiro_a"` dentro de `ingestao` | `ingestao.parceiro_a` |
| `task_id="parceiro_b"` dentro de `ingestao` | `ingestao.parceiro_b` |
| `task_id="parceiro_c"` dentro de `ingestao` | `ingestao.parceiro_c` |
| `task_id="limpar"` dentro de `transformacao` | `transformacao.limpar` |
| `task_id="normalizar"` dentro de `transformacao` | `transformacao.normalizar` |
| `task_id="deduplicar"` dentro de `transformacao` | `transformacao.deduplicar` |
| `task_id="agregar_vendas"` dentro de `gold` | `gold.agregar_vendas` |
| `task_id="gerar_relatorio"` dentro de `gold` | `gold.gerar_relatorio` |

**Impacto prático — XComs com namespacing:**

```python
# ❌ ERRADO — não vai encontrar a task
registros = ti.xcom_pull(task_ids="parceiro_a", key="registros")

# ✅ CORRETO — usa o task_id completo com prefixo do grupo
registros = ti.xcom_pull(task_ids="ingestao.parceiro_a", key="registros")
```

**Verificar task_ids no terminal:**

```bash
docker exec airflow-scheduler airflow tasks list dataflow_taskgroups_multi_fonte_v1
```

**Resultado esperado:**
```
ingestao.parceiro_a
ingestao.parceiro_b
ingestao.parceiro_c
transformacao.limpar
transformacao.normalizar
transformacao.deduplicar
gold.agregar_vendas
gold.gerar_relatorio
```

> **Carlos:** "O ponto (`.`) no task_id é a assinatura de um TaskGroup. Quando você vir `ingestao.parceiro_a` nos logs, sabe imediatamente que essa task pertence ao grupo de ingestão. É o mesmo conceito de namespaces em Python: `modulo.funcao`."

---

## Passo 6: Executar a DAG e Observar a Execução por Grupo

**Descrição:** Vamos acionar a DAG e observar como os grupos executam: primeiro todas as tasks de ingestão (paralelas), depois transformação (sequencial), depois gold (sequencial).

**Passos:**

1. No Airflow UI, clique em **"▶ Trigger DAG"** para `dataflow_taskgroups_multi_fonte_v1`
2. Vá para a aba **"Graph"** e observe a execução em tempo real

**Ordem de execução esperada:**

```
Tempo   │ O que acontece
────────┼──────────────────────────────────────────────────────
t=0     │ ingestao.parceiro_a, ingestao.parceiro_b, ingestao.parceiro_c
        │ (executam EM PARALELO — não há dependência entre eles)
        │
t=1     │ transformacao.limpar (inicia após TODAS as ingestões terminarem)
        │
t=2     │ transformacao.normalizar (após limpar)
        │
t=3     │ transformacao.deduplicar (após normalizar)
        │
t=4     │ gold.agregar_vendas (inicia após TODA transformação terminar)
        │
t=5     │ gold.gerar_relatorio (após agregar)
        │
t=6     │ ✅ DAG concluída!
```

**Na Graph View, observe as cores:**

- 🟢 Verde escuro = `success` (task concluída)
- 🟢 Verde claro = `running` (task executando agora)
- Sem cor = aguardando upstream

**Verificar logs de uma task específica:**

```bash
docker exec airflow-scheduler airflow tasks test \
    dataflow_taskgroups_multi_fonte_v1 \
    ingestao.parceiro_a \
    2024-01-01
```

**Resultado esperado:**
```
==================================================
📥 INGESTÃO — PARCEIRO_A
==================================================
  Fonte: parceiro_a
  Formato: CSV
  Data referência: 2024-01-01
  Destino: datalake/bronze/parceiro_a/2024-01-01/
  ✅ parceiro_a ingerido com sucesso!
```

> **Marina:** "Repare que os 3 parceiros executam em paralelo — exatamente o que queremos para ganhar tempo. E a transformação só começa quando TODOS chegaram. TaskGroups dão essa garantia visual e funcional."

---

## Passo 7: Factory Pattern — TaskGroups Reutilizáveis

**Descrição:** Quando o padrão se repete para cada parceiro (ingerir → validar → salvar), podemos criar uma **função factory** que gera TaskGroups automaticamente. Isso evita código duplicado quando novos parceiros são adicionados.

**Problema sem factory:**

```python
# ❌ Código repetitivo — imagine 10 parceiros!
with TaskGroup(group_id="parceiro_a") as grupo_a:
    ingerir_a = PythonOperator(task_id="ingerir", ...)
    validar_a = PythonOperator(task_id="validar", ...)
    salvar_a = PythonOperator(task_id="salvar", ...)
    ingerir_a >> validar_a >> salvar_a

with TaskGroup(group_id="parceiro_b") as grupo_b:
    ingerir_b = PythonOperator(task_id="ingerir", ...)
    validar_b = PythonOperator(task_id="validar", ...)
    salvar_b = PythonOperator(task_id="salvar", ...)
    ingerir_b >> validar_b >> salvar_b

# ... parceiro_c, parceiro_d, parceiro_e...
```

**Solução com factory:**

```python
def criar_grupo_parceiro(parceiro: str, formato: str, dag: DAG) -> TaskGroup:
    """
    Factory que cria um TaskGroup completo para um parceiro.
    Cada parceiro recebe: ingerir → validar → salvar_bronze
    
    Uso: grupo = criar_grupo_parceiro("parceiro_a", "CSV", dag)
    Task IDs gerados: parceiro_a.ingerir, parceiro_a.validar, parceiro_a.salvar_bronze
    """
    with TaskGroup(group_id=parceiro, dag=dag) as grupo:

        ingerir = PythonOperator(
            task_id="ingerir",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": parceiro, "formato": formato},
        )

        validar = PythonOperator(
            task_id="validar",
            python_callable=lambda fonte=parceiro, **ctx: print(
                f"✔️ Validando schema de {fonte}: OK"
            ),
        )

        salvar = PythonOperator(
            task_id="salvar_bronze",
            python_callable=lambda fonte=parceiro, **ctx: print(
                f"💾 Salvando {fonte} em datalake/bronze/{fonte}/"
            ),
        )

        ingerir >> validar >> salvar

    return grupo
```

**Uso da factory na DAG:**

```python
with DAG(dag_id="dataflow_factory_demo", ...) as dag:

    # Criar TaskGroups para cada parceiro com UMA linha cada
    parceiros = {
        "parceiro_a": "CSV",
        "parceiro_b": "JSON",
        "parceiro_c": "Parquet",
        "parceiro_d": "API",       # Novo parceiro? Uma linha!
        "parceiro_e": "Delta",     # Outro? Mais uma linha!
    }

    grupos = []
    for nome, formato in parceiros.items():
        grupo = criar_grupo_parceiro(nome, formato, dag)
        grupos.append(grupo)

    # Transformação após TODOS os parceiros
    with TaskGroup(group_id="transformacao") as grupo_transform:
        limpar = PythonOperator(task_id="limpar", ...)
        normalizar = PythonOperator(task_id="normalizar", ...)
        limpar >> normalizar

    # Todos os grupos de parceiros → transformação
    for grupo in grupos:
        grupo >> grupo_transform
```

**Resultado na Graph View:**

```
┌── parceiro_a ──┐
│ ingerir→validar│──┐
│ →salvar_bronze │  │
└────────────────┘  │
┌── parceiro_b ──┐  │    ┌─── transformacao ───┐
│ ingerir→validar│──┼───▶│ limpar → normalizar │
│ →salvar_bronze │  │    └─────────────────────┘
└────────────────┘  │
┌── parceiro_c ──┐  │
│ ingerir→validar│──┘
│ →salvar_bronze │
└────────────────┘
```

> **Carlos:** "O factory pattern é poderoso. Quando a DataFlow fechar com um parceiro D, basta adicionar uma entrada no dicionário: `'parceiro_d': 'API'`. Sem copiar e colar código, sem risco de esquecer uma task. É escalabilidade no design do pipeline."

---

## Resumo

| Conceito | Sintaxe | Resultado |
|----------|---------|-----------|
| Criar grupo | `with TaskGroup(group_id="nome") as g:` | Bloco colapsável no UI |
| Tasks paralelas no grupo | Sem dependência entre elas | Executam simultaneamente |
| Tasks sequenciais no grupo | `task_a >> task_b >> task_c` | Executam em ordem |
| Dependência entre grupos | `grupo_a >> grupo_b` | Todas de A antes de B |
| Task ID com namespace | `task_id="parceiro_a"` em grupo `ingestao` | `ingestao.parceiro_a` |
| Factory pattern | Função que retorna TaskGroup | Reutilização para N parceiros |

**TaskGroups vs SubDAGs (histórico):**

| Aspecto | TaskGroup (moderno ✅) | SubDAG (obsoleto ❌) |
|---------|----------------------|---------------------|
| Performance | Mesmo executor da DAG pai | Executor próprio (overhead) |
| Complexidade | Simples (context manager) | Requer DAG factory separada |
| Visibilidade | Inline na Graph View | Abre em tela separada |
| Status | Recomendado (Airflow 2.0+) | Deprecated (Airflow 2.4+) |
| Deadlocks | Não causa | Pode causar com SequentialExecutor |

> **Marina:** "A mensagem final é clara: se sua DAG tem mais de 6-8 tasks, use TaskGroups para organizar. É zero custo de performance e enorme ganho de legibilidade. No próximo exercício, vamos combinar TaskGroups com Branching e Sensors para criar um pipeline verdadeiramente profissional."

---

## ✅ Checklist de Conclusão

- [ ] Entendi que TaskGroups são agrupamentos visuais (não mudam a lógica)
- [ ] Criei a DAG `dag_taskgroups_multi_fonte.py` com 3 TaskGroups
- [ ] Visualizei os grupos colapsáveis na Graph View do Airflow
- [ ] Entendi o namespacing: `grupo.task_id`
- [ ] Executei a DAG e observei a ordem: ingestão (paralela) → transformação (sequencial) → gold
- [ ] Compreendi o factory pattern para TaskGroups reutilizáveis
