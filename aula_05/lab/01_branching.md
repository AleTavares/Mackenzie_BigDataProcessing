# Exercício 1 — BranchPythonOperator: Processamento Adaptativo

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Ana Rodrigues (Product Owner):** "Carlos, temos um problema. Nos dias de promoção, o volume de vendas explode — passa de 1 milhão de registros. Nesses dias, o processamento com Python puro demora horas. Mas nos dias normais, com 200 mil registros, usar Spark é matar formiga com canhão. Precisamos de um pipeline inteligente que escolha automaticamente o caminho certo."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Perfeito, Ana. O Airflow tem exatamente o que precisamos: o `BranchPythonOperator`. Ele funciona como um 'if/else' dentro da DAG — uma função Python analisa o cenário e retorna o nome da task que deve ser executada. As outras são automaticamente puladas."

> **Marina Silva (CTO):** "Isso é orquestração inteligente. O pipeline se adapta ao contexto sem intervenção humana. É a diferença entre um pipeline rígido e um pipeline resiliente. Vamos implementar."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender o conceito de branching em DAGs (decisão condicional)
- Criar uma DAG com `BranchPythonOperator` que escolhe entre dois caminhos
- Compreender por que tasks não escolhidas recebem status "skipped"
- Usar `trigger_rule="none_failed_min_one_success"` para convergência após branching
- Testar ambos os caminhos alterando o volume de dados
- Retornar uma lista de task_ids para ativar múltiplos caminhos simultaneamente

## Pré-requisitos

- Aula 04 concluída (DAGs básicas, PythonOperator, XComs, dependências)
- Ambiente Airflow rodando (ver `aula_04/lab/00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Login no Airflow com admin/admin

## O que vamos construir?

Um pipeline adaptativo que decide automaticamente como processar os dados de vendas:

```
                                    ┌───────────────────────┐
                                ┌──▶│ processar_spark       │──┐
┌───────────────┐    ┌────────┐│   │ (volume > 1M registros)│  │   ┌─────────────────┐
│ contar_registros│──▶│ decidir ││   └───────────────────────┘  ├──▶│ gerar_relatorio │
└───────────────┘    └────────┘│                               │   └─────────────────┘
                               │   ┌───────────────────────┐  │
                               └──▶│ processar_python      │──┘
                                   │ (volume ≤ 1M registros)│
                                   └───────────────────────┘
```

**Comportamento esperado:**

| Volume de dados | Caminho escolhido | Task pulada |
|----------------|-------------------|-------------|
| > 1.000.000 registros | `processar_spark` (distribuído) | `processar_python` → skipped |
| ≤ 1.000.000 registros | `processar_python` (leve) | `processar_spark` → skipped |

**Conceitos-chave deste exercício:**

| Conceito | O que é | Analogia |
|----------|---------|----------|
| **BranchPythonOperator** | Operador que retorna o task_id do caminho a seguir | Semáforo inteligente |
| **Branching** | Divisão condicional do fluxo da DAG | If/else no pipeline |
| **Skipped** | Status das tasks que não foram escolhidas pelo branch | Desvio na estrada |
| **trigger_rule** | Regra que define quando uma task downstream pode executar | Condição de entrada |

---

## Passo 1: Entender o Conceito de Branching

**Descrição:** Em pipelines reais, nem sempre todas as tasks devem executar. Às vezes, o fluxo precisa tomar decisões baseadas em dados, horário, dia da semana, ou qualquer outra condição. O `BranchPythonOperator` resolve isso.

**Como funciona:**

```
┌─────────────────────────────────────────────────────────────────┐
│  BranchPythonOperator                                            │
│                                                                  │
│  1. Executa uma função Python                                    │
│  2. A função RETORNA um task_id (string) ou lista de task_ids    │
│  3. Apenas a(s) task(s) retornada(s) será(ão) executada(s)       │
│  4. Todas as outras tasks downstream são marcadas como SKIPPED   │
└─────────────────────────────────────────────────────────────────┘
```

**Diferença entre PythonOperator e BranchPythonOperator:**

| Aspecto | PythonOperator | BranchPythonOperator |
|---------|---------------|---------------------|
| O que o `return` faz | Salva como XCom (dados) | **Decide o caminho** da DAG |
| Valor retornado | Qualquer tipo (int, dict, str) | Deve ser um `task_id` válido ou lista |
| Efeito no fluxo | Nenhum — próxima task sempre executa | Tasks não escolhidas são puladas |
| Uso típico | Processar dados | Tomar decisões |

**Regras do BranchPythonOperator:**

1. A função **deve retornar** um `task_id` (string) ou uma lista de `task_ids`
2. O `task_id` retornado deve ser de uma task **imediatamente downstream**
3. Tasks que não foram retornadas recebem status `skipped` (rosa/cinza no UI)
4. Tasks downstream das tasks puladas **também são puladas** (propagação)

> **Carlos:** "Pense no BranchPythonOperator como um guarda de trânsito inteligente. Ele olha a situação (volume de dados, dia da semana, status de um sistema) e decide qual caminho liberar. Os outros caminhos ficam bloqueados — não porque falharam, mas porque não eram necessários."

---

## Passo 2: Entender o Problema da Convergência

**Descrição:** Quando uma DAG tem branching, surge um problema: como fazer uma task downstream executar se uma das tasks anteriores foi pulada? Por padrão, uma task só executa se **todas** as upstream tiveram sucesso. Com branching, isso nunca acontece — sempre haverá pelo menos uma task com status `skipped`.

**O problema visual:**

```
                    ┌──────────────────┐
                ┌──▶│ processar_spark  │──┐
┌────────┐     │   └──────────────────┘  │    ┌─────────────────┐
│ decidir│─────┤                          ├───▶│ gerar_relatorio │ ← Nunca executa!
└────────┘     │   ┌──────────────────┐  │    └─────────────────┘
                └──▶│ processar_python │──┘
                    └──────────────────┘

Se o branch escolhe "processar_spark":
  - processar_spark → success ✅
  - processar_python → skipped ⏭️
  - gerar_relatorio → NÃO EXECUTA! (trigger_rule padrão = all_success)
```

**A solução: `trigger_rule`**

O parâmetro `trigger_rule` define **quando** uma task pode ser acionada. O padrão é `"all_success"`, mas para convergência após branching usamos:

| trigger_rule | Comportamento | Quando usar |
|-------------|--------------|-------------|
| `all_success` | Todas as upstream devem ter sucesso | Padrão (sem branching) |
| `none_failed_min_one_success` | Nenhuma falhou E pelo menos uma teve sucesso | **Convergência após branch** ✅ |
| `one_success` | Basta uma upstream ter sucesso | Quando qualquer caminho serve |
| `all_done` | Todas terminaram (sucesso, falha ou skip) | Cleanup/notificação final |
| `none_failed` | Nenhuma falhou (permite skipped) | Similar ao anterior |

**Com `trigger_rule="none_failed_min_one_success"`:**

```
Se o branch escolhe "processar_spark":
  - processar_spark → success ✅
  - processar_python → skipped ⏭️ (não conta como falha!)
  - gerar_relatorio → EXECUTA! ✅ (nenhuma falhou + pelo menos uma teve sucesso)
```

> **Marina:** "O `none_failed_min_one_success` é a regra padrão para convergência após branching. O nome é verboso mas autoexplicativo: 'nenhuma task upstream falhou' AND 'pelo menos uma teve sucesso'. Tasks puladas não contam como falha — é exatamente o que precisamos."

---

## Passo 3: Criar a DAG com Branching

**Descrição:** Vamos criar o arquivo completo da DAG de processamento adaptativo. A função de decisão verifica o volume de registros e retorna o `task_id` do caminho apropriado.

**Comando:** Execute o bloco completo abaixo:

```bash
cat > aula_05/code/dags/dag_branching_processamento.py << 'EOF'
"""
DAG: Pipeline de Processamento Adaptativo da DataFlow Analytics
================================================================
Descrição: Conta registros de vendas do dia e decide automaticamente
           se processa com Spark (alto volume) ou Python (baixo volume).
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: BranchPythonOperator com convergência via trigger_rule
Schedule: Todo dia às 7h (após as extrações)

Estrutura:
    contar_registros → decidir_processamento → processar_spark  ─┐
                                             → processar_python ─┼→ gerar_relatorio
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random


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
# 2. FUNÇÕES — Lógica de negócio
# ============================================================
def contar_registros(**context):
    """
    Conta o volume de registros de vendas do dia.
    Em produção, faria uma query COUNT(*) no banco ou verificaria
    o tamanho do arquivo de entrada.

    Para o lab, simula volumes variados para testar ambos os caminhos.
    """
    execution_date = context["ds"]

    print(f"=" * 50)
    print(f"📊 CONTAGEM DE REGISTROS")
    print(f"=" * 50)
    print(f"Data de referência: {execution_date}")
    print(f"Verificando volume de vendas do dia...")

    # Simula volume — alterne entre estes valores para testar:
    # volume = 1_500_000   # Alto volume → Spark
    # volume = 500_000     # Baixo volume → Python
    volume = random.choice([1_500_000, 500_000, 2_000_000, 300_000])

    print(f"✅ Volume detectado: {volume:,} registros")
    print(f"   Limiar de decisão: 1.000.000 registros")

    if volume > 1_000_000:
        print(f"   → Volume ALTO: processamento distribuído recomendado")
    else:
        print(f"   → Volume NORMAL: processamento leve suficiente")

    # Envia volume via XCom para a função de decisão
    context["ti"].xcom_push(key="volume_registros", value=volume)
    return volume

EOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado parcialmente, continuaremos no próximo passo)
```

> **💡 Nota:** Usamos `random.choice()` para que cada execução possa tomar um caminho diferente. Isso facilita testar ambos os branches sem editar o código. Em produção, o volume viria de uma query real.

---

## Passo 4: A Função de Decisão (Branch)

**Descrição:** Esta é a função mais importante — ela analisa o volume e retorna o `task_id` do caminho a seguir. O valor retornado **deve corresponder exatamente** ao `task_id` de uma task downstream.

**Adicione ao arquivo** (continuação do `cat` anterior — na prática, faz parte do mesmo arquivo):

```python
def decidir_processamento(**context):
    """
    FUNÇÃO DE BRANCHING — Decide qual caminho o pipeline deve seguir.

    REGRA DE NEGÓCIO (definida pela Ana):
    - Se volume > 1.000.000 registros → processar com Spark (distribuído)
    - Se volume ≤ 1.000.000 registros → processar com Python (mais leve)

    IMPORTANTE: Esta função DEVE retornar um task_id válido (string)
    ou uma lista de task_ids. O Airflow executa apenas o(s) retornado(s).
    """
    ti = context["ti"]

    # Recupera o volume contado pela task anterior
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"=" * 50)
    print(f"🔀 DECISÃO DE PROCESSAMENTO")
    print(f"=" * 50)
    print(f"Volume detectado: {volume:,} registros")
    print(f"Limiar: 1.000.000 registros")

    if volume > 1_000_000:
        decisao = "processar_spark"
        print(f"")
        print(f"📌 DECISÃO: Processar com SPARK (distribuído)")
        print(f"   Motivo: Volume ({volume:,}) excede o limiar de 1M")
        print(f"   → Task selecionada: '{decisao}'")
        print(f"   → Task pulada: 'processar_python' (será skipped)")
    else:
        decisao = "processar_python"
        print(f"")
        print(f"📌 DECISÃO: Processar com PYTHON (leve)")
        print(f"   Motivo: Volume ({volume:,}) está dentro do limiar de 1M")
        print(f"   → Task selecionada: '{decisao}'")
        print(f"   → Task pulada: 'processar_spark' (será skipped)")

    # RETORNO CRÍTICO: deve ser exatamente o task_id da task downstream!
    return decisao
```

**O que acontece internamente:**

```
BranchPythonOperator executa decidir_processamento()
          │
          ├── return "processar_spark"
          │         │
          │         ├── processar_spark → status: scheduled → running → success
          │         └── processar_python → status: skipped (automaticamente)
          │
          └── return "processar_python"
                    │
                    ├── processar_python → status: scheduled → running → success
                    └── processar_spark → status: skipped (automaticamente)
```

> **Carlos:** "O truque é que o BranchPythonOperator não 'desliga' manualmente as outras tasks — ele simplesmente comunica ao Scheduler qual caminho seguir. O Scheduler então marca todas as tasks dos caminhos não escolhidos como `skipped`. É elegante e eficiente."

---

## Passo 5: As Funções de Processamento (os dois caminhos)

**Descrição:** Cada caminho do branch implementa uma estratégia diferente de processamento. Em produção, um usaria SparkSubmitOperator e o outro processaria com pandas/Python puro.

**Funções de processamento:**

```python
def processar_spark(**context):
    """
    Processamento distribuído com Apache Spark.
    Usado quando o volume excede 1 milhão de registros.

    Em produção, aqui usaríamos SparkSubmitOperator para enviar
    o job ao cluster Spark. No lab, simulamos com prints.
    """
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"=" * 50)
    print(f"⚡ PROCESSAMENTO DISTRIBUÍDO (SPARK)")
    print(f"=" * 50)
    print(f"Volume a processar: {volume:,} registros")
    print(f"")
    print(f"Inicializando SparkSession...")
    print(f"  - Executors: 4")
    print(f"  - Memory per executor: 2GB")
    print(f"  - Cores per executor: 2")
    print(f"")
    print(f"Etapas de processamento:")
    print(f"  1. Leitura paralela dos dados (4 partições)...")
    print(f"  2. Filtragem e limpeza distribuída...")
    print(f"  3. Agregações com shuffle...")
    print(f"  4. Escrita em Parquet particionado...")
    print(f"")

    registros_processados = int(volume * 0.97)
    print(f"✅ Processamento Spark concluído!")
    print(f"   - Registros processados: {registros_processados:,}")
    print(f"   - Registros rejeitados: {volume - registros_processados:,}")
    print(f"   - Tempo estimado: ~3 minutos (distribuído)")

    return {
        "metodo": "spark",
        "registros_processados": registros_processados,
        "registros_rejeitados": volume - registros_processados,
    }


def processar_python(**context):
    """
    Processamento leve com Python puro (pandas).
    Usado quando o volume é menor que 1 milhão de registros.

    Em produção, usaria pandas ou polars para processamento local.
    Mais rápido de inicializar (sem overhead do Spark).
    """
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"=" * 50)
    print(f"🐍 PROCESSAMENTO LEVE (PYTHON)")
    print(f"=" * 50)
    print(f"Volume a processar: {volume:,} registros")
    print(f"")
    print(f"Carregando dados com pandas...")
    print(f"  - Processo single-node")
    print(f"  - Sem overhead de inicialização de cluster")
    print(f"  - Ideal para volumes < 1M registros")
    print(f"")
    print(f"Etapas de processamento:")
    print(f"  1. pd.read_parquet() — leitura direta...")
    print(f"  2. Filtragem com boolean indexing...")
    print(f"  3. Agregações com groupby()...")
    print(f"  4. df.to_parquet() — escrita local...")
    print(f"")

    registros_processados = int(volume * 0.98)
    print(f"✅ Processamento Python concluído!")
    print(f"   - Registros processados: {registros_processados:,}")
    print(f"   - Registros rejeitados: {volume - registros_processados:,}")
    print(f"   - Tempo estimado: ~1 minuto (local)")

    return {
        "metodo": "python",
        "registros_processados": registros_processados,
        "registros_rejeitados": volume - registros_processados,
    }
```

**Comparação dos dois caminhos:**

| Aspecto | Spark (alto volume) | Python (baixo volume) |
|---------|--------------------|-----------------------|
| Quando usar | > 1M registros | ≤ 1M registros |
| Overhead inicial | Alto (~30s para inicializar) | Baixo (~1s) |
| Paralelismo | 4+ executors simultâneos | Single process |
| Memória | Distribuída (cluster) | Local (1 máquina) |
| Ideal para | Dados massivos | Processamento ágil |

> **Ana:** "Faz total sentido! Nos dias normais (200-500K registros), não precisamos ligar o cluster Spark inteiro. Mas nos dias de promoção (2-3M registros), sem Spark travaria tudo. O pipeline decide sozinho!"

---

## Passo 6: A Função de Convergência e a DAG Completa

**Descrição:** A task `gerar_relatorio` converge os dois caminhos. Ela precisa do `trigger_rule` para funcionar corretamente após o branching.

**Função de convergência + definição completa da DAG:**

```python
def gerar_relatorio(**context):
    """
    Gera relatório consolidado independente do caminho escolhido.
    Usa trigger_rule="none_failed_min_one_success" para executar
    mesmo quando uma das tasks upstream foi pulada (skipped).
    """
    ti = context["ti"]

    # Tenta puxar XCom de ambas as tasks — apenas uma terá valor
    resultado_spark = ti.xcom_pull(task_ids="processar_spark")
    resultado_python = ti.xcom_pull(task_ids="processar_python")

    # Identifica qual caminho foi executado
    resultado = resultado_spark if resultado_spark else resultado_python

    print(f"=" * 50)
    print(f"📋 GERAÇÃO DE RELATÓRIO")
    print(f"=" * 50)
    print(f"Método utilizado: {resultado['metodo'].upper()}")
    print(f"Registros processados: {resultado['registros_processados']:,}")
    print(f"Registros rejeitados: {resultado['registros_rejeitados']:,}")
    print(f"")
    print(f"Gerando relatório diário...")
    print(f"✅ Relatório salvo em: relatorios/diario_{context['ds']}.pdf")

    return {
        "relatorio_gerado": True,
        "metodo_utilizado": resultado["metodo"],
        "total_processado": resultado["registros_processados"],
    }


# ============================================================
# 3. DAG — Pipeline de Processamento Adaptativo
# ============================================================
with DAG(
    dag_id="dataflow_branching_processamento_v1",
    default_args=default_args,
    description="Pipeline adaptativo: Spark ou Python baseado no volume de dados",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "branching", "adaptativo", "aula05"],
) as dag:

    # ========================================================
    # 4. TASKS
    # ========================================================

    # Task 1: Conta registros do dia
    contar = PythonOperator(
        task_id="contar_registros",
        python_callable=contar_registros,
    )

    # Task 2: BRANCH — Decide o caminho (BranchPythonOperator!)
    decidir = BranchPythonOperator(
        task_id="decidir_processamento",
        python_callable=decidir_processamento,
    )

    # Task 3a: Caminho Spark (alto volume)
    spark = PythonOperator(
        task_id="processar_spark",
        python_callable=processar_spark,
    )

    # Task 3b: Caminho Python (baixo volume)
    python_proc = PythonOperator(
        task_id="processar_python",
        python_callable=processar_python,
    )

    # Task 4: Convergência — TRIGGER RULE é obrigatório aqui!
    relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
        trigger_rule="none_failed_min_one_success",  # ← ESSENCIAL!
    )

    # ========================================================
    # 5. DEPENDÊNCIAS
    # ========================================================
    contar >> decidir >> [spark, python_proc] >> relatorio
```

**Agora, crie o arquivo completo de uma vez:**

```bash
cat > aula_05/code/dags/dag_branching_processamento.py << 'DAGEOF'
"""
DAG: Pipeline de Processamento Adaptativo da DataFlow Analytics
================================================================
Descrição: Conta registros de vendas do dia e decide automaticamente
           se processa com Spark (alto volume) ou Python (baixo volume).
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: BranchPythonOperator com convergência via trigger_rule
Schedule: Todo dia às 7h (após as extrações)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import random


default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def contar_registros(**context):
    """Conta o volume de registros de vendas do dia."""
    execution_date = context["ds"]
    print(f"{'=' * 50}")
    print(f"📊 CONTAGEM DE REGISTROS")
    print(f"{'=' * 50}")
    print(f"Data de referência: {execution_date}")
    print(f"Verificando volume de vendas do dia...")

    # Simula volume variado para testar ambos os caminhos
    volume = random.choice([1_500_000, 500_000, 2_000_000, 300_000])

    print(f"✅ Volume detectado: {volume:,} registros")
    print(f"   Limiar de decisão: 1.000.000 registros")

    if volume > 1_000_000:
        print(f"   → Volume ALTO: processamento distribuído recomendado")
    else:
        print(f"   → Volume NORMAL: processamento leve suficiente")

    context["ti"].xcom_push(key="volume_registros", value=volume)
    return volume


def decidir_processamento(**context):
    """
    FUNÇÃO DE BRANCHING — Retorna o task_id do caminho escolhido.
    - volume > 1M → "processar_spark"
    - volume ≤ 1M → "processar_python"
    """
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"{'=' * 50}")
    print(f"🔀 DECISÃO DE PROCESSAMENTO")
    print(f"{'=' * 50}")
    print(f"Volume detectado: {volume:,} registros")
    print(f"Limiar: 1.000.000 registros")

    if volume > 1_000_000:
        decisao = "processar_spark"
        print(f"\n📌 DECISÃO: Processar com SPARK (distribuído)")
        print(f"   Motivo: Volume ({volume:,}) excede 1M")
    else:
        decisao = "processar_python"
        print(f"\n📌 DECISÃO: Processar com PYTHON (leve)")
        print(f"   Motivo: Volume ({volume:,}) está dentro do limiar")

    print(f"   → Task selecionada: '{decisao}'")
    return decisao


def processar_spark(**context):
    """Processamento distribuído com Apache Spark."""
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"{'=' * 50}")
    print(f"⚡ PROCESSAMENTO DISTRIBUÍDO (SPARK)")
    print(f"{'=' * 50}")
    print(f"Volume: {volume:,} registros")
    print(f"Inicializando SparkSession (4 executors, 2GB RAM cada)...")
    print(f"Processando em paralelo...")

    registros_processados = int(volume * 0.97)
    print(f"✅ Spark concluído: {registros_processados:,} registros processados")

    return {"metodo": "spark", "registros_processados": registros_processados,
            "registros_rejeitados": volume - registros_processados}


def processar_python(**context):
    """Processamento leve com Python/pandas."""
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"{'=' * 50}")
    print(f"🐍 PROCESSAMENTO LEVE (PYTHON)")
    print(f"{'=' * 50}")
    print(f"Volume: {volume:,} registros")
    print(f"Carregando com pandas (single-node, sem overhead)...")
    print(f"Processando localmente...")

    registros_processados = int(volume * 0.98)
    print(f"✅ Python concluído: {registros_processados:,} registros processados")

    return {"metodo": "python", "registros_processados": registros_processados,
            "registros_rejeitados": volume - registros_processados}


def gerar_relatorio(**context):
    """Gera relatório consolidado (convergência dos dois caminhos)."""
    ti = context["ti"]
    resultado_spark = ti.xcom_pull(task_ids="processar_spark")
    resultado_python = ti.xcom_pull(task_ids="processar_python")
    resultado = resultado_spark if resultado_spark else resultado_python

    print(f"{'=' * 50}")
    print(f"📋 GERAÇÃO DE RELATÓRIO")
    print(f"{'=' * 50}")
    print(f"Método utilizado: {resultado['metodo'].upper()}")
    print(f"Registros processados: {resultado['registros_processados']:,}")
    print(f"✅ Relatório gerado com sucesso!")

    return {"relatorio_gerado": True, "metodo_utilizado": resultado["metodo"]}


# ============================================================
# DAG
# ============================================================
with DAG(
    dag_id="dataflow_branching_processamento_v1",
    default_args=default_args,
    description="Pipeline adaptativo: Spark ou Python baseado no volume",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "branching", "adaptativo", "aula05"],
) as dag:

    contar = PythonOperator(
        task_id="contar_registros",
        python_callable=contar_registros,
    )

    decidir = BranchPythonOperator(
        task_id="decidir_processamento",
        python_callable=decidir_processamento,
    )

    spark = PythonOperator(
        task_id="processar_spark",
        python_callable=processar_spark,
    )

    python_proc = PythonOperator(
        task_id="processar_python",
        python_callable=processar_python,
    )

    relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
        trigger_rule="none_failed_min_one_success",
    )

    # Dependências
    contar >> decidir >> [spark, python_proc] >> relatorio

DAGEOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado com sucesso)
```

**Verificação:**
```bash
ls -la aula_05/code/dags/dag_branching_processamento.py
```

> **Carlos:** "Repare na linha `trigger_rule="none_failed_min_one_success"` na task `relatorio`. Sem ela, o relatório **nunca** seria gerado — porque sempre haverá uma task com status `skipped` no branch. Essa é a pegadinha mais comum com branching no Airflow."

---

## Passo 7: Validar a DAG no Container

**Descrição:** Confirmar que o arquivo não tem erros de sintaxe e que o Airflow consegue carregá-lo.

**Comando:**
```bash
docker exec airflow-scheduler python /opt/airflow/dags/aula_05/dag_branching_processamento.py
```

**Resultado esperado:**
```
(nenhuma saída e nenhum erro — arquivo válido!)
```

**Se aparecer erro**, verifique:
1. `from airflow.operators.python import PythonOperator, BranchPythonOperator` — ambos importados
2. A função `decidir_processamento` retorna uma **string** (não um objeto task)
3. O `task_id` retornado ("processar_spark" ou "processar_python") corresponde **exatamente** ao `task_id` definido nas tasks

**Verificação — listar DAGs:**
```bash
docker exec airflow-scheduler airflow dags list 2>/dev/null | grep branching
```

**Resultado esperado:**
```
dataflow_branching_processamento_v1 | /opt/airflow/dags/aula_05/dag_branching_processamento.py | dataflow | False
```

---

## Passo 8: Verificar o Grafo no Airflow UI

**Descrição:** Vamos confirmar visualmente que o Airflow entendeu a estrutura de branching. O grafo deve mostrar claramente os dois caminhos divergentes.

**Passos no navegador:**

1. Acesse **http://localhost:8081**
2. Localize **"dataflow_branching_processamento_v1"** na lista de DAGs
3. Clique no nome da DAG para abrir os detalhes
4. Clique na aba **"Graph"**

**Resultado esperado — Graph View:**

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Graph                                                                    │
│                                                                           │
│  ┌──────────────────┐     ┌───────────────────────┐                      │
│  │ contar_registros │────▶│ decidir_processamento │                      │
│  └──────────────────┘     └───────────┬───────────┘                      │
│                                       │                                   │
│                           ┌───────────┼───────────┐                      │
│                           │                       │                       │
│                           ▼                       ▼                       │
│               ┌──────────────────┐   ┌──────────────────┐               │
│               │ processar_spark  │   │ processar_python │               │
│               └────────┬─────────┘   └────────┬─────────┘               │
│                        │                       │                          │
│                        └───────────┬───────────┘                         │
│                                    │                                      │
│                                    ▼                                      │
│                        ┌──────────────────┐                              │
│                        │ gerar_relatorio  │                              │
│                        └──────────────────┘                              │
│                                                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

**O que observar:**

- `decidir_processamento` tem **duas setas** saindo (fork/bifurcação)
- `processar_spark` e `processar_python` estão lado a lado (caminhos alternativos)
- `gerar_relatorio` tem **duas setas** entrando (convergência)
- A task `decidir_processamento` tem um ícone diferente (branch) no UI

> **Ana:** "O diagrama deixa super claro: é como uma estrada que bifurca e depois se junta novamente. Qualquer pessoa da equipe consegue entender o que esse pipeline faz só olhando o grafo."

---

## Passo 9: Executar e Observar o Branching em Ação

**Descrição:** Vamos acionar a DAG e observar como o Airflow executa apenas um dos caminhos enquanto marca o outro como `skipped`.

**Passos no navegador:**

1. **Ative a DAG** clicando no toggle:

```
Antes:  ○ dataflow_branching_processamento_v1     (pausada)
Depois: ● dataflow_branching_processamento_v1     (ativa)
```

2. Clique em **"▶ Trigger DAG"**

3. Vá para a aba **"Graph"** e observe a execução:

**Cenário A — Volume alto (> 1M registros):**

```
┌──────────────────┐     ┌───────────────────────┐
│ contar_registros │ 🟢  │ decidir_processamento │ 🟢
└──────────────────┘     └───────────────────────┘

         ┌──────────────────┐    ┌──────────────────┐
         │ processar_spark  │ 🟢 │ processar_python │ 🩷 ← SKIPPED!
         └──────────────────┘    └──────────────────┘

                    ┌──────────────────┐
                    │ gerar_relatorio  │ 🟢
                    └──────────────────┘
```

**Cenário B — Volume baixo (≤ 1M registros):**

```
┌──────────────────┐     ┌───────────────────────┐
│ contar_registros │ 🟢  │ decidir_processamento │ 🟢
└──────────────────┘     └───────────────────────┘

         ┌──────────────────┐    ┌──────────────────┐
         │ processar_spark  │ 🩷 │ processar_python │ 🟢
         └──────────────────┘    └──────────────────┘
                 ↑ SKIPPED!
                    ┌──────────────────┐
                    │ gerar_relatorio  │ 🟢
                    └──────────────────┘
```

> **💡 Cores no Airflow UI:**
> - 🟢 Verde = `success` (executou com sucesso)
> - 🩷 Rosa/Pink = `skipped` (foi pulada pelo branch)
> - O rosa indica que a task **não falhou** — simplesmente não era necessária nesta execução

4. **Verifique os logs** da task `decidir_processamento` — lá está registrado qual decisão foi tomada e por quê.

**Alternativa via terminal:**
```bash
docker exec airflow-scheduler airflow dags trigger dataflow_branching_processamento_v1
```

> **Carlos:** "A cor rosa (skipped) é fundamental para debugging. Se uma task está rosa, significa que o BranchPythonOperator decidiu não executá-la. Não é um erro — é uma decisão inteligente do pipeline."

---

## Passo 10: Testar Ambos os Caminhos

**Descrição:** Como usamos `random.choice()`, cada execução pode escolher um caminho diferente. Vamos executar a DAG algumas vezes para ver ambos os branches funcionando.

**Opção 1 — Múltiplos triggers (via terminal):**

```bash
# Execução 1
docker exec airflow-scheduler airflow dags trigger dataflow_branching_processamento_v1

# Aguarde concluir (~5 segundos), depois execute novamente:
docker exec airflow-scheduler airflow dags trigger dataflow_branching_processamento_v1

# Repita até ver ambos os caminhos executados
docker exec airflow-scheduler airflow dags trigger dataflow_branching_processamento_v1
```

**Opção 2 — Forçar um caminho específico (editar o código):**

Para **garantir** o caminho Spark, edite a função `contar_registros`:
```python
# Substituir a linha do random.choice por:
volume = 1_500_000   # Força caminho Spark
```

Para **garantir** o caminho Python:
```python
volume = 500_000     # Força caminho Python
```

**Verificação — ver histórico de execuções:**

No Airflow UI, clique na aba **"Grid"** (ou "Tree" em versões anteriores):

```
┌─────────────────────────────────────────────────────────────────────┐
│  Grid View — Histórico de Execuções                                  │
├─────────────────────────────────────────────────────────────────────┤
│              │ Run 1   │ Run 2   │ Run 3   │                        │
│──────────────┼─────────┼─────────┼─────────┤                        │
│ contar       │   🟢    │   🟢    │   🟢    │                        │
│ decidir      │   🟢    │   🟢    │   🟢    │                        │
│ proc_spark   │   🟢    │   🩷    │   🟢    │ ← Alterna!            │
│ proc_python  │   🩷    │   🟢    │   🩷    │ ← Alterna!            │
│ relatorio    │   🟢    │   🟢    │   🟢    │ ← Sempre executa!     │
└─────────────────────────────────────────────────────────────────────┘
```

**O que observar:**

- `contar`, `decidir` e `relatorio` são **sempre verdes** (executam em toda run)
- `processar_spark` e `processar_python` **alternam** entre verde e rosa
- Quando um está verde, o outro está rosa — **nunca ambos verdes ou ambos rosa**
- O `gerar_relatorio` sempre executa graças ao `trigger_rule`

> **Marina:** "Esse Grid View é ouro para monitoramento. Em um mês de execuções, você vê o padrão de distribuição: quantos dias usaram Spark vs Python. Se o Spark está sendo usado 90% das vezes, talvez seja hora de aumentar o cluster permanentemente."

---

## Passo 11: Entender a Propagação do "Skipped"

**Descrição:** Um comportamento importante do branching: o status `skipped` se **propaga** para todas as tasks downstream do caminho não escolhido. Isso pode causar surpresas se não for compreendido.

**Exemplo de propagação:**

```
Imagine uma DAG com mais tasks após cada caminho:

decidir → processar_spark → validar_spark → salvar_spark ─┐
        → processar_python → validar_python → salvar_python ─┼→ relatorio
```

Se o branch escolhe Spark:
```
processar_spark   → 🟢 success
validar_spark     → 🟢 success
salvar_spark      → 🟢 success

processar_python  → 🩷 skipped
validar_python    → 🩷 skipped (propagado!)
salvar_python     → 🩷 skipped (propagado!)

relatorio         → Depende do trigger_rule!
```

**A propagação acontece automaticamente** — se uma task é pulada, todas as suas downstream diretas também são puladas (a menos que tenham trigger_rule especial).

**Por isso o `trigger_rule` é essencial na convergência:**

```python
relatorio = PythonOperator(
    task_id="gerar_relatorio",
    python_callable=gerar_relatorio,
    # Sem isso, relatorio NUNCA executaria (sempre tem 1 upstream skipped)
    trigger_rule="none_failed_min_one_success",
)
```

> **Carlos:** "Essa propagação é intencional e útil — garante que tasks que dependem de um resultado específico não executem sem esse resultado. O ponto é: coloque `trigger_rule` apenas na task de **convergência**, não em todas as tasks do caminho."

---

## Passo 12: Branching com Múltiplos Caminhos (Lista de task_ids)

**Descrição:** O `BranchPythonOperator` pode retornar uma **lista** de `task_ids` — ativando múltiplos caminhos simultaneamente. Isso é útil quando a decisão não é binária.

**Cenário:** Nos dias de promoção relâmpago, a Ana quer processar com Spark E enviar um alerta especial ao mesmo tempo.

**Exemplo — retornando lista:**

```python
def decidir_processamento_avancado(**context):
    """
    Versão avançada: pode retornar MÚLTIPLOS task_ids.
    - Volume > 1M + dia de promoção → Spark + Alerta
    - Volume > 1M → apenas Spark
    - Volume ≤ 1M → apenas Python
    """
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")
    
    # Simula verificação de promoção (em produção, consultaria um calendário)
    dia_promocao = context["ds_nodash"][-2:] in ["01", "15"]  # Dias 1 e 15

    if volume > 1_000_000 and dia_promocao:
        # LISTA de task_ids — ambos serão executados!
        print(f"📌 DECISÃO: Spark + Alerta (promoção relâmpago!)")
        return ["processar_spark", "alerta_volume_alto"]
    elif volume > 1_000_000:
        print(f"📌 DECISÃO: Spark (alto volume normal)")
        return "processar_spark"
    else:
        print(f"📌 DECISÃO: Python (volume baixo)")
        return "processar_python"
```

**Comportamento com lista:**

```
return ["processar_spark", "alerta_volume_alto"]

Resultado:
  - processar_spark      → 🟢 success (executada)
  - alerta_volume_alto   → 🟢 success (executada)
  - processar_python     → 🩷 skipped (não na lista)
```

**Diagrama com 3 caminhos:**

```
                    ┌───────────────────┐
                ┌──▶│ processar_spark   │──┐
                │   └───────────────────┘  │
┌────────┐     │   ┌───────────────────┐  │   ┌─────────────────┐
│ decidir│─────┼──▶│ processar_python  │──┼──▶│ gerar_relatorio │
└────────┘     │   └───────────────────┘  │   └─────────────────┘
                │   ┌───────────────────┐  │
                └──▶│ alerta_volume_alto│──┘
                    └───────────────────┘
```

**Regras para retorno em lista:**

| Retorno | Comportamento |
|---------|--------------|
| `"task_a"` | Apenas task_a executa |
| `["task_a"]` | Equivalente ao anterior |
| `["task_a", "task_b"]` | task_a e task_b executam em paralelo |
| `["task_a", "task_b", "task_c"]` | Todas as 3 executam |
| `[]` | **Erro!** Deve retornar pelo menos 1 task_id |

> **Marina:** "O retorno em lista é poderoso para cenários onde a decisão não é mutuamente exclusiva. Por exemplo: 'se é fim de mês, gere tanto o relatório semanal quanto o mensal'. Não se limite ao if/else binário — o BranchPythonOperator suporta qualquer lógica de decisão."

> **💡 Dica:** Não implemente múltiplos caminhos neste exercício — é apenas para compreensão. Usaremos esse padrão no exercício de desafio (Exercício 7).

---

## Passo 13: Verificar os Logs e XComs da Execução

**Descrição:** Vamos inspecionar os logs para confirmar que a lógica de decisão funcionou corretamente e que os XComs foram passados entre as tasks.

**Inspecionar log da task `decidir_processamento`:**

1. Na aba **"Graph"**, clique na task **"decidir_processamento"** (verde)
2. Clique em **"Log"** no painel lateral

**Log esperado (cenário alto volume):**

```
[2024-xx-xx] {taskinstance.py} INFO - Executing <Task(BranchPythonOperator): decidir_processamento>
==================================================
🔀 DECISÃO DE PROCESSAMENTO
==================================================
Volume detectado: 1,500,000 registros
Limiar: 1.000.000 registros

📌 DECISÃO: Processar com SPARK (distribuído)
   Motivo: Volume (1,500,000) excede 1M
   → Task selecionada: 'processar_spark'
[2024-xx-xx] {taskinstance.py} INFO - Following branch processar_spark
[2024-xx-xx] {taskinstance.py} INFO - Skipping tasks ['processar_python']
```

**Observe as linhas finais** — o Airflow registra explicitamente:
- "Following branch processar_spark" — qual caminho seguiu
- "Skipping tasks ['processar_python']" — quais tasks foram puladas

**Verificar XComs:**

1. Vá em **Admin → XComs**
2. Filtre por DAG: `dataflow_branching_processamento_v1`

**Resultado esperado:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Key              │ Task ID                  │ Value                     │
├───────────────────┼──────────────────────────┼───────────────────────────┤
│ volume_registros  │ contar_registros         │ 1500000                   │
│ return_value      │ contar_registros         │ 1500000                   │
│ return_value      │ decidir_processamento    │ "processar_spark"         │
│ return_value      │ processar_spark          │ {"metodo":"spark",...}     │
│ return_value      │ gerar_relatorio          │ {"relatorio_gerado":true} │
└───────────────────┴──────────────────────────┴───────────────────────────┘
```

**O que observar:**

- `decidir_processamento` tem `return_value = "processar_spark"` — é o task_id retornado
- `processar_python` **não aparece** nos XComs — porque nunca executou (skipped)
- `gerar_relatorio` executou com sucesso — o `trigger_rule` funcionou!

> **Carlos:** "Os logs do BranchPythonOperator são os melhores amigos do debugging. Se o branch está escolhendo o caminho errado, o log mostra exatamente o valor da variável de decisão e qual caminho foi seguido. Sempre comece debugando pelo log do branch."

---

## Resumo do Exercício

### O que aprendemos:

| Conceito | Descrição |
|----------|-----------|
| `BranchPythonOperator` | Operador que retorna task_id para decidir o caminho da DAG |
| Branching condicional | Dividir fluxo baseado em lógica de negócio (volume, data, etc.) |
| Status `skipped` | Tasks não escolhidas pelo branch ficam rosa — não é erro |
| `trigger_rule` | Regra que controla quando uma task downstream pode executar |
| `none_failed_min_one_success` | A regra correta para convergência após branching |
| Retorno em lista | `return ["task_a", "task_b"]` ativa múltiplos caminhos |

### Erros comuns:

| Erro | Causa | Solução |
|------|-------|---------|
| Task de convergência não executa | Falta `trigger_rule` | Adicionar `trigger_rule="none_failed_min_one_success"` |
| `AirflowException: task_id not found` | Retornou task_id inexistente | Verificar se o string retornado corresponde ao task_id exato |
| Todas as tasks puladas | Função de branch retorna `None` | Garantir que a função sempre retorna um task_id válido |
| Branch não pula as tasks | Usou `PythonOperator` em vez de `BranchPythonOperator` | Trocar o operador para BranchPythonOperator |

### Diagrama final do pipeline:

```
┌──────────────────┐     ┌───────────────────────┐     ┌──────────────────┐
│ contar_registros │────▶│ decidir_processamento │──┬─▶│ processar_spark  │──┐
└──────────────────┘     └───────────────────────┘  │  └──────────────────┘  │
                                                    │                         │
                                                    │  ┌──────────────────┐  │  ┌─────────────────┐
                                                    └─▶│ processar_python │──┴─▶│ gerar_relatorio │
                                                       └──────────────────┘     └─────────────────┘
                                                                                trigger_rule=
                                                                                "none_failed_min_one_success"
```

### Próximo exercício:

No **Exercício 2**, vamos aprender a usar `FileSensor` — um operador que **espera** por uma condição externa antes de continuar o pipeline. A Ana precisa que o processamento só inicie quando o arquivo de vendas chegar no servidor de SFTP do parceiro.

---

> **Marina:** "Excelente progresso! Vocês acabaram de implementar o primeiro padrão de orquestração inteligente. O pipeline agora **se adapta** ao contexto sem intervenção humana. Isso é escalabilidade operacional — o sistema toma decisões que antes dependiam de um engenheiro olhando para a tela."
