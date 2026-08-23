# Exercício 2 — FileSensor: Esperando Dados dos Parceiros

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Ana Rodrigues (Product Owner):** "Carlos, temos um problema sério. O parceiro A entrega os dados de vendas entre 6h e 6h30, e o parceiro B entre 7h e 7h30. Nosso pipeline está programado para rodar às 6h, mas frequentemente falha porque o arquivo ainda não chegou. Não podemos começar a processar sem os dados!"

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Ana, isso é um problema clássico de dependência externa. O Airflow tem exatamente o que precisamos: **Sensors**. São operadores especiais que ficam 'esperando' uma condição ser satisfeita antes de liberar o pipeline. No nosso caso, vamos usar o `FileSensor` — ele verifica periodicamente se o arquivo existe e só libera a próxima task quando encontrar."

> **Marina Silva (CTO):** "Isso é maturidade operacional. Em vez de falhar e precisar de reexecução manual, o pipeline espera pacientemente pelo dado. É a diferença entre um pipeline frágil e um pipeline resiliente. Vamos implementar."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender o conceito de Sensor: operador que faz polling até uma condição ser satisfeita
- Configurar `FileSensor` com seus parâmetros principais (filepath, poke_interval, timeout, mode)
- Criar uma DAG com sensor → processamento → notificação
- Simular a chegada de um arquivo e observar o sensor transicionar de "sensing" para "success"
- Diferenciar `mode="poke"` vs `mode="reschedule"` e quando usar cada um
- Usar `soft_fail=True` para arquivos opcionais (timeout vira skip em vez de falha)
- Aplicar `{{ ds_nodash }}` no filepath para sensores date-aware

## Pré-requisitos

- Exercício 01 (BranchPythonOperator) concluído
- Ambiente Airflow rodando (ver `aula_04/lab/00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Login no Airflow com admin/admin

## O que vamos construir?

Um pipeline que espera o arquivo de vendas do parceiro chegar antes de processar:

```
┌─────────────────────────┐     ┌─────────────────────────┐     ┌─────────────────────┐
│ esperar_arquivo_vendas  │────▶│ processar_vendas        │────▶│ notificar_conclusao │
│ (FileSensor - polling)  │     │ (PythonOperator)        │     │ (BashOperator)      │
└─────────────────────────┘     └─────────────────────────┘     └─────────────────────┘
         │                                                               
         │ polling a cada 30s                                            
         │ timeout: 10 minutos                                           
         ▼                                                               
   Arquivo existe? ──NO──▶ Espera 30s → Verifica novamente              
         │                                                               
        YES                                                              
         │                                                               
         ▼                                                               
   Libera próxima task!                                                  
```

**Comportamento esperado:**

| Situação | Comportamento do Sensor | Status final |
|----------|------------------------|--------------|
| Arquivo já existe quando o sensor inicia | Libera imediatamente | 🟢 success |
| Arquivo chega após 2 minutos | Faz ~4 checks, depois libera | 🟢 success |
| Arquivo não chega em 10 minutos | Timeout! | 🔴 failed (ou 🩷 skipped com soft_fail) |

**Conceitos-chave deste exercício:**

| Conceito | O que é | Analogia |
|----------|---------|----------|
| **Sensor** | Operador que faz polling até condição ser verdadeira | Porteiro que verifica documento |
| **FileSensor** | Sensor específico para existência de arquivo | Alarme de "arquivo chegou" |
| **poke_interval** | Intervalo entre verificações (segundos) | "Verificar a cada X segundos" |
| **timeout** | Tempo máximo de espera antes de falhar | "Desistir após X minutos" |
| **mode** | Estratégia de espera (poke vs reschedule) | "Ficar na fila" vs "voltar depois" |

---

## Passo 1: Entender o Conceito de Sensor

**Descrição:** Um Sensor é um tipo especial de operador no Airflow. Enquanto operadores normais (PythonOperator, BashOperator) executam uma ação e terminam, o Sensor **espera** — ele fica verificando periodicamente se uma condição é verdadeira antes de liberar as tasks downstream.

**Como funciona o ciclo de um Sensor:**

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Ciclo de vida de um Sensor                                              │
│                                                                          │
│  1. Sensor inicia → executa poke() pela primeira vez                     │
│  2. poke() retorna True? → Sensor termina com SUCCESS ✅                 │
│  3. poke() retorna False? → Espera poke_interval segundos                │
│  4. Repete do passo 2                                                    │
│  5. Se tempo total > timeout → Sensor termina com FAILED 🔴              │
└─────────────────────────────────────────────────────────────────────────┘
```

**Diferença entre Operator e Sensor:**

| Aspecto | PythonOperator | FileSensor |
|---------|---------------|------------|
| Objetivo | Executar uma ação | Esperar uma condição |
| Duração | Rápida (segundos) | Variável (minutos a horas) |
| Comportamento | Executa e termina | Faz polling repetido |
| Falha | Erro no código | Timeout (condição nunca satisfeita) |
| Status durante execução | `running` (verde pulsante) | `sensing` (amarelo/laranja) |
| Uso típico | Transformar dados | Esperar arquivo, API, outra DAG |

**Tipos de Sensor disponíveis no Airflow:**

| Sensor | O que espera | Caso de uso |
|--------|-------------|-------------|
| `FileSensor` | Arquivo existir em um caminho | Esperar dados de parceiros |
| `ExternalTaskSensor` | Task de outra DAG terminar | Dependência entre DAGs |
| `HttpSensor` | Endpoint HTTP retornar resposta esperada | API ficar disponível |
| `SqlSensor` | Query SQL retornar resultado | Dados aparecerem no banco |
| `DateTimeSensor` | Horário específico chegar | Aguardar janela de processamento |
| `S3KeySensor` | Arquivo existir no S3 (AWS) | Dados em cloud storage |

> **Carlos:** "Pense no Sensor como um porteiro que verifica documentos. Ele olha, e se o documento (arquivo) ainda não chegou, ele espera um pouco e verifica novamente. Diferente de um operador normal, que seria como um mensageiro — vai, entrega, e volta. O Sensor não vai a lugar nenhum — ele fica ali, verificando pacientemente."

---

## Passo 2: Entender os Parâmetros do FileSensor

**Descrição:** O `FileSensor` possui parâmetros que controlam onde procurar, com que frequência verificar, e quando desistir. Entender cada parâmetro é essencial para configurar sensores eficientes.

**Parâmetros principais:**

```python
from airflow.sensors.filesystem import FileSensor

esperar_arquivo = FileSensor(
    task_id="esperar_arquivo_vendas",
    filepath="/opt/airflow/data/incoming/vendas_20240115.csv",  # Caminho do arquivo
    poke_interval=30,    # Verifica a cada 30 segundos
    timeout=600,         # Desiste após 600 segundos (10 minutos)
    mode="poke",         # Estratégia de espera
    soft_fail=False,     # Se True, timeout = skipped em vez de failed
    fs_conn_id="fs_default",  # Connection ID para o filesystem
)
```

**Detalhamento de cada parâmetro:**

| Parâmetro | Tipo | Default | Descrição |
|-----------|------|---------|-----------|
| `filepath` | str | (obrigatório) | Caminho absoluto do arquivo a monitorar |
| `poke_interval` | int | 60 | Segundos entre cada verificação |
| `timeout` | int | 604800 (7 dias!) | Segundos máximos de espera antes de falhar |
| `mode` | str | "poke" | Estratégia: "poke" ou "reschedule" |
| `soft_fail` | bool | False | Se True, timeout gera `skipped` em vez de `failed` |
| `fs_conn_id` | str | "fs_default" | Connection ID do filesystem no Airflow |

**Cálculo de tentativas:**

```
Número de pokes = timeout / poke_interval

Exemplo: timeout=600, poke_interval=30
  → 600 / 30 = 20 verificações antes de timeout
  → Verifica 20 vezes, espaçadas em 30 segundos
```

**Valores recomendados por cenário:**

| Cenário | poke_interval | timeout | Justificativa |
|---------|--------------|---------|---------------|
| Arquivo chega em minutos | 30s | 600s (10 min) | Verificação frequente, timeout curto |
| Arquivo chega em horas | 300s (5 min) | 14400s (4h) | Não sobrecarrega o scheduler |
| Dependência crítica (SLA) | 60s | 3600s (1h) | Balanço entre urgência e paciência |
| Arquivo opcional | 60s | 300s (5 min) | Timeout curto + `soft_fail=True` |

> **Marina:** "O timeout padrão de 7 dias é perigoso! Sempre defina um timeout explícito. Um sensor rodando por 7 dias ocupa um worker slot desnecessariamente. Definir timeout é como colocar um prazo — se o dado não chegou em X tempo, algo está errado e precisamos de intervenção humana."

---

## Passo 3: Entender mode="poke" vs mode="reschedule"

**Descrição:** O parâmetro `mode` define como o sensor ocupa recursos do Airflow enquanto espera. A escolha impacta diretamente a escalabilidade do seu cluster.

**mode="poke" (padrão):**

```
┌────────────────────────────────────────────────────────────────────────┐
│  mode="poke" — O sensor OCUPA um worker slot durante toda a espera     │
│                                                                         │
│  Worker Slot #1: [===SENSOR (sleeping)===(check)===(sleeping)===(check)]│
│                  │← O slot fica BLOQUEADO até success ou timeout →│     │
│                                                                         │
│  ⚠️ Problema: Se você tem 16 worker slots e 10 sensors em poke,        │
│     sobram apenas 6 slots para tasks reais!                             │
└────────────────────────────────────────────────────────────────────────┘
```

**mode="reschedule":**

```
┌────────────────────────────────────────────────────────────────────────┐
│  mode="reschedule" — O sensor LIBERA o worker slot entre checks        │
│                                                                         │
│  Worker Slot #1: [check]...(slot livre)...[check]...(slot livre)...    │
│                                                                         │
│  ✅ Vantagem: O slot fica disponível para outras tasks entre os checks  │
│     10 sensors em reschedule ≈ 0 slots bloqueados (apenas durante check)│
└────────────────────────────────────────────────────────────────────────┘
```

**Comparação detalhada:**

| Aspecto | mode="poke" | mode="reschedule" |
|---------|-------------|-------------------|
| Worker slot | Ocupado durante TODA a espera | Ocupado APENAS durante o check |
| Overhead no scheduler | Baixo (sensor dorme no slot) | Maior (scheduler reagenda a cada intervalo) |
| Ideal para | Espera curta (< 5 minutos) | Espera longa (> 10 minutos) |
| Status no UI | `sensing` contínuo | Alterna entre `up_for_reschedule` e `sensing` |
| Paralelismo | Pode bloquear outros tasks | Não impacta outros tasks |
| Precisão | Verifica exatamente a cada poke_interval | Pode ter pequenos atrasos de reagendamento |

**Quando usar cada modo:**

```python
# Arquivo chega rápido (parceiro A: 6h-6h30, máximo 30 min de espera)
# → Use "poke" — espera curta, não vale o overhead de reschedule
esperar_parceiro_a = FileSensor(
    task_id="esperar_parceiro_a",
    filepath="/data/incoming/parceiro_a_{{ ds_nodash }}.csv",
    poke_interval=30,
    timeout=1800,       # 30 minutos
    mode="poke",        # ← Espera curta: poke é eficiente
)

# Arquivo pode demorar horas (relatório mensal do parceiro C)
# → Use "reschedule" — não bloqueia worker slots por horas
esperar_relatorio_mensal = FileSensor(
    task_id="esperar_relatorio_mensal",
    filepath="/data/incoming/relatorio_mensal_{{ ds_nodash }}.csv",
    poke_interval=300,  # Verifica a cada 5 minutos
    timeout=14400,      # 4 horas
    mode="reschedule",  # ← Espera longa: libera o slot!
)
```

> **Carlos:** "A regra de ouro é simples: se o sensor vai esperar menos de 5-10 minutos, use `poke`. Se pode esperar mais que isso, use `reschedule`. No nosso caso, os parceiros entregam em ~30 minutos, então `poke` funciona bem. Mas se tivéssemos um parceiro que entrega 'em algum momento do dia', `reschedule` seria obrigatório."

---

## Passo 4: Preparar o Diretório de Incoming

**Descrição:** Antes de criar a DAG, precisamos garantir que o diretório onde o sensor vai monitorar exista dentro do container do Airflow. Esse diretório simula o local onde parceiros depositam arquivos (via SFTP, S3, etc.).

**Comando:**

```bash
# Criar diretório de incoming dentro do container Airflow
docker exec airflow-scheduler mkdir -p /opt/airflow/data/incoming
```

**Resultado esperado:**
```
(nenhuma saída — diretório criado com sucesso)
```

**Verificação:**
```bash
docker exec airflow-scheduler ls -la /opt/airflow/data/incoming/
```

**Resultado esperado:**
```
total 0
drwxr-xr-x 2 airflow airflow 40 ... .
drwxr-xr-x 3 airflow airflow 60 ... ..
```

> **💡 Nota:** O diretório está vazio! Isso é intencional — o sensor vai ficar esperando até que criemos o arquivo manualmente (simulando a entrega do parceiro).

**Verificar a Connection `fs_default`:**

O `FileSensor` usa uma Connection do tipo "File (path)" no Airflow. Vamos verificar se ela existe:

```bash
docker exec airflow-scheduler airflow connections get fs_default 2>/dev/null || echo "Connection não existe"
```

Se não existir, crie:

```bash
docker exec airflow-scheduler airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/"}'
```

**Resultado esperado:**
```
Successfully added `conn_id`=fs_default : fs://:@:
```

> **Carlos:** "A Connection `fs_default` diz ao FileSensor qual é o filesystem base. Com `path: /`, os caminhos no `filepath` são absolutos. Em produção, você poderia apontar para um mount de NFS ou SFTP."

---

## Passo 5: Criar a DAG com FileSensor

**Descrição:** Vamos criar a DAG completa que espera o arquivo de vendas chegar, processa os dados e notifica a equipe. O `FileSensor` usa `{{ ds_nodash }}` no filepath para tornar o sensor date-aware — cada execução espera pelo arquivo do dia correspondente.

**Comando:**

```bash
cat > aula_05/code/dags/dag_sensor_arquivo.py << 'EOF'
"""
DAG: Sensor de Arquivo de Vendas da DataFlow Analytics
======================================================
Descrição: Espera o arquivo de vendas do parceiro chegar no diretório
           de incoming antes de iniciar o processamento.
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: FileSensor → Processamento → Notificação
Schedule: Todo dia às 6h (horário que o parceiro começa a entregar)

Cenário de Negócio:
    - Parceiro A entrega dados entre 6h e 6h30
    - Pipeline deve esperar o arquivo chegar
    - Se não chegar em 10 minutos → timeout (investigar!)
    - Sensor verifica a cada 30 segundos
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os


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
# 2. FUNÇÕES — Lógica de negócio
# ============================================================
def processar_vendas(**context):
    """
    Processa o arquivo de vendas após o sensor confirmar sua existência.
    Em produção, aqui teria leitura com PySpark, transformações,
    e escrita no data lake (bronze → silver).
    """
    execution_date = context["ds"]
    ds_nodash = context["ds_nodash"]
    filepath = f"/opt/airflow/data/incoming/vendas_{ds_nodash}.csv"

    print(f"{'=' * 50}")
    print(f"📦 PROCESSAMENTO DE VENDAS")
    print(f"{'=' * 50}")
    print(f"Data de referência: {execution_date}")
    print(f"Arquivo detectado: {filepath}")
    print(f"")

    # Verifica tamanho do arquivo
    if os.path.exists(filepath):
        tamanho = os.path.getsize(filepath)
        print(f"Tamanho do arquivo: {tamanho:,} bytes")
    else:
        print(f"⚠️ Arquivo não encontrado (não deveria chegar aqui!)")
        return

    print(f"")
    print(f"Etapas de processamento:")
    print(f"  1. Leitura do CSV...")
    print(f"  2. Validação de schema...")
    print(f"  3. Limpeza de dados (nulls, duplicatas)...")
    print(f"  4. Escrita na camada Bronze...")
    print(f"")
    print(f"✅ Processamento concluído!")
    print(f"   - Arquivo processado: vendas_{ds_nodash}.csv")
    print(f"   - Destino: datalake/bronze/vendas/{execution_date}/")

    context["ti"].xcom_push(key="arquivo_processado", value=filepath)
    context["ti"].xcom_push(key="tamanho_bytes", value=tamanho if os.path.exists(filepath) else 0)


EOF
```

**Resultado esperado:**
```
(nenhuma saída — primeira parte do arquivo criada)
```

---

## Passo 6: Completar a DAG — Definição do Sensor e Dependências

**Descrição:** Agora vamos criar o arquivo completo da DAG de uma vez, incluindo o FileSensor com `{{ ds_nodash }}` para tornar o caminho date-aware.

**Comando — crie o arquivo completo:**

```bash
cat > aula_05/code/dags/dag_sensor_arquivo.py << 'EOF'
"""
DAG: Sensor de Arquivo de Vendas da DataFlow Analytics
======================================================
Descrição: Espera o arquivo de vendas do parceiro chegar no diretório
           de incoming antes de iniciar o processamento.
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: FileSensor → Processamento → Notificação
Schedule: Todo dia às 6h (horário que o parceiro começa a entregar)

Cenário de Negócio:
    - Parceiro A entrega dados entre 6h e 6h30
    - Pipeline deve esperar o arquivo chegar
    - Se não chegar em 10 minutos → timeout (investigar!)
    - Sensor verifica a cada 30 segundos
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os


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
# 2. FUNÇÕES — Lógica de negócio
# ============================================================
def processar_vendas(**context):
    """
    Processa o arquivo de vendas após o sensor confirmar sua existência.
    Em produção, aqui teria leitura com PySpark e escrita no data lake.
    """
    execution_date = context["ds"]
    ds_nodash = context["ds_nodash"]
    filepath = f"/opt/airflow/data/incoming/vendas_{ds_nodash}.csv"

    print(f"{'=' * 50}")
    print(f"📦 PROCESSAMENTO DE VENDAS")
    print(f"{'=' * 50}")
    print(f"Data de referência: {execution_date}")
    print(f"Arquivo detectado: {filepath}")

    if os.path.exists(filepath):
        tamanho = os.path.getsize(filepath)
        print(f"Tamanho do arquivo: {tamanho:,} bytes")
    else:
        print(f"⚠️ Arquivo não encontrado!")
        return

    print(f"")
    print(f"Etapas de processamento:")
    print(f"  1. Leitura do CSV...")
    print(f"  2. Validação de schema...")
    print(f"  3. Limpeza de dados...")
    print(f"  4. Escrita na camada Bronze...")
    print(f"")
    print(f"✅ Processamento concluído!")
    print(f"   Destino: datalake/bronze/vendas/{execution_date}/")

    context["ti"].xcom_push(key="arquivo_processado", value=filepath)
    context["ti"].xcom_push(key="tamanho_bytes", value=tamanho)


# ============================================================
# 3. DAG — Pipeline com FileSensor
# ============================================================
with DAG(
    dag_id="dataflow_sensor_arquivo_v1",
    default_args=default_args,
    description="Espera arquivo de vendas chegar antes de processar",
    schedule_interval="0 6 * * *",  # Todo dia às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "sensor", "arquivo", "aula05"],
) as dag:

    # ========================================================
    # 4. TASKS
    # ========================================================

    # Task 1: SENSOR — Espera o arquivo de vendas do dia chegar
    # O {{ ds_nodash }} é um template Jinja que o Airflow substitui
    # pela data de execução no formato YYYYMMDD (ex: 20240115)
    esperar_arquivo = FileSensor(
        task_id="esperar_arquivo_vendas",
        filepath="/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.csv",
        poke_interval=30,       # Verifica a cada 30 segundos
        timeout=600,            # Timeout: 10 minutos (600 segundos)
        mode="poke",            # Mantém worker slot ocupado (espera curta)
        soft_fail=False,        # Timeout = FAILED (arquivo é obrigatório)
        fs_conn_id="fs_default",
    )

    # Task 2: Processar o arquivo após sensor confirmar existência
    processar = PythonOperator(
        task_id="processar_vendas",
        python_callable=processar_vendas,
    )

    # Task 3: Notificar equipe sobre conclusão
    notificar = BashOperator(
        task_id="notificar_conclusao",
        bash_command=(
            'echo "✅ [{{ ds }}] Pipeline de vendas concluído com sucesso! '
            'Arquivo vendas_{{ ds_nodash }}.csv processado e carregado no data lake."'
        ),
    )

    # ========================================================
    # 5. DEPENDÊNCIAS
    # ========================================================
    esperar_arquivo >> processar >> notificar

EOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado com sucesso)
```

**Verificação:**
```bash
ls -la aula_05/code/dags/dag_sensor_arquivo.py
```

**O que observar no código:**

1. **`filepath` com template Jinja:** `vendas_{{ ds_nodash }}.csv`
   - Para execução de 2024-01-15, vira: `vendas_20240115.csv`
   - Cada execução espera pelo arquivo do **seu dia** específico

2. **`poke_interval=30`:** Verifica a cada 30 segundos
   - Em 10 minutos de timeout → máximo 20 verificações

3. **`mode="poke"`:** Parceiro A entrega em ~30 min, espera curta → poke é adequado

4. **`soft_fail=False`:** O arquivo é obrigatório — se não chegar, é falha crítica

> **Carlos:** "O `{{ ds_nodash }}` é a mágica aqui. Se a DAG é agendada para 2024-01-15, o sensor espera por `vendas_20240115.csv`. No dia 2024-01-16, espera por `vendas_20240116.csv`. Cada execução é independente e espera pelo arquivo correto."

---

## Passo 7: Validar a DAG no Container

**Descrição:** Confirmar que o arquivo não tem erros de sintaxe e que o Airflow carrega a DAG corretamente.

**Comando:**
```bash
docker exec airflow-scheduler python /opt/airflow/dags/aula_05/dag_sensor_arquivo.py
```

**Resultado esperado:**
```
(nenhuma saída e nenhum erro — arquivo válido!)
```

**Se aparecer erro**, verifique:
1. `from airflow.sensors.filesystem import FileSensor` — import correto
2. O parâmetro `filepath` usa aspas (é uma string template Jinja, não f-string)
3. O `fs_conn_id="fs_default"` corresponde a uma Connection existente

**Verificar listagem de DAGs:**
```bash
docker exec airflow-scheduler airflow dags list 2>/dev/null | grep sensor
```

**Resultado esperado:**
```
dataflow_sensor_arquivo_v1 | /opt/airflow/dags/aula_05/dag_sensor_arquivo.py | dataflow | False
```

---

## Passo 8: Ativar a DAG e Observar o Sensor em "Sensing"

**Descrição:** Vamos acionar a DAG SEM criar o arquivo primeiro. Assim, podemos observar o sensor no estado `sensing` — verificando periodicamente e não encontrando o arquivo.

**Passos no navegador:**

1. Acesse **http://localhost:8081**
2. Localize **"dataflow_sensor_arquivo_v1"** na lista
3. **Ative a DAG** clicando no toggle
4. Clique em **"▶ Trigger DAG"** (execute manualmente)
5. Vá para a aba **"Graph"**

**Resultado esperado — sensor em estado "sensing":**

```
┌───────────────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ esperar_arquivo_vendas  🟡    │     │ processar_vendas │     │ notificar_conclusao │
│ (sensing... aguardando)       │     │ (não iniciou)    │     │ (não iniciou)       │
└───────────────────────────────┘     └──────────────────┘     └─────────────────────┘
```

> **💡 Cores no Airflow UI:**
> - 🟡 Amarelo/Laranja = `sensing` ou `running` — o sensor está ativamente verificando
> - As tasks downstream ficam sem cor (cinza claro) — aguardando o sensor liberar

**Verificar logs do sensor em tempo real:**

1. Clique na task **"esperar_arquivo_vendas"** (amarela)
2. Clique em **"Log"**

**Log esperado (sensor em espera):**

```
[2024-xx-xx 06:00:00] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
[2024-xx-xx 06:00:00] {base.py} INFO - Sensor has not yet succeeded, 
    sleeping for 30 seconds before trying again.
[2024-xx-xx 06:00:30] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
[2024-xx-xx 06:00:30] {base.py} INFO - Sensor has not yet succeeded, 
    sleeping for 30 seconds before trying again.
[2024-xx-xx 06:01:00] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
...
```

**O que observar nos logs:**

- A mensagem "Poking for file..." se repete a cada 30 segundos
- O sensor informa que "has not yet succeeded" — condição não satisfeita
- O caminho inclui a data renderizada (`20240101` em vez de `{{ ds_nodash }}`)

> **Ana:** "Legal! O pipeline não falhou — ele está **esperando pacientemente**. Isso é muito melhor do que falhar às 6h01 porque o parceiro atrasou 2 minutos. Agora preciso ver o que acontece quando o arquivo chegar."

---

## Passo 9: Simular a Chegada do Arquivo (Sensor → Success)

**Descrição:** Agora vamos simular a entrega do parceiro criando o arquivo manualmente. O sensor detectará o arquivo na próxima verificação e liberará o pipeline.

**Primeiro, descubra a data de execução:** verifique no log do sensor qual data está sendo usada. Se você trigou a DAG manualmente, a data será a `start_date` da DAG ou a data atual. Procure a linha "Poking for file" no log para ver o nome exato do arquivo esperado.

**Comando — criar o arquivo (simulando entrega do parceiro):**

```bash
# Descubra a data no log do sensor (ex: vendas_20240101.csv)
# Substitua 20240101 pela data que aparece no log do seu sensor

docker exec airflow-scheduler bash -c '
cat > /opt/airflow/data/incoming/vendas_20240101.csv << CSVEOF
order_id,customer_id,product_id,quantity,unit_price,total_amount,order_date,payment_method,shipping_city,shipping_state,status
ORD-001,CUST-1234,PROD-567,2,49.90,99.80,2024-01-01,credit_card,São Paulo,SP,delivered
ORD-002,CUST-5678,PROD-890,1,199.00,199.00,2024-01-01,pix,Rio de Janeiro,RJ,shipped
ORD-003,CUST-9012,PROD-345,3,29.90,89.70,2024-01-01,boleto,Belo Horizonte,MG,pending
ORD-004,CUST-3456,PROD-678,1,599.00,599.00,2024-01-01,credit_card,Curitiba,PR,delivered
ORD-005,CUST-7890,PROD-123,5,15.00,75.00,2024-01-01,pix,Salvador,BA,shipped
CSVEOF
'
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado)
```

**Verificação:**
```bash
docker exec airflow-scheduler ls -la /opt/airflow/data/incoming/
```

**Resultado esperado:**
```
total 4
drwxr-xr-x 2 airflow airflow   60 ... .
drwxr-xr-x 3 airflow airflow   60 ... ..
-rw-r--r-- 1 airflow airflow  523 ... vendas_20240101.csv
```

**Agora volte ao Airflow UI e observe a transição:**

```
ANTES (sensor em espera):
┌───────────────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ esperar_arquivo_vendas  🟡    │     │ processar_vendas │     │ notificar_conclusao │
│ (sensing...)                  │     │ (aguardando)     │     │ (aguardando)        │
└───────────────────────────────┘     └──────────────────┘     └─────────────────────┘

DEPOIS (arquivo encontrado! pipeline executa):
┌───────────────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ esperar_arquivo_vendas  🟢    │────▶│ processar_vendas │────▶│ notificar_conclusao │
│ (success!)                    │  🟢 │ (success)        │  🟢 │ (success)           │
└───────────────────────────────┘     └──────────────────┘     └─────────────────────┘
```

**Log do sensor após encontrar o arquivo:**

```
[2024-xx-xx 06:02:00] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
[2024-xx-xx 06:02:00] {base.py} INFO - Sensor has not yet succeeded, sleeping for 30 seconds...
[2024-xx-xx 06:02:30] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
[2024-xx-xx 06:02:30] {base.py} INFO - Success criteria met. Exiting.
```

**A linha-chave:** `Success criteria met. Exiting.` — o sensor encontrou o arquivo e liberou o pipeline!

> **Carlos:** "Viu? O sensor ficou esperando pacientemente e, no momento em que o arquivo apareceu, liberou o processamento automaticamente. Sem intervenção humana, sem falhas desnecessárias. É exatamente o que a Ana pediu."

---

## Passo 10: Testar o Timeout (Arquivo Não Chega)

**Descrição:** Vamos testar o cenário de falha: o que acontece quando o arquivo não chega dentro do timeout? Para isso, vamos executar a DAG sem criar o arquivo e aguardar o timeout.

**Para agilizar o teste**, vamos criar uma versão temporária com timeout mais curto. Edite o parâmetro do sensor:

```python
# Altere TEMPORARIAMENTE para teste rápido:
esperar_arquivo = FileSensor(
    task_id="esperar_arquivo_vendas",
    filepath="/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.csv",
    poke_interval=10,       # Verifica a cada 10 segundos (mais rápido para teste)
    timeout=60,             # Timeout em apenas 1 minuto (para teste)
    mode="poke",
    soft_fail=False,
    fs_conn_id="fs_default",
)
```

**Passos:**

1. Limpe o arquivo criado anteriormente (para forçar o timeout):
```bash
docker exec airflow-scheduler rm -f /opt/airflow/data/incoming/vendas_20240101.csv
```

2. Trigger a DAG novamente no UI (▶ Trigger DAG)

3. Aguarde ~1 minuto e observe

**Resultado esperado — sensor em timeout (FAILED):**

```
┌───────────────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ esperar_arquivo_vendas  🔴    │     │ processar_vendas │     │ notificar_conclusao │
│ (FAILED — timeout!)          │     │ (upstream_failed)│     │ (upstream_failed)   │
└───────────────────────────────┘     └──────────────────┘     └─────────────────────┘
```

**Log do sensor em timeout:**

```
[2024-xx-xx] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
[2024-xx-xx] {base.py} INFO - Sensor has not yet succeeded, sleeping for 10 seconds...
[2024-xx-xx] {filesystem.py} INFO - Poking for file /opt/airflow/data/incoming/vendas_20240101.csv
...
[2024-xx-xx] {taskinstance.py} ERROR - Sensor has timed out; run duration of 60 seconds exceeds 
    the specified timeout of 60 seconds.
```

**O que acontece em cascata:**

| Task | Status | Motivo |
|------|--------|--------|
| esperar_arquivo_vendas | 🔴 `failed` | Timeout: arquivo não chegou em 60s |
| processar_vendas | 🟠 `upstream_failed` | Não executa porque upstream falhou |
| notificar_conclusao | 🟠 `upstream_failed` | Não executa porque upstream falhou |

> **Ana:** "Perfeito! Se o arquivo não chega, o pipeline falha de forma clara e controlada. Mas e se o arquivo for opcional? Por exemplo, o parceiro C nem sempre manda dados todo dia..."

> **💡 Importante:** Após o teste, volte os valores originais (`poke_interval=30`, `timeout=600`).

---

## Passo 11: soft_fail=True — Arquivos Opcionais

**Descrição:** Quando `soft_fail=True`, o timeout gera status `skipped` em vez de `failed`. Isso é útil para arquivos opcionais — se não chegarem, o pipeline pode continuar sem eles (usando `trigger_rule` nas tasks downstream).

**Cenário:** O parceiro C envia dados eventualmente (nem todo dia). Se o arquivo não chegar, queremos **pular** o processamento desse parceiro, mas continuar o resto do pipeline.

**Exemplo — sensor com soft_fail:**

```python
# Sensor para arquivo OPCIONAL (parceiro C)
esperar_parceiro_c = FileSensor(
    task_id="esperar_parceiro_c",
    filepath="/opt/airflow/data/incoming/parceiro_c_{{ ds_nodash }}.csv",
    poke_interval=30,
    timeout=300,          # Espera apenas 5 minutos
    mode="poke",
    soft_fail=True,       # ← DIFERENÇA! Timeout = skipped, não failed
    fs_conn_id="fs_default",
)
```

**Comparação de comportamento no timeout:**

| Parâmetro | Quando dá timeout | Efeito em downstream | Quando usar |
|-----------|-------------------|---------------------|-------------|
| `soft_fail=False` | 🔴 `failed` | 🟠 `upstream_failed` (não executam) | Arquivo **obrigatório** |
| `soft_fail=True` | 🩷 `skipped` | Depende do `trigger_rule` | Arquivo **opcional** |

**Combinação com trigger_rule:**

```python
# Sensor opcional
esperar_parceiro_c = FileSensor(
    task_id="esperar_parceiro_c",
    filepath="/opt/airflow/data/incoming/parceiro_c_{{ ds_nodash }}.csv",
    poke_interval=30,
    timeout=300,
    mode="poke",
    soft_fail=True,       # Timeout = skipped
    fs_conn_id="fs_default",
)

# Task downstream que executa MESMO se sensor foi skipped
processar_parceiro_c = PythonOperator(
    task_id="processar_parceiro_c",
    python_callable=lambda: print("Processando parceiro C..."),
    trigger_rule="none_failed_min_one_success",  # Executa se sensor deu success OU skipped
)

esperar_parceiro_c >> processar_parceiro_c
```

**Cenário completo — múltiplos parceiros com diferentes criticidades:**

```
┌────────────────────────────────────────────────────────────────────────────┐
│  Parceiro A (obrigatório):  soft_fail=False  → Timeout = FAILED          │
│  Parceiro B (obrigatório):  soft_fail=False  → Timeout = FAILED          │
│  Parceiro C (opcional):     soft_fail=True   → Timeout = SKIPPED         │
│                                                                            │
│  Se A ou B não chegam → pipeline FALHA (precisamos desses dados!)         │
│  Se C não chega → pipeline CONTINUA sem esses dados (é complementar)      │
└────────────────────────────────────────────────────────────────────────────┘
```

> **Carlos:** "O `soft_fail=True` é elegante para fontes opcionais. Em vez de falhar o pipeline inteiro porque um parceiro complementar não mandou dados, o sensor simplesmente marca como 'pulado' e a vida continua. O relatório final terá dados de A e B, e incluirá C apenas se estava disponível."

---

## Passo 12: Template {{ ds_nodash }} — Sensors Date-Aware

**Descrição:** O uso de templates Jinja no `filepath` é fundamental para sensors em pipelines agendados. Cada execução da DAG tem sua própria data (`ds`), e o sensor deve esperar pelo arquivo **daquele dia específico**.

**Templates Jinja disponíveis para filepath:**

| Template | Formato | Exemplo (para 2024-01-15) |
|----------|---------|---------------------------|
| `{{ ds }}` | YYYY-MM-DD | 2024-01-15 |
| `{{ ds_nodash }}` | YYYYMMDD | 20240115 |
| `{{ execution_date.strftime('%Y%m%d_%H') }}` | Customizado | 20240115_06 |
| `{{ data_interval_start \| ds }}` | YYYY-MM-DD | 2024-01-15 |
| `{{ macros.ds_add(ds, -1) }}` | Dia anterior | 2024-01-14 |

**Exemplos de filepath com templates:**

```python
# Arquivo do dia: vendas_20240115.csv
filepath="/data/incoming/vendas_{{ ds_nodash }}.csv"

# Arquivo com data formatada: vendas_2024-01-15.csv
filepath="/data/incoming/vendas_{{ ds }}.csv"

# Arquivo do dia ANTERIOR (parceiro entrega D-1):
filepath="/data/incoming/vendas_{{ macros.ds_add(ds, -1) | ds_nodash }}.csv"

# Arquivo com diretório por data: /2024/01/15/vendas.csv
filepath="/data/incoming/{{ execution_date.strftime('%Y/%m/%d') }}/vendas.csv"

# Wildcard (qualquer arquivo .csv no diretório do dia):
filepath="/data/incoming/{{ ds_nodash }}/*.csv"
```

**Por que isso é importante:**

```
Execução de 2024-01-15:
  Sensor espera: vendas_20240115.csv ← Arquivo correto para este dia

Execução de 2024-01-16:
  Sensor espera: vendas_20240116.csv ← Arquivo correto para este dia

Se fizéssemos BACKFILL (reprocessamento):
  Execução de 2024-01-10:
    Sensor espera: vendas_20240110.csv ← Cada backfill espera SEU arquivo!
```

**Sem template (ERRADO — não faça isso!):**

```python
# ❌ ERRADO: filepath fixo não funciona com backfill/reprocessamento
filepath="/data/incoming/vendas_hoje.csv"

# ✅ CORRETO: filepath dinâmico com template Jinja
filepath="/data/incoming/vendas_{{ ds_nodash }}.csv"
```

> **Marina:** "Usar templates no filepath garante que o pipeline é **idempotente**. Se precisar reprocessar dados de 3 dias atrás, cada execução sabe exatamente qual arquivo procurar. Sem template, todas as execuções procurariam o mesmo arquivo — desastre."

---

## Passo 13: Verificar o Grafo e Logs Completos

**Descrição:** Vamos revisitar a execução completa no Airflow UI para consolidar o entendimento do ciclo de vida do sensor.

**No Airflow UI — aba Graph:**

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Graph — dataflow_sensor_arquivo_v1                                       │
│                                                                           │
│  ┌───────────────────────────┐   ┌──────────────────┐   ┌──────────────┐│
│  │ esperar_arquivo_vendas    │──▶│ processar_vendas │──▶│ notificar_   ││
│  │ (FileSensor)              │   │ (PythonOperator) │   │  conclusao   ││
│  └───────────────────────────┘   └──────────────────┘   └──────────────┘│
│                                                                           │
│  ⏱️ Sensor: poke_interval=30s, timeout=600s, mode=poke                   │
└──────────────────────────────────────────────────────────────────────────┘
```

**Timeline de uma execução típica (arquivo chega após 2 minutos):**

```
06:00:00 → Sensor inicia, verifica: arquivo NÃO existe
06:00:30 → Sensor verifica (poke #2): arquivo NÃO existe
06:01:00 → Sensor verifica (poke #3): arquivo NÃO existe
06:01:30 → Sensor verifica (poke #4): arquivo NÃO existe
06:02:00 → Sensor verifica (poke #5): arquivo EXISTE! ✅ Success!
06:02:01 → processar_vendas inicia
06:02:03 → processar_vendas termina (success)
06:02:04 → notificar_conclusao executa
06:02:04 → Pipeline completo! ✅
```

**Verificar XComs da execução:**

No Airflow UI → Admin → XComs → Filtrar por DAG `dataflow_sensor_arquivo_v1`:

```
┌───────────────────────────────────────────────────────────────────────┐
│  Key                │ Task ID              │ Value                     │
├─────────────────────┼──────────────────────┼───────────────────────────┤
│ arquivo_processado  │ processar_vendas     │ "/opt/airflow/data/..."   │
│ tamanho_bytes       │ processar_vendas     │ 523                       │
│ return_value        │ notificar_conclusao  │ (bash output)             │
└─────────────────────┴──────────────────────┴───────────────────────────┘
```

**O que observar:**

- O sensor **não gera XComs** — ele apenas verifica condição e libera
- `processar_vendas` registra metadados do arquivo processado
- O tempo entre início do sensor e success do sensor = tempo de espera pelo arquivo

> **Carlos:** "Na aba 'Grid', você verá o histórico de execuções. Se o parceiro está consistentemente entregando com 5 minutos de atraso, o sensor sempre terá ~5 minutos de duração. Se um dia o sensor demora 9 minutos — perto do timeout — é sinal de que algo está errado no lado do parceiro."

---

## Passo 14: Boas Práticas com Sensors

**Descrição:** Sensors são poderosos mas podem causar problemas se mal configurados. Aqui estão as boas práticas que a equipe da DataFlow segue.

**1. Sempre defina timeout explícito:**

```python
# ❌ ERRADO: timeout padrão é 7 dias! Worker slot bloqueado por uma semana
esperar_arquivo = FileSensor(
    task_id="esperar_arquivo",
    filepath="/data/incoming/arquivo.csv",
)

# ✅ CORRETO: timeout explícito e razoável
esperar_arquivo = FileSensor(
    task_id="esperar_arquivo",
    filepath="/data/incoming/arquivo.csv",
    timeout=3600,  # 1 hora — se não chegou em 1h, algo está errado
)
```

**2. Use reschedule para esperas longas:**

```python
# ❌ ERRADO: poke com espera de 4 horas bloqueia worker slot
esperar_relatorio = FileSensor(
    task_id="esperar_relatorio",
    filepath="/data/reports/mensal.csv",
    poke_interval=300,
    timeout=14400,   # 4 horas
    mode="poke",     # ← Worker slot BLOQUEADO por 4 horas!
)

# ✅ CORRETO: reschedule libera o slot entre verificações
esperar_relatorio = FileSensor(
    task_id="esperar_relatorio",
    filepath="/data/reports/mensal.csv",
    poke_interval=300,
    timeout=14400,
    mode="reschedule",  # ← Worker slot LIVRE entre checks!
)
```

**3. Use poke_interval proporcional ao timeout:**

```python
# ❌ ERRADO: poke a cada 5 segundos com timeout de 4 horas = 2880 checks!
poke_interval=5, timeout=14400

# ✅ CORRETO: poke a cada 5 minutos com timeout de 4 horas = 48 checks
poke_interval=300, timeout=14400
```

**Regra de ouro:** `poke_interval` deve ser pelo menos 1% do `timeout`.

**4. Combine com callbacks para alertar sobre espera prolongada:**

```python
def alerta_sensor_demorado(context):
    """Callback que dispara se o sensor está demorando muito."""
    print(f"⚠️ ALERTA: Sensor {context['task_instance'].task_id} "
          f"em execução há mais de {context['task_instance'].duration}s!")

esperar_arquivo = FileSensor(
    task_id="esperar_arquivo_vendas",
    filepath="/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.csv",
    poke_interval=30,
    timeout=600,
    mode="poke",
    execution_timeout=timedelta(minutes=8),  # Alerta antes do timeout
    on_failure_callback=alerta_sensor_demorado,
)
```

**Resumo de boas práticas:**

| Prática | Motivo |
|---------|--------|
| Sempre definir `timeout` | Evitar sensors "órfãos" rodando indefinidamente |
| Usar `reschedule` para espera > 10 min | Liberar worker slots para tasks produtivas |
| `poke_interval` ≥ 1% do `timeout` | Evitar excesso de verificações |
| `soft_fail=True` para fontes opcionais | Pipeline não falha por dado complementar |
| Templates Jinja no filepath | Garantir idempotência entre execuções |
| Monitorar duração do sensor | Detectar degradação na entrega dos parceiros |

> **Marina:** "Sensors mal configurados são a causa #1 de 'DAGs travadas' no Airflow. O cenário mais comum: alguém cria um sensor com timeout padrão (7 dias!) e mode=poke. Resultado: worker slot bloqueado por uma semana inteira. Monitoramento de 'sensors em espera há mais de X minutos' é obrigatório em produção."

---

## Resumo do Exercício

### O que aprendemos:

| Conceito | Descrição |
|----------|-----------|
| **Sensor** | Operador que faz polling até condição ser satisfeita |
| **FileSensor** | Sensor específico que espera arquivo existir em um caminho |
| **poke_interval** | Intervalo entre verificações (segundos) |
| **timeout** | Tempo máximo de espera antes de falhar |
| **mode="poke"** | Mantém worker slot ocupado (espera curta) |
| **mode="reschedule"** | Libera worker slot entre checks (espera longa) |
| **soft_fail=True** | Timeout gera `skipped` em vez de `failed` (arquivos opcionais) |
| **{{ ds_nodash }}** | Template Jinja para filepath date-aware (YYYYMMDD) |

### Ciclo de vida do FileSensor:

```
INÍCIO → poke() → arquivo existe?
                      │
                     YES → SUCCESS ✅ → libera tasks downstream
                      │
                      NO → timeout atingido?
                               │
                              YES → FAILED 🔴 (ou SKIPPED 🩷 com soft_fail)
                               │
                              NO → sleep(poke_interval) → poke() novamente ↺
```

### Erros comuns:

| Erro | Causa | Solução |
|------|-------|---------|
| Sensor em timeout sem motivo | Caminho do arquivo incorreto ou permissão | Verificar filepath exato e permissões |
| Sensor nunca termina | Timeout padrão de 7 dias! | Definir `timeout` explícito |
| Workers esgotados | Muitos sensors em `mode="poke"` | Usar `mode="reschedule"` para esperas longas |
| `AirflowSensorTimeout` | Arquivo não chegou no prazo | Verificar se parceiro está entregando; ajustar timeout |
| Template não renderiza | Usou f-string em vez de Jinja | Usar `"{{ ds_nodash }}"` (aspas, não f-string) |
| Connection error | `fs_conn_id` não existe | Criar Connection `fs_default` no Airflow |

### Diagrama final do pipeline:

```
┌───────────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ esperar_arquivo_vendas    │────▶│ processar_vendas │────▶│ notificar_conclusao │
│ FileSensor                │     │ PythonOperator   │     │ BashOperator        │
│ poke_interval=30s         │     │                  │     │                     │
│ timeout=600s              │     │                  │     │                     │
│ mode="poke"               │     │                  │     │                     │
│ filepath=vendas_{{ds}}.csv│     │                  │     │                     │
└───────────────────────────┘     └──────────────────┘     └─────────────────────┘
```

### Próximo exercício:

No **Exercício 3**, vamos aprender a usar `TaskGroups` — uma forma de organizar visualmente tasks relacionadas em grupos colapsáveis. A Marina quer que DAGs com 20+ tasks sejam legíveis no UI.

---

> **Marina:** "Excelente! Agora nosso pipeline é resiliente a atrasos na entrega de dados. O sensor espera pacientemente e só libera quando o dado está disponível. Isso elimina falhas falsas e reexecuções manuais — ganhamos horas de produtividade da equipe por semana."

> **Ana:** "Adorei o `soft_fail`! Agora posso configurar o pipeline para processar os parceiros obrigatórios sem falhar por causa do parceiro C que é complementar. O relatório sai com o que temos, e se o parceiro C mandar depois, reprocessamos apenas ele."

