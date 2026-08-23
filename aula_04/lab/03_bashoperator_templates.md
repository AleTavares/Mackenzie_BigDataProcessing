# Exercício 3 — BashOperator: Notificações e Comandos Auxiliares

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Nosso pipeline de vendas está funcionando bem, mas preciso adicionar tarefas auxiliares: verificar espaço em disco antes de processar, criar diretórios de staging particionados por data, enviar notificações quando o pipeline termina e limpar arquivos temporários antigos. São coisas simples de shell — perfeitas para o BashOperator."

> **Marina Silva (CTO):** "Boa, Carlos. O BashOperator é ideal para operações de infraestrutura que complementam a lógica de negócio do PythonOperator. E aproveita para usar as template variables do Airflow — o `{{ ds }}` e amigos. Isso torna os comandos dinâmicos sem precisar de Python."

## Objetivos

Ao final deste exercício, você será capaz de:

- Usar `BashOperator` para comandos simples, multi-linha e com variáveis de ambiente
- Aplicar **Jinja template variables** (`{{ ds }}`, `{{ ds_nodash }}`, `{{ ts }}`, `{{ macros }}`)
- Criar diretórios particionados por data dinamicamente
- Simular notificações via `curl` (webhook estilo Slack)
- Entender como exit codes do bash afetam o estado da task (0 = success, 1 = fail)
- Usar `params` para configurar comandos do BashOperator

## Pré-requisitos

- Exercícios 1 e 2 concluídos
- Ambiente Airflow rodando (ver `00_setup.md`)
- Airflow UI acessível em http://localhost:8081

## O que vamos construir?

Uma DAG de **operações auxiliares** que Carlos executa antes e depois do pipeline principal:

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ verificar   │────▶│ criar_diretorios│────▶│ processar_dados │
│ disco       │     │ staging         │     │ (simulado)      │
└─────────────┘     └─────────────────┘     └─────────────────┘
                                                     │
                          ┌──────────────────────────┤
                          ▼                          ▼
                   ┌─────────────┐          ┌─────────────────┐
                   │ limpar_tmp  │          │ notificar_slack │
                   └─────────────┘          └─────────────────┘
                          │                          │
                          └──────────┬───────────────┘
                                     ▼
                            ┌─────────────────┐
                            │ relatorio_final │
                            └─────────────────┘
```

**6 tasks usando apenas BashOperator** — demonstrando diferentes padrões de uso.

---

## Passo 1: Entender o BashOperator e Template Variables

**Descrição:** O `BashOperator` executa comandos shell dentro do worker do Airflow. O Airflow renderiza **Jinja templates** antes de executar — isso permite injetar datas, timestamps e expressões dinâmicas nos comandos.

**Template Variables mais usadas:**

| Variável | Exemplo de valor | Descrição |
|----------|-----------------|-----------|
| `{{ ds }}` | `2024-01-15` | Data de execução (YYYY-MM-DD) |
| `{{ ds_nodash }}` | `20240115` | Data sem hífens (bom para nomes de arquivo) |
| `{{ prev_ds }}` | `2024-01-14` | Data da execução anterior |
| `{{ next_ds }}` | `2024-01-16` | Data da próxima execução |
| `{{ ts }}` | `2024-01-15T06:00:00+00:00` | Timestamp completo ISO |
| `{{ execution_date }}` | datetime object | Objeto datetime Python |
| `{{ macros.ds_add(ds, 7) }}` | `2024-01-22` | Adiciona dias à data |
| `{{ macros.ds_format(ds, '%d/%m/%Y') }}` | `15/01/2024` | Formata a data |
| `{{ params.meu_param }}` | (definido pelo usuário) | Parâmetro customizado |

> **Carlos:** "O Jinja rendering acontece **antes** do comando executar. Ou seja, o scheduler substitui `{{ ds }}` pelo valor real da data e só então roda o bash. É como um find-and-replace automático."

**Exit codes e estado da task:**

| Exit Code | Estado no Airflow | Significado |
|-----------|-------------------|-------------|
| `0` | ✅ Success | Comando executou sem erros |
| `1` (ou qualquer ≠ 0) | ❌ Failed | Comando falhou |

---

## Passo 2: Criar a DAG com BashOperator

**Descrição:** Vamos criar a DAG `dag_operacoes_auxiliares.py` com 6 tasks que demonstram diferentes padrões do BashOperator.

**Comando:**
```bash
cat > aula_04/code/dags/dag_operacoes_auxiliares.py << 'EOF'
"""
DAG: Operações Auxiliares de Infraestrutura
============================================
Descrição: Tasks de suporte ao pipeline principal - verificação de disco,
           criação de diretórios, limpeza e notificações.
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Schedule: Todo dia às 5:30h (antes do pipeline principal)

Demonstra:
- BashOperator com comandos simples e multi-linha
- Jinja template variables ({{ ds }}, {{ ds_nodash }}, etc.)
- Variáveis de ambiente no bash
- Parâmetros configuráveis via params
- Exit codes e efeito no estado da task
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
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
# 2. DEFINIÇÃO DA DAG
# ============================================================
with DAG(
    dag_id="dataflow_operacoes_auxiliares",
    default_args=default_args,
    description="Operações auxiliares: disco, diretórios, limpeza e notificação",
    schedule_interval="30 5 * * *",  # 5:30h, antes do pipeline principal (6h)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "infraestrutura", "bash"],
    params={
        "disco_minimo_gb": 5,
        "dias_retencao_tmp": 7,
        "webhook_url": "http://httpbin.org/post",
    },
) as dag:

    # ============================================================
    # TASK 1: Verificar espaço em disco
    # Padrão: comando simples com lógica condicional
    # ============================================================
    verificar_disco = BashOperator(
        task_id="verificar_disco",
        bash_command="""
            echo "=== Verificação de Disco ==="
            echo "Data de execução: {{ ds }}"
            echo "Verificando espaço disponível..."

            # Captura espaço livre em GB na partição /
            ESPACO_LIVRE=$(df -BG / | tail -1 | awk '{print $4}' | sed 's/G//')
            echo "Espaço livre: ${ESPACO_LIVRE}GB"

            # Verifica mínimo necessário (usa params do Airflow)
            MINIMO={{ params.disco_minimo_gb }}
            echo "Mínimo necessário: ${MINIMO}GB"

            if [ "${ESPACO_LIVRE}" -lt "${MINIMO}" ]; then
                echo "ERRO: Espaço insuficiente! Pipeline NÃO deve executar."
                exit 1
            fi

            echo "OK: Espaço suficiente para processar dados de {{ ds }}"
        """,
    )

    # ============================================================
    # TASK 2: Criar diretórios de staging particionados por data
    # Padrão: template variables para nomes dinâmicos
    # ============================================================
    criar_diretorios = BashOperator(
        task_id="criar_diretorios_staging",
        bash_command="""
            echo "=== Criando Diretórios de Staging ==="
            echo "Data de execução: {{ ds }}"
            echo "Data sem hífen: {{ ds_nodash }}"

            # Cria estrutura de diretórios particionada por data
            mkdir -p /tmp/dataflow/staging/{{ ds }}
            mkdir -p /tmp/dataflow/staging/{{ ds }}/bronze
            mkdir -p /tmp/dataflow/staging/{{ ds }}/silver
            mkdir -p /tmp/dataflow/staging/{{ ds }}/gold

            # Cria diretório para logs do dia
            mkdir -p /tmp/dataflow/logs/{{ ds_nodash }}

            echo "Diretórios criados:"
            find /tmp/dataflow/staging/{{ ds }} -type d | sort

            echo ""
            echo "Pronto para receber dados de {{ ds }}"
            echo "Diretório anterior ({{ prev_ds }}) também disponível para comparação"
        """,
    )

    # ============================================================
    # TASK 3: Processar dados (simulado)
    # Padrão: variáveis de ambiente + multi-linha
    # ============================================================
    processar_dados = BashOperator(
        task_id="processar_dados",
        bash_command="""
            echo "=== Processamento de Dados ==="
            echo "Executando pipeline para data: {{ ds }}"
            echo "Timestamp completo: {{ ts }}"
            echo ""

            # Simula processamento escrevendo arquivo de controle
            ARQUIVO_CONTROLE="/tmp/dataflow/staging/{{ ds }}/processamento.log"

            echo "inicio={{ ts }}" > ${ARQUIVO_CONTROLE}
            echo "data_execucao={{ ds }}" >> ${ARQUIVO_CONTROLE}
            echo "data_anterior={{ prev_ds }}" >> ${ARQUIVO_CONTROLE}
            echo "proxima_execucao={{ next_ds }}" >> ${ARQUIVO_CONTROLE}

            # Simula tempo de processamento
            sleep 2

            echo "registros_processados=1500" >> ${ARQUIVO_CONTROLE}
            echo "status=sucesso" >> ${ARQUIVO_CONTROLE}
            echo "fim=$(date -Iseconds)" >> ${ARQUIVO_CONTROLE}

            echo "Processamento concluído. Arquivo de controle:"
            cat ${ARQUIVO_CONTROLE}
        """,
        # Variáveis de ambiente disponíveis no script
        env={
            "PIPELINE_NAME": "dataflow_operacoes_auxiliares",
            "ENVIRONMENT": "desenvolvimento",
        },
    )

    # ============================================================
    # TASK 4: Limpar arquivos temporários antigos
    # Padrão: uso de macros para cálculo de datas
    # ============================================================
    limpar_tmp = BashOperator(
        task_id="limpar_tmp",
        bash_command="""
            echo "=== Limpeza de Arquivos Temporários ==="
            echo "Data atual: {{ ds }}"
            echo "Retenção: {{ params.dias_retencao_tmp }} dias"
            echo "Data limite: {{ macros.ds_add(ds, -params.dias_retencao_tmp) }}"
            echo ""

            # Conta arquivos .tmp com mais de N dias
            TOTAL_ANTES=$(find /tmp -name "*.tmp" 2>/dev/null | wc -l)
            echo "Arquivos .tmp encontrados: ${TOTAL_ANTES}"

            # Remove arquivos temporários antigos (mais de 7 dias)
            echo "Removendo arquivos com mais de {{ params.dias_retencao_tmp }} dias..."
            find /tmp -name "*.tmp" -mtime +{{ params.dias_retencao_tmp }} -delete 2>/dev/null || true

            TOTAL_DEPOIS=$(find /tmp -name "*.tmp" 2>/dev/null | wc -l)
            REMOVIDOS=$((TOTAL_ANTES - TOTAL_DEPOIS))
            echo "Arquivos removidos: ${REMOVIDOS}"
            echo "Limpeza concluída para {{ ds }}"
        """,
    )

    # ============================================================
    # TASK 5: Enviar notificação (simula webhook Slack)
    # Padrão: curl com dados dinâmicos via template
    # ============================================================
    notificar_slack = BashOperator(
        task_id="notificar_slack",
        bash_command="""
            echo "=== Enviando Notificação ==="
            echo "Webhook URL: {{ params.webhook_url }}"

            # Monta payload JSON com informações do pipeline
            PAYLOAD='{
                "text": "✅ Pipeline DataFlow concluído",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Pipeline:* dataflow_operacoes_auxiliares\\n*Data:* {{ ds }}\\n*Status:* Sucesso\\n*Próxima execução:* {{ next_ds }}"
                        }
                    }
                ]
            }'

            echo "Payload:"
            echo "${PAYLOAD}" | python3 -m json.tool 2>/dev/null || echo "${PAYLOAD}"

            # Envia via curl (httpbin.org retorna o que recebeu - perfeito para teste)
            echo ""
            echo "Enviando para webhook..."
            curl -s -X POST "{{ params.webhook_url }}" \
                -H "Content-Type: application/json" \
                -d "${PAYLOAD}" \
                -o /dev/null \
                -w "HTTP Status: %{http_code}\n" || true

            echo "Notificação enviada para execução de {{ ds }}"
        """,
    )

    # ============================================================
    # TASK 6: Relatório final com resumo
    # Padrão: script multi-linha com macros avançados
    # ============================================================
    relatorio_final = BashOperator(
        task_id="relatorio_final",
        bash_command="""
            echo "╔══════════════════════════════════════════════╗"
            echo "║   RELATÓRIO DE EXECUÇÃO - DataFlow Analytics ║"
            echo "╠══════════════════════════════════════════════╣"
            echo "║ Data de execução:  {{ ds }}                  ║"
            echo "║ Data anterior:     {{ prev_ds }}             ║"
            echo "║ Próxima execução:  {{ next_ds }}             ║"
            echo "║ Semana seguinte:   {{ macros.ds_add(ds, 7) }}║"
            echo "║ Timestamp:         {{ ts }}                  ║"
            echo "╠══════════════════════════════════════════════╣"
            echo "║ Tasks executadas:  6/6                       ║"
            echo "║ Status:            SUCESSO                   ║"
            echo "╚══════════════════════════════════════════════╝"
            echo ""
            echo "Diretórios criados para {{ ds }}:"
            ls -la /tmp/dataflow/staging/{{ ds }}/ 2>/dev/null || echo "(diretório não encontrado)"
            echo ""
            echo "Pipeline de operações auxiliares concluído com sucesso!"
        """,
    )

    # ============================================================
    # 3. DEPENDÊNCIAS
    # ============================================================
    # Verificar disco → Criar diretórios → Processar
    # Após processamento: limpar tmp E notificar (paralelo)
    # Ambos convergem para o relatório final

    verificar_disco >> criar_diretorios >> processar_dados
    processar_dados >> [limpar_tmp, notificar_slack]
    [limpar_tmp, notificar_slack] >> relatorio_final

EOF
```

**Resultado esperado:**
```
(arquivo criado sem erros)
```

**Explicação das seções:**

| Seção | O que faz |
|-------|-----------|
| `params` na DAG | Define valores configuráveis acessíveis via `{{ params.nome }}` |
| `env` no BashOperator | Injeta variáveis de ambiente no script bash |
| `{{ ds }}`, `{{ ds_nodash }}` | Templates substituídos pela data real de execução |
| `{{ macros.ds_add(ds, 7) }}` | Calcula datas relativas usando macros do Airflow |
| `exit 1` na task de disco | Faz a task falhar se não houver espaço suficiente |
| `|| true` após comandos | Evita que erros não-críticos matem a task |

---

## Passo 3: Verificar se o Airflow detectou a DAG

**Descrição:** Após criar o arquivo, o Airflow scheduler leva até 30 segundos para detectar a nova DAG. Vamos verificar.

**Comando:**
```bash
docker exec airflow-scheduler airflow dags list | grep operacoes
```

**Resultado esperado:**
```
dataflow_operacoes_auxiliares | /opt/airflow/dags/dag_operacoes_auxiliares.py | dataflow | False
```

> 💡 **Dica:** Se a DAG não aparecer, verifique erros de sintaxe com:
> ```bash
> docker exec airflow-scheduler python /opt/airflow/dags/dag_operacoes_auxiliares.py
> ```
> Se não houver output, o arquivo está correto. Se aparecer um traceback, corrija o erro indicado.

---

## Passo 4: Ativar e acionar a DAG

**Descrição:** Vamos ativar a DAG (unpause) e dispará-la manualmente para verificar o comportamento.

**4.1 — Ativar a DAG:**
```bash
docker exec airflow-scheduler airflow dags unpause dataflow_operacoes_auxiliares
```

**Resultado esperado:**
```
Dag: dataflow_operacoes_auxiliares, paused: False
```

**4.2 — Acionar manualmente:**
```bash
docker exec airflow-scheduler airflow dags trigger dataflow_operacoes_auxiliares
```

**Resultado esperado:**
```
Created <DagRun dataflow_operacoes_auxiliares @ 2024-XX-XXTXX:XX:XX+00:00: manual__2024-...>
```

**4.3 — Monitorar execução:**
```bash
docker exec airflow-scheduler airflow tasks states-for-dag-run \
    dataflow_operacoes_auxiliares \
    "$(docker exec airflow-scheduler airflow dags list-runs -d dataflow_operacoes_auxiliares --no-backfill -o plain | tail -1 | awk '{print $3}')"
```

Ou, mais simples — acompanhe no **Airflow UI**:
1. Acesse http://localhost:8081
2. Clique em `dataflow_operacoes_auxiliares`
3. Vá na aba **Graph** para ver o fluxo visual
4. Clique em cada task para ver os **logs**

---

## Passo 5: Verificar logs de cada task

**Descrição:** Vamos inspecionar o log de uma task para confirmar que as template variables foram renderizadas corretamente.

**Comando:**
```bash
docker exec airflow-scheduler airflow tasks test \
    dataflow_operacoes_auxiliares \
    criar_diretorios_staging \
    2024-01-15
```

**Resultado esperado (trecho):**
```
=== Criando Diretórios de Staging ===
Data de execução: 2024-01-15
Data sem hífen: 20240115
...
Diretórios criados:
/tmp/dataflow/staging/2024-01-15
/tmp/dataflow/staging/2024-01-15/bronze
/tmp/dataflow/staging/2024-01-15/silver
/tmp/dataflow/staging/2024-01-15/gold
...
Pronto para receber dados de 2024-01-15
Diretório anterior (2024-01-14) também disponível para comparação
```

> **Carlos:** "Viu como `{{ ds }}` virou `2024-01-15` e `{{ prev_ds }}` virou `2024-01-14`? O Airflow fez o rendering antes de executar o bash. Isso é poderoso — os mesmos comandos funcionam para qualquer data, inclusive em reprocessamento (backfill)."

---

## Passo 6: Testar comportamento de falha (exit code)

**Descrição:** Vamos simular uma falha na verificação de disco para entender como o exit code afeta o estado da task.

**Comando (teste com parâmetro impossível):**
```bash
docker exec airflow-scheduler airflow dags trigger \
    dataflow_operacoes_auxiliares \
    --conf '{"disco_minimo_gb": 99999}'
```

> ⚠️ **Nota:** O parâmetro `disco_minimo_gb` precisa ser passado via `--conf` para sobrescrever o valor padrão em uma execução específica.

**O que observar no Airflow UI:**
1. A task `verificar_disco` ficará com status **Failed** (vermelho)
2. Todas as tasks downstream **não executarão** (cinza - upstream_failed)
3. O `exit 1` no script impediu o pipeline de continuar com disco cheio

**Para ver o log da falha:**
```bash
docker exec airflow-scheduler airflow tasks test \
    dataflow_operacoes_auxiliares \
    verificar_disco \
    2024-01-15
```

> **Marina:** "Esse padrão de 'guard task' é muito comum em produção. Antes de processar GBs de dados, você verifica se o sistema aguenta. Um `exit 1` simples protege o pipeline inteiro."

---

## Passo 7: Entender `params` — configuração sem alterar código

**Descrição:** O parâmetro `params` permite que a mesma DAG se comporte de forma diferente conforme a configuração, sem alterar o código Python.

**Como funciona:**

```python
# Na definição da DAG:
params={
    "disco_minimo_gb": 5,         # Acessível como {{ params.disco_minimo_gb }}
    "dias_retencao_tmp": 7,       # Acessível como {{ params.dias_retencao_tmp }}
    "webhook_url": "http://...",  # Acessível como {{ params.webhook_url }}
}
```

**No bash:**
```bash
MINIMO={{ params.disco_minimo_gb }}           # Vira: MINIMO=5
find /tmp -name "*.tmp" -mtime +{{ params.dias_retencao_tmp }} -delete  # Vira: -mtime +7
curl "{{ params.webhook_url }}"              # Vira: curl "http://httpbin.org/post"
```

**Sobrescrevendo via trigger:**
```bash
# Executa com retenção de 3 dias em vez de 7
docker exec airflow-scheduler airflow dags trigger \
    dataflow_operacoes_auxiliares \
    --conf '{"dias_retencao_tmp": 3}'
```

> **Carlos:** "Isso é ótimo para ambientes diferentes. Em desenvolvimento uso `disco_minimo_gb: 1`, em produção uso `disco_minimo_gb: 20`. Mesmo código, comportamento adaptável."

---

## Resumo

Neste exercício você aprendeu:

| Conceito | O que faz | Exemplo |
|----------|-----------|---------|
| **BashOperator** | Executa comandos shell como task | `bash_command="echo hello"` |
| **Template variables** | Injeta valores dinâmicos no comando | `{{ ds }}` → `2024-01-15` |
| **`params`** | Configuração customizável da DAG | `{{ params.disco_minimo_gb }}` |
| **`env`** | Variáveis de ambiente no script | `env={"KEY": "value"}` |
| **Exit codes** | Controlam sucesso/falha da task | `exit 1` → task falha |
| **Macros** | Funções para cálculo de datas | `{{ macros.ds_add(ds, 7) }}` |
| **Guard tasks** | Verificam pré-condições antes de processar | Disco, permissões, etc. |

**Padrões de uso do BashOperator na prática:**

| Caso de uso | Quando usar |
|-------------|-------------|
| Verificar pré-condições | Antes de processamento pesado |
| Criar diretórios | Estrutura de staging/output |
| Notificações | Alertas via curl/webhook |
| Limpeza | Remover arquivos antigos |
| Comandos do sistema | `df`, `du`, `ps`, `top` |
| Scripts externos | Chamar `.sh` existentes |

> **Marina:** "PythonOperator para lógica de negócio, BashOperator para infraestrutura e glue code. Essa é a divisão natural. No próximo exercício vocês vão combinar ambos em um pipeline mais sofisticado com scheduling."

---

## Próximo Exercício

No **Exercício 4** (intermediário), você vai criar uma DAG com `schedule_interval` e usar template variables `{{ ds }}` para processar dados particionados por data — combinando PythonOperator e BashOperator em um pipeline diário completo.
