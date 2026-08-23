# Exercício 5 — Retry e Error Handling: Resiliência no Pipeline (Intermediário)

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "O pipeline diário da DataFlow está automatizado, mas todo dia eu acordo com medo de abrir o Airflow e ver tasks vermelhas. Ontem a API do parceiro A deu timeout 2 vezes — na terceira tentativa funcionou. Mas como a DAG não tinha retry configurado, marquei falha e precisei disparar manualmente de manhã. Perdi 2 horas de SLA."

> **Marina Silva (CTO):** "Carlos, precisamos de **resiliência automática**. A maioria das falhas em pipelines de dados são transitórias — timeout de rede, disco cheio por 30 segundos, API retornando 503. Se configurarmos retry com backoff exponencial, 90% das falhas se resolvem sozinhas. E quando realmente falhar de vez, preciso de um callback que nos avise no Slack para agirmos rápido."

## Objetivos

Ao final deste exercício, você será capaz de:

- Configurar `retries` e `retry_delay` em `default_args` (nível DAG) e por task individual
- Ativar `retry_exponential_backoff=True` para intervalos crescentes entre tentativas
- Criar uma task que falha propositalmente para observar o comportamento de retry na UI
- Usar `on_failure_callback` para executar lógica customizada quando todas as tentativas se esgotam
- Usar `on_success_callback` para ações pós-conclusão (ex: log de métricas)
- Configurar `execution_timeout` para matar tasks que travam
- Identificar os estados de uma task durante retry: `running → failed → up_for_retry → running → success/failed`
- Limpar (clear) uma task falha na UI para forçar re-execução manual

## Pré-requisitos

- Exercícios 1 a 4 concluídos
- Ambiente Airflow rodando (ver `00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Familiaridade com `default_args`, `PythonOperator` e template variables

## O que vamos construir?

Uma DAG de **processamento com resiliência** que simula falhas transitórias e demonstra o mecanismo completo de retry e error handling do Airflow:

```
┌──────────────────┐     ┌──────────────────────┐     ┌──────────────────┐
│ extrair_parceiro │────▶│ transformar_dados     │────▶│ carregar_datalake│
│ (falha aleatória)│     │ (execution_timeout)   │     │ (on_success_cb)  │
└──────────────────┘     └──────────────────────┘     └──────────────────┘
        │                                                       │
        │ retry 3x                                              │
        │ exponential backoff                                    │
        ▼                                                       ▼
  on_failure_callback                                  on_success_callback
  (alerta simulado)                                    (log de métricas)
```

**Ciclo de vida de uma task com retry:**

```
                    ┌───────────────────────────────────────────────────────┐
                    │                                                       │
  ┌─────────┐    ┌─┴───────┐    ┌──────────────┐    ┌─────────┐    ┌─────┴────┐
  │ queued  │───▶│ running │───▶│ up_for_retry │───▶│ running │───▶│ success  │
  └─────────┘    └────┬────┘    └──────────────┘    └────┬────┘    └──────────┘
                      │                                   │
                      │ (falha final após N retries)      │
                      ▼                                   ▼
                ┌──────────┐                        ┌──────────┐
                │  failed  │                        │  failed  │
                └──────────┘                        └──────────┘
                      │
                      ▼
              on_failure_callback
```

---

## Exercício 5.1: Configurar Retries nos default_args vs Per-Task

### O que fazer

Crie o arquivo `dag_retry_resiliente.py` na pasta de DAGs do Airflow. Configure retry em dois níveis:

1. **`default_args`** (nível DAG): aplica-se a TODAS as tasks como padrão
2. **Per-task override**: uma task específica com configuração diferente

A DAG deve ter:
- `default_args` com 2 retries e retry_delay de 1 minuto
- Uma task de extração com override: 4 retries, retry_delay de 30 segundos, e `retry_exponential_backoff=True`
- Uma task de transformação com `execution_timeout` de 2 minutos

### Dicas

1. Parâmetros de retry em `default_args` funcionam como **fallback** — qualquer task pode sobrescrever:
   ```python
   default_args = {
       "retries": 2,
       "retry_delay": timedelta(minutes=1),
   }
   ```
2. Para exponential backoff, adicione na task (não em default_args):
   ```python
   PythonOperator(
       task_id="minha_task",
       retry_exponential_backoff=True,
       max_retry_delay=timedelta(minutes=10),  # teto do backoff
       ...
   )
   ```
3. O backoff exponencial funciona assim:
   - Tentativa 1: retry_delay × 1 = 30s
   - Tentativa 2: retry_delay × 2 = 60s
   - Tentativa 3: retry_delay × 4 = 120s (ou max_retry_delay se menor)
4. `execution_timeout` é um `timedelta` — se a task exceder esse tempo, o Airflow a mata com `AirflowTaskTimeout`
5. `retries` per-task sobrescreve o `default_args` — não é cumulativo

### Tabela de precedência:

| Parâmetro | default_args | Per-task | Resultado |
|-----------|-------------|----------|-----------|
| retries | 2 | 4 | 4 (per-task vence) |
| retry_delay | 1 min | 30s | 30s (per-task vence) |
| retry_exponential_backoff | — | True | True |
| execution_timeout | — | 2 min | 2 min |

### Critérios de Validação

- [ ] `default_args` define `retries` e `retry_delay` como fallback para todas as tasks
- [ ] Task de extração sobrescreve com mais retries e `retry_exponential_backoff=True`
- [ ] Task de transformação define `execution_timeout` para matar execuções travadas
- [ ] `max_retry_delay` limita o crescimento exponencial do backoff
- [ ] DAG aparece na UI sem erros de parsing

---

## Exercício 5.2: Criar Task com Falha Intencional para Observar Retry

### O que fazer

Crie uma task de extração que **simula falhas transitórias**: ela falha nas primeiras N execuções e depois tem sucesso. Isso permite observar o comportamento de retry na UI do Airflow.

A lógica deve simular o cenário real de Carlos: "A API do parceiro A retorna timeout nas 2 primeiras chamadas, mas na terceira funciona."

### Dicas

1. Para simular falha transitória, use uma variável de controle. Abordagem simples com arquivo:
   ```python
   import os
   
   def extrair_com_falha_transitoria(**context):
       # Arquivo de controle para simular tentativas
       arquivo_controle = "/tmp/tentativas_extracao.txt"
       
       # Ler tentativa atual
       tentativa = 1
       if os.path.exists(arquivo_controle):
           with open(arquivo_controle, "r") as f:
               tentativa = int(f.read().strip()) + 1
       
       # Gravar tentativa
       with open(arquivo_controle, "w") as f:
           f.write(str(tentativa))
       
       # ... lógica de falha/sucesso baseada na tentativa
   ```
2. Para forçar falha, use `raise Exception(...)` — isso faz a task entrar em `up_for_retry`:
   ```python
   raise Exception("Timeout na API do parceiro A (simulado)")
   ```
3. Abordagem alternativa usando `context["ti"].try_number`:
   - `try_number` começa em 1 e incrementa a cada retry
   - Não precisa de arquivo externo
   - Exemplo: falha se `try_number < 3`
4. Quando a task falha e tem retries disponíveis, o estado vai para `up_for_retry` (amarelo na UI)
5. Limpe o arquivo de controle no fim (sucesso) para poder testar novamente

### O que observar na UI

Após disparar a DAG, acompanhe a task na interface:

| Estado | Cor na UI | Significado |
|--------|-----------|-------------|
| running | verde-claro (pulsando) | Task executando |
| failed → up_for_retry | amarelo | Falhou, mas tem retries restantes |
| running (retry) | verde-claro novamente | Tentando novamente |
| success | verde-escuro | Sucesso após retry |
| failed (final) | vermelho | Esgotou todos os retries |

### Critérios de Validação

- [ ] Task de extração falha intencionalmente nas primeiras tentativas
- [ ] Após N tentativas, a task tem sucesso (simulando resolução de falha transitória)
- [ ] Na UI, é possível ver o estado `up_for_retry` entre as tentativas
- [ ] Nos logs da task, cada tentativa mostra a mensagem de erro e o número do retry
- [ ] `try_number` no log comprova qual tentativa está em execução

---

## Exercício 5.3: Implementar on_failure_callback e on_success_callback

### O que fazer

Adicione callbacks à DAG para que Carlos seja alertado quando uma task realmente falha (após esgotar todos os retries) e para registrar métricas de sucesso:

1. **`on_failure_callback`**: simula envio de alerta (Slack/email) quando a task falha definitivamente
2. **`on_success_callback`**: registra métricas de performance (duração, data processada)

### Dicas

1. Um callback é uma **função Python** que recebe `context` como argumento:
   ```python
   def alerta_falha(context):
       task_id = context["task_instance"].task_id
       dag_id = context["task_instance"].dag_id
       execution_date = context["ds"]
       exception = context.get("exception", "Desconhecido")
       
       # Em produção: enviar para Slack, PagerDuty, email...
       print(f"🚨 ALERTA: Task {task_id} falhou na DAG {dag_id}")
       print(f"   Data: {execution_date}")
       print(f"   Erro: {exception}")
   ```
2. Callbacks podem ser definidos em `default_args` (todas as tasks) ou por task:
   ```python
   # Em default_args — aplica a todas:
   default_args = {
       "on_failure_callback": alerta_falha,
   }
   
   # Per-task — sobrescreve default_args:
   PythonOperator(
       task_id="carregar",
       on_success_callback=registrar_metricas,
       ...
   )
   ```
3. O `on_failure_callback` só dispara **depois de esgotar TODOS os retries** — não dispara em cada falha intermediária
4. Para callback a cada retry (não apenas na falha final), use `on_retry_callback`
5. O `context` no callback contém tudo: `task_instance`, `dag_run`, `ds`, `exception`, etc.

### Comparação de callbacks:

| Callback | Quando dispara | Uso típico |
|----------|---------------|------------|
| `on_failure_callback` | Após esgotar TODOS os retries | Alerta para equipe |
| `on_success_callback` | Task concluiu com sucesso | Registrar métricas |
| `on_retry_callback` | A cada retry (antes de re-executar) | Log detalhado |

### Critérios de Validação

- [ ] `on_failure_callback` definido e dispara apenas quando retries se esgotam
- [ ] Callback imprime informações úteis: task_id, dag_id, data de execução, exceção
- [ ] `on_success_callback` registra métricas de performance na task de carregamento
- [ ] Callbacks NÃO impedem o funcionamento normal do retry (são independentes)
- [ ] Nos logs, é possível distinguir a mensagem do callback das mensagens normais da task

---

## Exercício 5.4: Configurar execution_timeout para Tasks Travadas

### O que fazer

Carlos percebeu que às vezes a task de transformação "trava" — fica executando por horas sem progresso (ex: lock em banco, deadlock, loop infinito). Configure `execution_timeout` para matar automaticamente tasks que excedem um tempo limite.

Crie uma task que **simula travamento** (sleep longo) e configure timeout para interrompê-la.

### Dicas

1. `execution_timeout` é um `timedelta` passado ao operador:
   ```python
   PythonOperator(
       task_id="transformar",
       python_callable=transformar_dados,
       execution_timeout=timedelta(minutes=2),
       ...
   )
   ```
2. Quando o timeout é atingido, o Airflow levanta `AirflowTaskTimeout` — a task **falha** e entra no fluxo normal de retry
3. Para simular travamento:
   ```python
   import time
   
   def transformar_dados_lento(**context):
       print("Iniciando transformação...")
       time.sleep(300)  # Simula 5 minutos de "travamento"
       print("Isso nunca será impresso se timeout < 5min")
   ```
4. O timeout **inclui** o tempo de execução do código Python — não é tempo de espera por recursos
5. Combinação típica em produção:
   - `execution_timeout=timedelta(minutes=30)` — mata se travar
   - `retries=2` — tenta de novo (pode ter sido transiente)
   - `retry_delay=timedelta(minutes=5)` — espera antes de tentar

### Cenário da DataFlow:

| Task | Tempo normal | execution_timeout | Justificativa |
|------|-------------|-------------------|---------------|
| Extração API | 2-5 min | 15 min | API lenta mas não infinita |
| Transformação Spark | 10-20 min | 45 min | Job pesado mas previsível |
| Carregamento | 1-3 min | 10 min | Escrita rápida no data lake |

### Critérios de Validação

- [ ] `execution_timeout` configurado em pelo menos uma task
- [ ] Task simulada com sleep que excede o timeout
- [ ] Ao exceder o timeout, a task é morta e entra em retry (se configurado)
- [ ] Nos logs, a exceção `AirflowTaskTimeout` é visível
- [ ] O timeout não afeta tasks que completam dentro do limite

---

## Exercício 5.5: Observar Estados na UI e Clear Manual

### O que fazer

Após executar a DAG com as tasks configuradas acima, pratique as seguintes ações na UI do Airflow:

1. **Observar a sequência de estados** durante retry
2. **Inspecionar logs** de cada tentativa (try_number)
3. **Clear** uma task falha para forçar re-execução manual
4. **Marcar como sucesso** uma task (útil em emergência)

### O que observar na UI

Ao clicar em uma task na Grid View ou Graph View:

```
┌─────────────────────────────────────────────────────────┐
│ Task Instance: extrair_parceiro                          │
├─────────────────────────────────────────────────────────┤
│ State: up_for_retry                                      │
│ Try Number: 2 of 4                                       │
│ Start Date: 2024-01-16 06:01:00                          │
│ Duration: 0.3s                                           │
│ Next Retry: 2024-01-16 06:02:00                          │
│                                                          │
│ [Log] [Clear] [Mark Success] [Mark Failed]               │
└─────────────────────────────────────────────────────────┘
```

### Como fazer o Clear:

1. Na UI, clique na task que falhou (ícone vermelho)
2. Clique em **"Clear"** no painel lateral
3. Marque "Include downstream" se quiser re-executar tasks dependentes também
4. Confirme — a task volta para `queued` e será re-executada pelo scheduler

### Dicas

1. **Clear** é diferente de **trigger**: clear re-executa UMA task específica para aquela data; trigger dispara a DAG inteira
2. O clear pode ser feito via CLI também:
   ```bash
   docker exec airflow-scheduler airflow tasks clear \
       dag_retry_resiliente \
       --task-regex "extrair_parceiro" \
       --start-date "2024-01-16" \
       --end-date "2024-01-16" \
       --yes
   ```
3. **Mark Success** é o "bypass de emergência" — marca a task como sucesso sem executá-la. Útil quando Carlos resolve o problema manualmente e quer que o pipeline continue
4. Nos logs, cada retry aparece com **try number** separado — é possível ver todos os logs de todas as tentativas
5. O `try_number` final = número de tentativas realizadas (1 + número de retries executados)

### Comparação: Clear vs Mark Success vs Trigger

| Ação | O que faz | Quando usar |
|------|-----------|-------------|
| Clear | Re-executa a task (e downstream opcionalmente) | Task falhou, problema foi resolvido |
| Mark Success | Marca como sucesso sem executar | Resolução manual, quer continuar pipeline |
| Trigger DAG | Executa toda a DAG do início | Nova execução completa |

### Critérios de Validação

- [ ] Você observou os estados `up_for_retry` (amarelo) na UI durante os retries
- [ ] Você inspecionou logs de diferentes try numbers na mesma task
- [ ] Você fez clear de uma task falha e ela foi re-executada automaticamente
- [ ] Você entende a diferença entre Clear, Mark Success e Trigger
- [ ] Você sabe usar clear via CLI para automação

---

## Resumo e Conceitos-Chave

Ao completar este exercício, você domina **resiliência e error handling** no Airflow:

```
╔══════════════════════════════════════════════════════════════════════╗
║        RESUMO: RETRY E ERROR HANDLING                                ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  🔄 Retries:         default_args (global) ou per-task (override)    ║
║  📈 Exponential:     retry_exponential_backoff=True (30s→60s→120s)   ║
║  ⏱️ Timeout:         execution_timeout mata tasks travadas            ║
║  🚨 on_failure_cb:   Alerta quando TODOS os retries falharam         ║
║  ✅ on_success_cb:   Ação pós-sucesso (métricas, notificação)        ║
║  🔃 Clear:           Re-executa task específica para uma data        ║
║  ⚡ Mark Success:    Bypass manual para continuar o pipeline         ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Parâmetros de Resiliência — Referência Rápida

| Parâmetro | Onde definir | Valor típico produção | Efeito |
|-----------|-------------|----------------------|--------|
| `retries` | default_args ou task | 2-5 | Quantas tentativas extras |
| `retry_delay` | default_args ou task | 1-5 min | Espera entre tentativas |
| `retry_exponential_backoff` | task | True | Delays crescentes |
| `max_retry_delay` | task | 10-30 min | Teto do backoff |
| `execution_timeout` | task | 15-60 min | Mata se exceder |
| `on_failure_callback` | default_args ou task | função | Alerta no Slack/email |
| `on_success_callback` | task | função | Log de métricas |
| `on_retry_callback` | task | função | Log a cada retry |

### Anti-patterns — O que NÃO fazer:

| ❌ Errado | ✅ Correto | Por quê |
|-----------|-----------|---------|
| `retries=50` | `retries=3` com backoff | 50 retries mascara bug real |
| Sem `execution_timeout` | Timeout realista por task | Tasks travadas consomem recursos |
| `on_failure` com lógica pesada | Callback leve (envio de alerta) | Callback lento trava o scheduler |
| Ignorar `up_for_retry` na UI | Monitorar estados | Detectar instabilidade cedo |
| `retry_delay=timedelta(seconds=1)` | Delay proporcional à causa | 1s não resolve timeout de rede |

### Cenário de produção da DataFlow:

```python
# Configuração que Carlos implementou após as falhas:
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alertar_slack_dataflow,
}
```

> **Marina:** "Com retry e exponential backoff, 90% das falhas transitórias se resolvem sozinhas. O `on_failure_callback` só dispara quando realmente temos um problema real — isso evita fadiga de alertas na equipe. E o `execution_timeout` garante que nenhuma task fique presa eternamente consumindo recursos do cluster."

---

## Próximo Exercício

➡️ **Exercício 6 — DAG Completa: Pipeline Diário com 6+ Tasks** (`06_desafio_pipeline_completo.md`): combinar tudo que aprendemos — scheduling, templates, retry, callbacks — em uma DAG de produção simulando o pipeline diário completo da DataFlow com extração, validação, transformação, carregamento e notificação.
