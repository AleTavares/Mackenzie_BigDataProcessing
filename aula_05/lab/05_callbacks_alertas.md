# Exercício 5 — Callbacks e Alertas Automáticos

## Duração Estimada

⏱️ ~12 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, ontem o job Spark de processamento de vendas falhou às 3h da madrugada e só descobrimos às 9h quando a Ana perguntou do relatório. São 6 horas de SLA perdido! Precisamos de **alertas automáticos** — quando qualquer pipeline falhar, a equipe precisa ser notificada na hora."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Perfeito, Marina. O Airflow tem um mecanismo de **callbacks** — funções Python que são disparadas automaticamente quando uma task muda de estado. Vou configurar um `on_failure_callback` que simula um alerta no Slack com todos os detalhes da falha: qual task, qual DAG, a data de execução e a exceção que causou o problema. E de bônus, um `on_success_callback` para logar métricas de execução."

> **Marina Silva (CTO):** "Excelente. E se uma task demorar mais que o esperado sem necessariamente falhar? Quero saber disso também."

> **Carlos Mendes:** "Aí usamos **SLA** (Service Level Agreement). Definimos um tempo máximo esperado e, se a task ultrapassar, o Airflow dispara um `sla_miss_callback`. Não mata a task — apenas nos avisa que algo está lento."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender o conceito de callbacks no Airflow: funções acionadas por mudanças de estado
- Implementar `on_failure_callback` que simula um alerta Slack com detalhes da falha
- Implementar `on_success_callback` para registro de métricas de execução
- Explorar o dicionário `context` disponível dentro de um callback
- Aplicar callbacks via `default_args` (todas as tasks) e per-task (tasks específicas)
- Configurar SLA com `sla=timedelta()` e `sla_miss_callback`
- Criar uma DAG com falha intencional para validar que os callbacks disparam corretamente

## Pré-requisitos

- Exercícios 01 a 04 concluídos (Branching, FileSensor, TaskGroups, SparkSubmit)
- Ambiente Docker com Airflow rodando (ver `aula_04/lab/00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Familiaridade com `default_args` e `PythonOperator`

## Conceito: O que são Callbacks?

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  CALLBACKS NO AIRFLOW: FUNÇÕES ACIONADAS POR EVENTOS                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Callback = função Python chamada AUTOMATICAMENTE quando algo acontece       ║
║                                                                              ║
║  ┌──────────────────────────────────────────────────────────────────────┐    ║
║  │  Task lifecycle (eventos que disparam callbacks)                     │    ║
║  │                                                                      │    ║
║  │  ┌─────────┐     ┌─────────┐     ┌───────────┐     ┌────────────┐  │    ║
║  │  │ running │────▶│ success │────▶│ callback! │     │ on_success │  │    ║
║  │  └────┬────┘     └─────────┘     └───────────┘     └────────────┘  │    ║
║  │       │                                                              │    ║
║  │       │ falha                                                        │    ║
║  │       ▼                                                              │    ║
║  │  ┌─────────┐     ┌───────────┐     ┌────────────┐                  │    ║
║  │  │ failed  │────▶│ callback! │────▶│ on_failure │                   │    ║
║  │  └─────────┘     └───────────┘     └────────────┘                  │    ║
║  │                                                                      │    ║
║  │  ┌─────────────┐     ┌───────────┐     ┌────────────┐              │    ║
║  │  │ up_for_retry│────▶│ callback! │────▶│ on_retry   │              │    ║
║  │  └─────────────┘     └───────────┘     └────────────┘              │    ║
║  │                                                                      │    ║
║  └──────────────────────────────────────────────────────────────────────┘    ║
║                                                                              ║
║  Casos de uso reais:                                                         ║
║  • on_failure → enviar alerta no Slack/Teams/PagerDuty                       ║
║  • on_success → registrar métricas, atualizar dashboard                      ║
║  • on_retry   → incrementar contador de instabilidade                        ║
║  • sla_miss   → avisar que task está lenta (sem matar)                       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### O dicionário `context` — o que está disponível?

Todo callback recebe um dicionário `context` com informações completas sobre a execução:

| Chave | Tipo | Descrição |
|-------|------|-----------|
| `task_instance` | TaskInstance | Objeto com task_id, dag_id, state, try_number |
| `ds` | str | Data de execução (ex: `"2024-01-15"`) |
| `execution_date` | datetime | Data/hora completa da execução |
| `dag_run` | DagRun | Objeto do dag_run atual |
| `exception` | Exception | A exceção que causou a falha (só em on_failure) |
| `dag` | DAG | Referência ao objeto DAG |
| `params` | dict | Parâmetros passados à DAG |
| `task` | BaseOperator | O operador/task que disparou |

---

## Exercício 5.1: Implementar `on_failure_callback` — Alerta de Falha

### O que fazer

Crie uma DAG em `aula_05/code/dags/dag_callbacks_alertas.py` que demonstra callbacks. Comece implementando uma função `alerta_falha_slack` que será chamada automaticamente quando qualquer task falhar.

A função deve:
1. Extrair do `context`: `task_id`, `dag_id`, `execution_date` e `exception`
2. Formatar uma mensagem simulando um alerta Slack com emoji 🚨
3. Imprimir a mensagem formatada (em produção seria uma chamada HTTP ao webhook do Slack)
4. Registrar o horário do alerta

### Dicas

1. A assinatura de um callback é sempre uma função que recebe `context` como argumento:
   ```python
   def alerta_falha_slack(context):
       # context é um dicionário com toda info da execução
       ti = context["task_instance"]
       ...
   ```

2. Para extrair a exceção que causou a falha:
   ```python
   exception = context.get("exception", "Erro desconhecido")
   ```

3. Informações úteis disponíveis no `task_instance`:
   ```python
   ti = context["task_instance"]
   ti.task_id        # Nome da task
   ti.dag_id         # Nome da DAG
   ti.try_number     # Número da tentativa atual
   ti.log_url        # URL para os logs no Airflow UI
   ```

4. Use f-strings para montar a mensagem simulando o formato do Slack:
   ```python
   mensagem = f"""
   🚨 *ALERTA: Pipeline Falhou*
   • DAG: {dag_id}
   • Task: {task_id}
   • Data Execução: {ds}
   • Erro: {exception}
   • Tentativa: {ti.try_number}
   """
   ```

5. Aplique o callback em `default_args` para que **todas** as tasks da DAG o utilizem:
   ```python
   default_args = {
       "owner": "dataflow-analytics",
       "on_failure_callback": alerta_falha_slack,
       ...
   }
   ```

### Critérios de Validação

- [ ] Função `alerta_falha_slack` definida e recebe `context` como argumento
- [ ] Extrai `task_id`, `dag_id`, `execution_date` (ou `ds`) e `exception` do context
- [ ] Formata mensagem com formato visual de alerta (emoji + campos rotulados)
- [ ] Callback aplicado em `default_args` (cobertura para todas as tasks)
- [ ] Mensagem inclui o número da tentativa (`try_number`)

---

## Exercício 5.2: Implementar `on_success_callback` — Métricas de Execução

### O que fazer

Crie uma segunda função `registrar_metricas_sucesso` que será chamada quando uma task finalizar com sucesso. A função deve logar métricas úteis para monitoramento:

1. Duração da execução da task
2. Data de execução (para correlacionar com dados processados)
3. Task e DAG que completou com sucesso
4. Timestamp do registro

### Dicas

1. A duração da task pode ser calculada a partir do `task_instance`:
   ```python
   ti = context["task_instance"]
   duracao = ti.end_date - ti.start_date  # timedelta
   ```

2. Formate a saída como um log estruturado (simulando envio ao sistema de métricas):
   ```python
   print(f"📊 MÉTRICA | task={task_id} | duracao={duracao.total_seconds():.1f}s | ...")
   ```

3. Para aplicar este callback apenas em **tasks específicas** (não em todas via default_args):
   ```python
   carregar = PythonOperator(
       task_id="carregar_dados",
       python_callable=funcao_carregar,
       on_success_callback=registrar_metricas_sucesso,  # só nesta task
   )
   ```

4. Diferença entre aplicar no `default_args` vs per-task:

   | Onde aplicar | Efeito | Quando usar |
   |-------------|--------|-------------|
   | `default_args` | Todas as tasks da DAG | Alertas de falha (quer saber de qualquer falha) |
   | Per-task | Apenas a task específica | Métricas de tasks críticas (ex: o job principal) |

5. Um callback per-task **sobrescreve** o de `default_args` para aquele evento

### Critérios de Validação

- [ ] Função `registrar_metricas_sucesso` definida e recebe `context`
- [ ] Calcula duração da task usando `start_date` e `end_date`
- [ ] Loga pelo menos: task_id, duração em segundos, data de execução
- [ ] Aplicado per-task (em pelo menos uma task crítica) — não em default_args
- [ ] Formato de log estruturado (chave=valor) para facilitar parsing

---

## Exercício 5.3: Configurar SLA e `sla_miss_callback`

### O que fazer

Configure um **SLA (Service Level Agreement)** em uma task crítica da DAG. O SLA define o tempo máximo aceitável para a task completar. Se ultrapassar, o Airflow dispara um callback — mas **não mata** a task (ela continua executando).

Implemente:
1. Uma função `alerta_sla_violado` que avisa quando uma task está demorando demais
2. Aplique `sla=timedelta(minutes=30)` na task de processamento principal
3. Configure o `sla_miss_callback` no nível da DAG

### Dicas

1. O SLA é definido **por task** com o parâmetro `sla`:
   ```python
   from datetime import timedelta

   processar = PythonOperator(
       task_id="processar_dados",
       python_callable=funcao_processar,
       sla=timedelta(minutes=30),  # Se demorar >30min, dispara alerta
   )
   ```

2. O `sla_miss_callback` é definido **no nível da DAG**, não da task:
   ```python
   def alerta_sla_violado(dag, task_list, blocking_task_list, slas, blocking_tis):
       for sla in slas:
           print(f"⏰ SLA VIOLADO: task={sla.task_id}, dag={sla.dag_id}")
   ```

3. Atenção: a assinatura do `sla_miss_callback` é **diferente** dos outros callbacks — recebe 5 argumentos, não `context`:
   ```python
   with DAG(
       dag_id="dag_com_sla",
       sla_miss_callback=alerta_sla_violado,  # Nível DAG
       ...
   ) as dag:
   ```

4. SLA vs `execution_timeout`:

   | Mecanismo | O que faz | Mata a task? | Quando usar |
   |-----------|-----------|--------------|-------------|
   | `sla=timedelta(...)` | Dispara callback após tempo | ❌ Não | Monitoramento/alertas |
   | `execution_timeout=timedelta(...)` | Mata a task com timeout | ✅ Sim | Proteção contra travamento |

5. No lab, use `sla=timedelta(seconds=5)` para conseguir testar rapidamente (em produção seria minutos/horas)

### Critérios de Validação

- [ ] Função `alerta_sla_violado` implementada com a assinatura correta (5 parâmetros)
- [ ] Pelo menos uma task com `sla=timedelta(...)` definido
- [ ] `sla_miss_callback` configurado no nível da DAG (dentro do construtor `DAG(...)`)
- [ ] Mensagem de alerta SLA diferenciada visualmente dos alertas de falha (emoji ⏰)
- [ ] Entende a diferença: SLA = alerta sem matar, execution_timeout = mata a task

---

## Exercício 5.4: Criar Task com Falha Intencional para Testar Callbacks

### O que fazer

Para validar que seus callbacks funcionam de verdade, crie na mesma DAG uma task que **falha propositalmente**. Isso simula uma falha de pipeline em ambiente controlado e permite observar no Airflow UI se o `on_failure_callback` disparou corretamente.

A DAG final deve ter esta estrutura:

```
┌────────────────────┐     ┌──────────────────────────┐     ┌──────────────────┐
│ validar_ambiente   │────▶│ processar_dados          │────▶│ notificar_sucesso│
│ (simula sucesso)   │     │ (simula FALHA proposital)│     │ (on_success_cb)  │
└────────────────────┘     └──────────────────────────┘     └──────────────────┘
                                       │
                                       ▼
                           on_failure_callback dispara
                           → mensagem de alerta no log
```

### Dicas

1. Crie uma task que levanta exceção intencionalmente:
   ```python
   def processar_dados_falha(**context):
       """Simula uma falha para testar o callback de alerta."""
       import time
       time.sleep(2)  # Simula processamento antes de falhar
       raise Exception("Erro simulado: conexão com banco de dados perdida")
   ```

2. Crie uma task de sucesso para validar o `on_success_callback`:
   ```python
   def validar_ambiente(**context):
       """Task que completa com sucesso para testar on_success_callback."""
       print("✅ Ambiente validado com sucesso")
       return "ok"
   ```

3. Use `retries=0` na task que falha para ver o callback disparar imediatamente (sem esperar retries):
   ```python
   processar = PythonOperator(
       task_id="processar_dados",
       python_callable=processar_dados_falha,
       retries=0,  # Sem retry — falha direto e dispara callback
   )
   ```

4. Após rodar a DAG, verifique nos logs da task que falhou — o output do callback aparece lá

5. Na Airflow UI: task vermelha → clique → aba "Log" → procure pela mensagem 🚨 do callback

### Critérios de Validação

- [ ] DAG possui pelo menos 3 tasks com dependências definidas
- [ ] Uma task levanta exceção propositalmente (falha controlada)
- [ ] Task com falha usa `retries=0` para callback disparar imediatamente
- [ ] Ao executar a DAG, o `on_failure_callback` imprime a mensagem de alerta nos logs
- [ ] O `on_success_callback` dispara para as tasks que completam com sucesso
- [ ] A DAG não tem erros de import e aparece no Airflow UI

---

## Exercício 5.5: Executar e Verificar os Callbacks

### O que fazer

Execute a DAG e valide que os callbacks estão funcionando conforme esperado. Observe os logs no Airflow UI para confirmar que os alertas foram gerados.

### Passos

1. **Acionar a DAG** no Airflow UI ou via CLI:
   ```bash
   docker exec airflow-scheduler airflow dags trigger dag_callbacks_alertas
   ```

2. **Observar no Airflow UI:**
   - `validar_ambiente` → deve ficar verde (success)
   - `processar_dados` → deve ficar vermelho (failed)
   - `notificar_sucesso` → não deve executar (upstream failed)

3. **Verificar logs do `on_failure_callback`:**
   - Clique na task `processar_dados` (vermelha)
   - Vá na aba "Log"
   - Procure pela mensagem 🚨 com os detalhes da falha

4. **Verificar logs do `on_success_callback`:**
   - Clique na task `validar_ambiente` (verde)
   - Vá na aba "Log"
   - Procure pela mensagem 📊 com as métricas

### Dicas

1. Se a DAG não aparece no UI, verifique erros de import:
   ```bash
   docker exec airflow-scheduler airflow dags list-import-errors
   ```

2. O output do callback aparece **nos logs da task**, não em um local separado

3. Para re-executar após ver os resultados:
   ```bash
   docker exec airflow-scheduler airflow tasks clear dag_callbacks_alertas --yes
   ```

4. Compare a mensagem do callback com o que você veria em uma integração real:

   | Lab (print) | Produção |
   |-------------|----------|
   | `print("🚨 ALERTA...")` | POST para webhook do Slack |
   | `print("📊 MÉTRICA...")` | Envio para Prometheus/Datadog |
   | `print("⏰ SLA...")` | POST para PagerDuty |

### Critérios de Validação

- [ ] DAG executada pelo menos uma vez
- [ ] Logs da task falhada contêm a mensagem do `on_failure_callback`
- [ ] Logs da task bem-sucedida contêm a mensagem do `on_success_callback`
- [ ] Mensagem de falha inclui: task_id, dag_id, data de execução e exceção
- [ ] Comportamento observado no UI corresponde ao esperado (verde/vermelho)

---

## Resumo e Conceitos-Chave

```
╔══════════════════════════════════════════════════════════════════════╗
║   CALLBACKS E ALERTAS: VISIBILIDADE AUTOMÁTICA DO PIPELINE           ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  🚨 on_failure_callback → alerta quando task FALHA                   ║
║  📊 on_success_callback → registra métricas quando task COMPLETA     ║
║  🔄 on_retry_callback   → avisa quando task entra em RETRY           ║
║  ⏰ sla_miss_callback   → avisa quando task está LENTA               ║
║                                                                      ║
║  ┌─────────────────────────────────────────────────────────────┐     ║
║  │  Onde aplicar                                               │     ║
║  │                                                             │     ║
║  │  default_args = {                                           │     ║
║  │      "on_failure_callback": alerta_slack,  ← TODAS tasks   │     ║
║  │  }                                                          │     ║
║  │                                                             │     ║
║  │  task_critica = PythonOperator(                             │     ║
║  │      on_success_callback=metricas,  ← SÓ esta task         │     ║
║  │  )                                                          │     ║
║  │                                                             │     ║
║  │  DAG(sla_miss_callback=alerta_sla)  ← nível DAG            │     ║
║  │                                                             │     ║
║  └─────────────────────────────────────────────────────────────┘     ║
║                                                                      ║
║  O dicionário `context`:                                             ║
║  • context["task_instance"] → task_id, dag_id, try_number            ║
║  • context["ds"]            → data de execução (str)                 ║
║  • context["exception"]     → exceção que causou falha               ║
║  • context["execution_date"]→ datetime completo                      ║
║                                                                      ║
║  SLA vs execution_timeout:                                           ║
║  • sla = alerta sem matar (monitoramento)                            ║
║  • execution_timeout = mata a task (proteção)                        ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Comparação: Aplicação de Callbacks

| Callback | Onde aplicar | Assinatura | Quando dispara |
|----------|-------------|------------|----------------|
| `on_failure_callback` | default_args ou per-task | `func(context)` | Task falha (após todos retries) |
| `on_success_callback` | default_args ou per-task | `func(context)` | Task completa com sucesso |
| `on_retry_callback` | default_args ou per-task | `func(context)` | Task entra em retry |
| `sla_miss_callback` | Nível DAG | `func(dag, task_list, blocking_task_list, slas, blocking_tis)` | Task excede SLA |

### Anti-patterns

| ❌ Errado | ✅ Correto | Por quê |
|-----------|-----------|---------|
| Lógica pesada no callback (query, API longa) | Callback leve: enfileirar alerta | Callback roda no worker — bloqueia recursos |
| Ignorar `exception` no on_failure | Sempre incluir a exceção na mensagem | Sem a exceção, o alerta não ajuda a diagnosticar |
| Usar apenas `print` em produção | POST para Slack/PagerDuty webhook | Print só aparece nos logs internos |
| `on_failure_callback` com retry > 0 sem entender | Saber que callback dispara APÓS todos retries | Callback de falha só roda quando desistiu de retry |
| Confundir SLA com execution_timeout | SLA = avisa; timeout = mata | São mecanismos complementares, não substitutos |

---

## ✅ Checklist de Conclusão

- [ ] Entendi que callbacks são funções acionadas automaticamente por eventos do Airflow
- [ ] Implementei `on_failure_callback` que simula alerta Slack com detalhes da falha
- [ ] Implementei `on_success_callback` que registra métricas de duração
- [ ] Explorei o dicionário `context` e sei quais informações estão disponíveis
- [ ] Entendi a diferença entre aplicar callbacks via `default_args` (global) vs per-task
- [ ] Configurei `sla` em uma task e `sla_miss_callback` na DAG
- [ ] Criei task com falha intencional e verifiquei que o callback disparou nos logs
- [ ] Entendi: SLA = alerta sem matar, execution_timeout = mata a task

---

## Próximo Exercício

➡️ **Exercício 6 — Desafio: Pipeline Completo** (`06_desafio_pipeline.md`): combinar tudo que aprendeu — FileSensor espera o arquivo, BranchPythonOperator decide o caminho, TaskGroups organizam, SparkSubmitOperator processa, e callbacks alertam a equipe. Um pipeline de produção completo orquestrado pelo Airflow.
