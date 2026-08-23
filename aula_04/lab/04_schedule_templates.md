# Exercício 4 — DAG com Schedule e Template Variables: Processamento por Data (Intermediário)

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Até agora criamos DAGs que disparam manualmente. Mas o pipeline da DataFlow precisa rodar **todo dia automaticamente**, processando os dados do dia anterior. O Airflow tem um conceito poderoso chamado `execution_date` — quando a DAG roda no dia 16, ela processa dados do dia 15. Isso permite backfill: se caiu um dia, posso reprocessar uma data específica sem alterar código."

> **Marina Silva (CTO):** "Exatamente. E usamos as **template variables** — `{{ ds }}`, `{{ ds_nodash }}`, `{{ macros.ds_add(...) }}` — para tornar o código genérico. O mesmo pipeline funciona para qualquer data, seja execução agendada, manual ou backfill. Isso é a base de todo pipeline de dados em produção."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar uma DAG com `schedule_interval="@daily"` que executa automaticamente
- Usar `{{ ds }}` em BashOperator para referenciar a data de execução
- Acessar `context["ds"]` no PythonOperator para construir paths de entrada/saída
- Entender o conceito de **data interval**: a DAG do dia N processa dados do dia N-1
- Usar `{{ macros.ds_add(ds, -1) }}` para calcular datas relativas
- Testar uma DAG para uma data específica com `airflow tasks test`
- Diferenciar entre trigger manual (usa data atual) e backfill (usa data especificada)

## Pré-requisitos

- Exercícios 1, 2 e 3 concluídos
- Ambiente Airflow rodando (ver `00_setup.md`)
- Airflow UI acessível em http://localhost:8081
- Diretório de dados particionados disponível: `datasets/vendas_diarias/`

## O que vamos construir?

Uma DAG de **processamento diário de vendas** que:
- Roda todo dia automaticamente (`@daily`)
- Lê dados da partição correta (`vendas_diarias/dt=YYYY-MM-DD/`)
- Processa e grava resultados na partição de saída correspondente
- Usa template variables para que o mesmo código funcione para qualquer data

```
┌────────────────────┐     ┌──────────────────────┐     ┌─────────────────────┐
│ verificar_partição │────▶│ processar_vendas_dia │────▶│ gravar_resultado    │
│ (BashOperator)     │     │ (PythonOperator)     │     │ (PythonOperator)    │
└────────────────────┘     └──────────────────────┘     └─────────────────────┘
                                                                  │
                                                                  ▼
                                                        ┌─────────────────────┐
                                                        │ log_execucao        │
                                                        │ (BashOperator)      │
                                                        └─────────────────────┘
```

**Conceito-chave — execution_date e data interval:**

```
Dia 15 (00:00) ──────────── Dia 16 (00:00) ──────────── Dia 17 (00:00)
     │                           │
     │  ◀── data interval ──▶   │
     │                           │
     │                     DAG EXECUTA AQUI
     │                     execution_date = "2024-01-15"
     │                     (processa dados do dia 15)
```

> A DAG com `@daily` agendada no dia 16 tem `execution_date = 2024-01-15` — ela processa o **intervalo de dados que acabou de fechar** (o dia 15 inteiro).

---

## Exercício 4.1: Criar a DAG com Schedule Diário

### O que fazer

Crie o arquivo `dag_vendas_schedule.py` na pasta de DAGs do Airflow. A DAG deve:

1. Ter `schedule_interval="@daily"` (equivalente a `0 0 * * *`)
2. Definir `start_date` no passado (ex: `datetime(2024, 1, 1)`)
3. Usar `catchup=False` para não executar todas as datas passadas ao ativar
4. Ter `default_args` com 2 retries e retry_delay de 5 minutos

### Dicas

1. O `schedule_interval` aceita:
   - Presets: `"@daily"`, `"@hourly"`, `"@weekly"`, `"@monthly"`
   - Cron expressions: `"0 6 * * *"` (todo dia às 6h)
   - `None` para execução apenas manual
2. `catchup=False` é **essencial** em desenvolvimento — sem isso, ao ativar a DAG, o Airflow tentaria executar para CADA dia desde `start_date` até hoje
3. Em produção, `catchup=True` é útil para preencher dados históricos automaticamente
4. A DAG deve ter `tags` para organização: `["dataflow", "vendas", "diario", "schedule"]`

### Conceito importante — catchup:

| catchup | Comportamento ao ativar DAG |
|---------|----------------------------|
| `False` | Executa apenas a próxima data agendada |
| `True` | Executa TODAS as datas entre `start_date` e hoje |

### Critérios de Validação

- [ ] Arquivo criado na pasta correta de DAGs
- [ ] `schedule_interval="@daily"` configurado
- [ ] `catchup=False` definido
- [ ] `start_date` é uma data no passado
- [ ] DAG aparece no Airflow UI após ~30 segundos

---

## Exercício 4.2: Verificar Partição de Entrada com BashOperator

### O que fazer

Crie a primeira task da DAG: um `BashOperator` que verifica se a partição de dados do dia existe antes de processar. Use `{{ ds }}` para referenciar dinamicamente a data de execução.

O caminho esperado dos dados é: `/opt/airflow/data/vendas_diarias/dt={{ ds }}/`

### Dicas

1. Use `{{ ds }}` no `bash_command` — o Airflow substitui pela data de execução no formato `YYYY-MM-DD`
2. Verifique existência do diretório com `test -d` ou `ls`:
   ```bash
   # Exemplo de padrão (NÃO é a solução completa):
   if [ -d "/caminho/{{ ds }}" ]; then
       echo "Partição encontrada"
   else
       echo "ERRO: partição não encontrada"
       exit 1
   fi
   ```
3. Se a partição não existir, a task deve **falhar** (`exit 1`) para impedir processamento de dados inexistentes
4. Imprima informações úteis: data de execução, caminho verificado, tamanho dos dados

### Critérios de Validação

- [ ] Task usa `BashOperator` com `{{ ds }}` no caminho
- [ ] Verifica existência do diretório da partição
- [ ] Falha com `exit 1` se a partição não existir
- [ ] Imprime a data de execução e o caminho verificado no log

---

## Exercício 4.3: Processar Dados do Dia com PythonOperator

### O que fazer

Crie a task principal: um `PythonOperator` que recebe a data de execução via `context["ds"]` e simula o processamento dos dados daquela partição. A função deve:

1. Acessar `context["ds"]` para obter a data de execução
2. Construir o path de entrada: `vendas_diarias/dt={ds}/`
3. Construir o path de saída: `resultados/dt={ds}/`
4. Simular processamento (contar registros, calcular totais)
5. Passar resultado via XCom para a task seguinte

### Dicas

1. No Airflow 2+, `provide_context=True` é o **padrão** — basta usar `**context` na função:
   ```python
   def minha_funcao(**context):
       ds = context["ds"]           # "2024-01-15"
       ds_nodash = context["ds_nodash"]  # "20240115"
       # ... construir paths com a data
   ```
2. Também é possível usar `{{ ds }}` em `op_kwargs` para injetar a data como parâmetro:
   ```python
   PythonOperator(
       task_id="processar",
       python_callable=processar,
       op_kwargs={"data_execucao": "{{ ds }}"},
   )
   ```
3. Use `context["ti"].xcom_push(key="registros", value=N)` para enviar dados à próxima task
4. Para o conceito de "dados de ontem", use:
   ```python
   from datetime import datetime, timedelta
   dt = datetime.strptime(context["ds"], "%Y-%m-%d")
   ontem = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
   ```
5. A lógica de processamento pode ser simulada (print + XCom) — o importante é o uso correto de `context["ds"]`

### Conceito — provide_context no Airflow 2:

| Versão | Comportamento |
|--------|---------------|
| Airflow 1.x | Precisa de `provide_context=True` explícito |
| Airflow 2.x | Context é passado automaticamente quando `**context` está na assinatura |

### Critérios de Validação

- [ ] Função recebe `**context` e acessa `context["ds"]`
- [ ] Path de entrada construído dinamicamente com a data
- [ ] Path de saída construído dinamicamente com a data
- [ ] Resultado (registros processados) enviado via XCom
- [ ] A mesma função funciona para qualquer data sem alteração de código

---

## Exercício 4.4: Gravar Resultado e Usar Macros de Data

### O que fazer

Crie a terceira task que grava o resultado do processamento na partição de saída. Demonstre o uso de `{{ macros.ds_add(ds, -1) }}` para referenciar "ontem" — simulando um cenário onde a DAG precisa comparar dados do dia atual com o dia anterior.

### Dicas

1. Macros úteis para cálculo de datas no Jinja:
   ```
   {{ ds }}                          → 2024-01-15
   {{ macros.ds_add(ds, -1) }}       → 2024-01-14 (ontem)
   {{ macros.ds_add(ds, 7) }}        → 2024-01-22 (semana que vem)
   {{ macros.ds_format(ds, '%d/%m/%Y') }} → 15/01/2024
   ```
2. No `PythonOperator`, para calcular datas relativas use Python puro:
   ```python
   from datetime import datetime, timedelta
   ds = context["ds"]  # "2024-01-15"
   dt = datetime.strptime(ds, "%Y-%m-%d")
   ontem = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
   ```
3. Cenário: Carlos quer comparar o faturamento de hoje com o de ontem para detectar anomalias
4. Use XCom pull para pegar o resultado da task anterior:
   ```python
   registros = context["ti"].xcom_pull(task_ids="processar_vendas_dia", key="registros")
   ```
5. A task final (`log_execucao`) pode ser um `BashOperator` que usa `{{ ds }}` e `{{ macros.ds_add(ds, -1) }}` para registrar um log legível

### Critérios de Validação

- [ ] Task utiliza `{{ macros.ds_add(ds, -1) }}` ou equivalente Python para data anterior
- [ ] Resultado gravado em path particionado por data de execução
- [ ] XCom pull funciona corretamente para obter dados da task anterior
- [ ] Log final mostra data de execução e data de comparação (dia anterior)

---

## Exercício 4.5: Testar a DAG para uma Data Específica

### O que fazer

Use o comando `airflow tasks test` para executar tasks individualmente para uma data específica. Isso é essencial para:
- Desenvolvimento e debug de DAGs
- Reprocessamento de um dia que falhou
- Validar que a DAG funciona corretamente para datas passadas

### Comandos para testar

```bash
# Testar a task de verificação para 15 de janeiro
docker exec airflow-scheduler airflow tasks test \
    dag_vendas_schedule \
    verificar_particao \
    2024-01-15

# Testar o processamento para a mesma data
docker exec airflow-scheduler airflow tasks test \
    dag_vendas_schedule \
    processar_vendas_dia \
    2024-01-15
```

### Dicas

1. `airflow tasks test` executa UMA task para UMA data, sem registrar no metadata DB
2. Útil para debug — mostra logs completos no terminal
3. O formato é: `airflow tasks test <dag_id> <task_id> <execution_date>`
4. A data fornecida se torna o `{{ ds }}` / `context["ds"]` daquela execução
5. XComs não são persistidos no `test` — use `airflow tasks run` para execução completa

### O que observar

- Verifique que `{{ ds }}` foi substituído por `2024-01-15` nos logs
- Confirme que os paths construídos contêm a data correta
- Teste com diferentes datas para confirmar que o código é genérico

### Critérios de Validação

- [ ] Comando `airflow tasks test` executa sem erros para a data especificada
- [ ] Nos logs, `{{ ds }}` aparece substituído pela data (`2024-01-15`)
- [ ] O mesmo comando funciona com outra data (ex: `2024-01-20`) sem alterar código
- [ ] Você entende a diferença entre `tasks test` (debug) e `tasks run` (produção)

---

## Exercício 4.6: Entender Trigger vs Backfill

### O que fazer

Compare o comportamento de disparo manual (trigger) com backfill para entender quando usar cada um.

### Cenário

Carlos percebe que o pipeline falhou nos dias 10, 11 e 12 de janeiro. Ele precisa reprocessar esses 3 dias. Quais são as opções?

### Opção A — Trigger manual (um por um):

```bash
# Dispara para cada dia individualmente
docker exec airflow-scheduler airflow dags trigger \
    dag_vendas_schedule \
    --exec-date "2024-01-10"

docker exec airflow-scheduler airflow dags trigger \
    dag_vendas_schedule \
    --exec-date "2024-01-11"

docker exec airflow-scheduler airflow dags trigger \
    dag_vendas_schedule \
    --exec-date "2024-01-12"
```

### Opção B — Backfill (range de datas):

```bash
# Reprocessa um intervalo inteiro de uma vez
docker exec airflow-scheduler airflow dags backfill \
    dag_vendas_schedule \
    --start-date "2024-01-10" \
    --end-date "2024-01-12" \
    --reset-dagruns
```

### Comparação

| Aspecto | Trigger Manual | Backfill |
|---------|---------------|----------|
| Quantas datas | 1 por comando | Range inteiro |
| `execution_date` | Data especificada ou agora | Cada data do range |
| Registro no DB | Cria DagRun | Cria DagRuns para cada data |
| Uso | Reprocessar 1 dia | Preencher gaps históricos |
| `catchup` | Não depende | Não depende (ignora config) |

### Dicas

1. O **backfill** é a grande vantagem de usar `{{ ds }}` em vez de `datetime.now()`:
   - Com `{{ ds }}`: funciona para qualquer data, passada ou futura
   - Com `datetime.now()`: sempre processaria dados de "agora", impossibilitando reprocessamento
2. Quando `depends_on_past=True`, o backfill respeita a ordem cronológica
3. `--reset-dagruns` limpa execuções anteriores antes de reprocessar (idempotência)
4. Em produção, Carlos usaria backfill para preencher 3 dias de falha sem escrever código novo

### Critérios de Validação

- [ ] Você sabe executar um trigger para uma data específica
- [ ] Você sabe executar um backfill para um range de datas
- [ ] Você entende por que `{{ ds }}` é superior a `datetime.now()` para pipelines de dados
- [ ] Você sabe quando usar trigger (1 data) vs backfill (múltiplas datas)
- [ ] A DAG é **idempotente**: rodar 2x para a mesma data produz o mesmo resultado

---

## Resumo e Conceitos-Chave

Ao completar este exercício, você domina os conceitos fundamentais de **scheduling e template variables** no Airflow:

```
╔══════════════════════════════════════════════════════════════════════╗
║        RESUMO: SCHEDULE + TEMPLATE VARIABLES                         ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  📅 Schedule:     @daily (executa 1x/dia automaticamente)            ║
║  📆 execution_date: Data DO INTERVALO processado (não "agora")       ║
║  🔧 {{ ds }}:     Injetado automaticamente em Bash e Python          ║
║  🧮 macros:       ds_add, ds_format para cálculos de data            ║
║  🔄 Backfill:     Reprocessa range de datas sem alterar código       ║
║  ✅ Idempotência: Mesmo resultado ao rodar 2x para mesma data        ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Conceitos Praticados

| Conceito | No BashOperator | No PythonOperator |
|----------|-----------------|-------------------|
| Data de execução | `{{ ds }}` | `context["ds"]` |
| Data sem hífen | `{{ ds_nodash }}` | `context["ds_nodash"]` |
| Data anterior | `{{ macros.ds_add(ds, -1) }}` | `dt - timedelta(days=1)` |
| Formatar data | `{{ macros.ds_format(ds, '%d/%m') }}` | `dt.strftime('%d/%m')` |

### Anti-patterns — O que NÃO fazer:

| ❌ Errado | ✅ Correto | Por quê |
|-----------|-----------|---------|
| `datetime.now()` | `context["ds"]` | `now()` impede backfill/reprocessamento |
| Path hardcoded: `"/data/2024-01-15/"` | `f"/data/{ds}/"` | Não funciona para outras datas |
| `catchup=True` sem necessidade | `catchup=False` em dev | Evita executar 365 DAGs ao ativar |
| Ignorar `execution_date` | Usar `{{ ds }}` em todo path | Base da idempotência |

> **Marina:** "Com schedule e template variables, o pipeline da DataFlow é completamente genérico. O mesmo código processa dados de ontem, de um mês atrás, ou de qualquer data que precisemos reprocessar. Isso é o que separa pipelines de produção de scripts manuais."

---

## Próximo Exercício

➡️ **Exercício 5 — Retry e Error Handling** (`05_retry_error_handling.md`): configurar estratégias de retry, `on_failure_callback`, alertas quando tasks falham, e `email_on_failure` para que Carlos saiba imediatamente quando algo dá errado no pipeline diário.
