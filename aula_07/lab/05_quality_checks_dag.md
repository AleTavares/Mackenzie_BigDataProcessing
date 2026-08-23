# Exercício 5 — Adicionar Quality Checks como Task no DAG de Produção

## Duração Estimada

⏱️ ~12 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, o pipeline está orquestrando perfeitamente — o sensor espera o arquivo, o Spark processa, e a notificação confirma. Mas ontem entraram dados com valores negativos no faturamento e estados nulos na camada Gold. O Roberto gerou relatório executivo com números errados. Precisamos de um **quality gate** entre o processamento Spark e a notificação. Se os dados não passarem na validação, o pipeline PARA e alerta a equipe. Dados errados na Gold são piores do que dados atrasados."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Faz total sentido. Vou adicionar uma task `quality_checks` entre o `spark_submit` e a `notificação`. Ela vai ler a partição recém-escrita na Gold, validar regras básicas — não vazio, sem negativos, sem estados nulos, volume mínimo — e se qualquer check falhar, levanta exceção. Com o `on_failure_callback` já configurado, a equipe recebe alerta automaticamente. Se tudo passar, a notificação confirma que os dados estão prontos para consumo."

> **Marina Silva (CTO):** "Perfeito. Quero que use a mesma `data_ref={{ ds }}` para garantir que está validando a partição correta, não dados de outro dia."

## Objetivos

Ao final deste exercício, você será capaz de:

- Inserir uma task de validação de qualidade em um DAG existente
- Implementar checks de qualidade que leem dados da camada Gold
- Usar `{{ ds }}` para validar a partição correta (mesma data do processamento)
- Configurar falha automática (raise Exception) quando checks não passam
- Entender o padrão "fail-fast" — pipeline para antes de publicar dados ruins
- Integrar quality checks com o `on_failure_callback` já existente

## Pré-requisitos

- Exercício 04 concluído (`dag_pipeline_vendas.py` com sensor → spark → notificação)
- Aula 06 — conceitos de quality checks (completude, validade, integridade)
- Familiaridade com PythonOperator e passagem de parâmetros via `**context`

---

## O que você vai construir

Evolução do DAG do Exercício 04 com a adição de uma task de quality checks:

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│  DAG: dataflow_pipeline_vendas_producao (atualizada)                                  │
│  Schedule: @daily (06:00 UTC) | Catchup: False                                        │
│                                                                                       │
│  ┌────────────┐    ┌────────────────┐    ┌─────────────────┐    ┌────────────────┐   │
│  │ FileSensor  │───▶│ SparkSubmit    │───▶│ quality_checks  │───▶│ Notificação    │   │
│  │             │    │                │    │                 │    │                │   │
│  │ Espera      │    │ pipeline_      │    │ Lê Gold/{{ ds }}│    │ ✅ Confirma    │   │
│  │ arquivo     │    │ vendas.py      │    │ Valida 4 regras │    │ dados prontos  │   │
│  │ do dia      │    │ --data-ref     │    │ Falha se ❌     │    │ para consumo   │   │
│  └────────────┘    └────────────────┘    └─────────────────┘    └────────────────┘   │
│                                                                                       │
│  Quality Checks executados:                                                           │
│    1. Não vazio (count > 0)                                                           │
│    2. Sem valores negativos em faturamento                                            │
│    3. Sem estados nulos                                                               │
│    4. Volume acima de threshold mínimo                                                │
│                                                                                       │
│  Se checks falham → Exception → on_failure_callback → alerta equipe                  │
│  Se checks passam → notificação confirma dados publicados na Gold                     │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

**Por que essa posição no DAG?**

| Posição | Motivo |
|---------|--------|
| **Depois** do SparkSubmit | Os dados já foram escritos na Gold — agora validamos |
| **Antes** da Notificação | Só notificamos sucesso se dados estiverem corretos |
| Usa `{{ ds }}` | Valida exatamente a partição que foi processada, não dados antigos |
| Levanta Exception | Faz a task falhar → Airflow trata como falha → callback dispara |

---

## Problema

Modifique o arquivo `aula_07/code/dags/dag_pipeline_vendas.py` para adicionar uma task `quality_checks` que:

1. **Posição no fluxo:** executa APÓS o `SparkSubmitOperator` e ANTES da notificação
2. **Leitura dos dados:** lê a partição da camada Gold correspondente a `data_ref={{ ds }}`
3. **Executa 4 checks de qualidade:**
   - **Não vazio:** o DataFrame tem pelo menos 1 registro
   - **Sem negativos:** coluna `valor_total` não contém valores negativos
   - **Sem nulos em estado:** coluna `estado` não tem valores null
   - **Volume mínimo:** quantidade de registros acima de um threshold (ex: 100)
4. **Comportamento em falha:** se QUALQUER check falhar, levanta `Exception` com mensagem descritiva
5. **Comportamento em sucesso:** loga métricas e permite que o fluxo continue para notificação
6. **Data dinâmica:** usa `{{ ds }}` (via `**context`) para identificar a partição correta

---

## Dicas

### Dica 1: Estrutura da função de quality checks

A função recebe `**context` do Airflow para acessar a data de execução:

```python
def executar_quality_checks(**context):
    data_ref = context["ds"]  # "2024-01-15"
    
    # Caminho da partição Gold
    gold_path = f"/opt/spark/data/gold/vendas/data_ref={data_ref}"
    
    # Ler dados (SparkSession ou pandas — para o lab, pandas é suficiente)
    # ...
    
    # Lista para acumular falhas
    falhas = []
    
    # Check 1, 2, 3, 4...
    # ...
    
    if falhas:
        raise Exception(f"Quality checks FALHARAM para {data_ref}: {'; '.join(falhas)}")
    
    print(f"✅ Todos os quality checks passaram para {data_ref}")
```

### Dica 2: Implementando os 4 checks

Cada check é uma validação simples. Acumule falhas em uma lista para reportar todas de uma vez:

```python
# Check 1: Não vazio
if len(df) == 0:
    falhas.append("DataFrame vazio — nenhum registro na partição")

# Check 2: Sem negativos
negativos = df[df["valor_total"] < 0]
if len(negativos) > 0:
    falhas.append(f"{len(negativos)} registros com valor_total negativo")

# Check 3: Sem nulos em estado
# ... (similar pattern)

# Check 4: Volume mínimo
VOLUME_MINIMO = 100
# ... (similar pattern)
```

### Dica 3: Inserindo a task no fluxo existente

Você precisa alterar a cadeia de dependências. Antes era:

```python
sensor >> spark_submit >> notificacao
```

Agora precisa ser:

```python
sensor >> spark_submit >> quality_checks >> notificacao
```

A task usa `PythonOperator`:

```python
quality_checks = PythonOperator(
    task_id="quality_checks",
    python_callable=executar_quality_checks,
)
```

### Dica 4: Por que raise Exception?

Quando um `PythonOperator` levanta uma exceção não tratada, o Airflow marca a task como `failed`. Isso:
- Impede tasks downstream de executar (notificação não roda)
- Dispara o `on_failure_callback` já configurado no Exercício 04
- Permite que retries tentem novamente (se o problema for transiente)

É o padrão "fail-fast" — melhor parar do que propagar dados ruins.

### Dica 5: Logando métricas de sucesso

Quando todos os checks passam, é útil logar métricas para observabilidade:

```python
print(f"📊 Quality Report para {data_ref}:")
print(f"   • Total registros: {len(df)}")
print(f"   • Valores negativos: 0")
print(f"   • Estados nulos: 0")
print(f"   • Volume OK: {len(df)} >= {VOLUME_MINIMO}")
```

Esses logs aparecem na UI do Airflow e ajudam no diagnóstico futuro.

---

## Critérios de Validação

Verifique se sua implementação atende a **todos** os critérios:

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | Task `quality_checks` existe no DAG | Inspecionar código — `PythonOperator` com `task_id="quality_checks"` |
| 2 | Posição correta: após spark_submit, antes de notificação | Dependências: `spark_submit >> quality_checks >> notificacao` |
| 3 | Função usa `context["ds"]` para obter data dinâmica | Não tem data hardcoded — funciona para qualquer execução/backfill |
| 4 | Check de não-vazio implementado | `len(df) == 0` ou `df.empty` detectado |
| 5 | Check de valores negativos implementado | Filtra `valor_total < 0` e verifica contagem |
| 6 | Check de estados nulos implementado | Verifica `df["estado"].isnull().sum() > 0` ou equivalente |
| 7 | Check de volume mínimo implementado | Compara `len(df)` com threshold definido (ex: 100) |
| 8 | Levanta Exception se qualquer check falha | `raise Exception(...)` com mensagem descritiva |
| 9 | Loga métricas quando checks passam | Print ou logging com contagens e status |
| 10 | DAG não tem erros de sintaxe | `python dag_pipeline_vendas.py` executa sem erro |
| 11 | Mensagem de falha inclui quais checks falharam | Ex: "Quality checks FALHARAM: 5 registros com valor_total negativo; 12 estados nulos" |
| 12 | Lê dados do caminho correto da Gold com `data_ref` | Path inclui a partição correspondente à data de execução |

---

## Teste sua Implementação

**1. Verificar sintaxe:**
```bash
python aula_07/code/dags/dag_pipeline_vendas.py
```

**2. Simular checks passando (criar dados válidos no caminho esperado):**
```bash
# Criar diretório e arquivo de teste
mkdir -p /opt/spark/data/gold/vendas/data_ref=2024-01-15/
# Inserir parquet de teste com dados válidos
```

**3. Simular checks falhando (dados com problemas):**
Altere temporariamente o path ou insira dados com valores negativos para ver o `raise Exception` em ação.

**4. Verificar no Airflow UI:**
- Graph View deve mostrar 4 tasks em sequência linear
- Trigger manual → quality_checks deve aparecer como `success` ou `failed`
- Em caso de falha, verificar logs da task para a mensagem descritiva

---

## Conceitos Consolidados

| Conceito | Aula de Origem | Aplicação Aqui |
|----------|----------------|----------------|
| Quality checks (completude, validade) | Aula 06 - Exercícios 01-03 | Checks de não-vazio, negativos, nulos |
| Quality gate em DAG | Aula 06 - Exercício 05 | Task que para pipeline se dados inválidos |
| PythonOperator | Aula 04 - Exercício 01 | Task que executa função Python |
| `on_failure_callback` | Aula 07 - Exercício 04 | Já configurado — alerta dispara automaticamente |
| Template `{{ ds }}` / `context["ds"]` | Aula 04 - Exercício 04 | Identifica partição correta |
| Padrão fail-fast | Aula 06 - Conceitos | Melhor parar do que propagar dados ruins |

---

## Reflexão

Antes de seguir para o próximo exercício, considere:

1. **Ordem dos checks importa?** Se o DataFrame está vazio, faz sentido rodar os outros checks? (Dica: fail-fast no primeiro)
2. **Threshold dinâmico:** o volume mínimo de 100 faz sentido para todos os dias? E domingos, quando o volume é naturalmente menor?
3. **Granularidade:** esses 4 checks são suficientes para produção? O que mais você validaria? (Dica: freshness, schema drift, distribuição estatística)
4. **Alertas informativos:** quando a equipe recebe o alerta de falha, a mensagem tem informação suficiente para diagnosticar sem abrir o Airflow UI?

---

## Próximo Passo

No **Exercício 06** (Desafio), você vai montar o **Docker Compose completo** que integra todo o pipeline end-to-end de forma automatizada — Spark cluster, Airflow, volumes compartilhados, e o pipeline rodando do sensor até a notificação com quality checks, tudo containerizado e pronto para demonstrar em produção.
