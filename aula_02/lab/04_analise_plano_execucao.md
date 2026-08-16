# Exercício 4 — Análise de Plano de Execução (Intermediário)

## Contexto

> **Carlos Mendes (Eng. Sênior):** "No exercício anterior vocês viram como `explain()` revela a presença de UDFs no plano. Mas o verdadeiro poder do explain está em diagnosticar operações custosas como joins, shuffles e agregações. Hoje vamos dissecar planos de execução de queries reais — joins entre tabelas grandes, filtros com pushdown e otimizações automáticas do Catalyst."

## Objetivos

Ao final deste exercício, você será capaz de:

- Interpretar nós de join no plano físico (SortMergeJoin vs BroadcastHashJoin)
- Identificar operações Exchange (shuffle) e seu impacto
- Verificar predicate pushdown em filtros
- Observar column pruning no ReadSchema
- Diagnosticar gargalos em pipelines complexos

## Pré-requisitos

- Exercícios 1, 2 e 3 concluídos
- SparkSession ativa com `df_vendas` (1M registros) e `df_clientes` (500K registros) carregados
- Jupyter Notebook acessível em http://localhost:8888

## Duração Estimada

⏱️ ~10 minutos

## Nível

🟡 **Intermediário** — Dicas e indicações do que observar, sem solução completa.

---

## Exercício 4.1: Analisar Plano de Join sem Broadcast

### O que fazer

Execute um join entre `df_vendas` (1M registros) e `df_clientes` (500K registros) por `customer_id` e analise o plano de execução com `explain(mode="formatted")`.

### O que procurar no plano

- **SortMergeJoin** — o Spark escolhe esse tipo de join quando ambas as tabelas são grandes
- **Exchange hashpartitioning** — aparece antes do join, indica shuffle (redistribuição de dados pela rede)
- **Sort** — ordenação necessária antes do merge

### Dicas

```python
df_join = df_vendas.join(df_clientes, "customer_id", "inner")
df_join.explain(mode="formatted")
```

- Conte quantos nós `Exchange` aparecem no plano (devem ser 2 — um para cada lado do join)
- Cada Exchange significa que dados estão sendo redistribuídos entre partições

### Observação esperada

O plano mostrará dois `Exchange hashpartitioning(customer_id)` seguidos de `Sort` e depois `SortMergeJoin`. Isso significa que o Spark precisa redistribuir **ambos** os DataFrames pela chave de join antes de executar a operação.

---

## Exercício 4.2: Analisar Plano com Broadcast Join

### O que fazer

Execute o mesmo join, mas agora usando `broadcast()` na tabela menor. Compare o plano de execução com o exercício anterior.

### O que procurar no plano

- **BroadcastHashJoin** — substitui o SortMergeJoin
- **BroadcastExchange** — cópia da tabela menor para todos os executors
- Ausência de `Exchange` no lado da tabela grande (sem shuffle do df_vendas!)

### Dicas

```python
from pyspark.sql.functions import broadcast

df_join_broadcast = df_vendas.join(broadcast(df_clientes), "customer_id", "inner")
df_join_broadcast.explain(mode="formatted")
```

- Compare: quantos `Exchange` existiam antes vs agora?
- O `BroadcastExchange` transmite a tabela menor inteira para cada executor
- A tabela grande (`df_vendas`) **não se move** — o Spark percorre localmente

### Observação esperada

Apenas um `BroadcastExchange` (tabela de clientes) aparece no plano. O `df_vendas` não precisa de shuffle — grande economia de I/O de rede. O join físico muda de `SortMergeJoin` para `BroadcastHashJoin`.

---

## Exercício 4.3: Predicate Pushdown

### O que fazer

Compare dois cenários:
1. Filtrar `df_vendas` por `shipping_state = 'SP'` **antes** de um join
2. Filtrar **após** o join

Use `explain()` em ambos e verifique se o Catalyst empurra o filtro para o scan.

### O que procurar no plano

- **PushedFilters** dentro do nó `FileScan parquet` — indica que o filtro foi empurrado para a leitura
- Posição do nó `Filter` no plano — aparece antes ou depois do join?

### Dicas

```python
# Cenário A: filtro ANTES do join
df_filtrado = df_vendas.filter(col("shipping_state") == "SP")
df_resultado_a = df_filtrado.join(df_clientes, "customer_id")
df_resultado_a.explain(mode="formatted")

# Cenário B: filtro DEPOIS do join
df_resultado_b = df_vendas.join(df_clientes, "customer_id") \
    .filter(col("shipping_state") == "SP")
df_resultado_b.explain(mode="formatted")
```

- O Catalyst Optimizer é inteligente: mesmo no cenário B, ele pode empurrar o filtro para antes do join
- Procure `PushedFilters: [IsNotNull(...), EqualTo(shipping_state, SP)]` no scan

### Observação esperada

Em ambos os casos o plano físico deve ser **idêntico ou muito similar** — o Catalyst move o filtro automaticamente para perto do scan (predicate pushdown). Isso reduz dados antes do join, otimizando performance sem intervenção manual.

---

## Exercício 4.4: Column Pruning

### O que fazer

A partir de `df_vendas` (que tem 12+ colunas), selecione apenas 3 colunas e verifique o `ReadSchema` no plano de execução.

### O que procurar no plano

- **ReadSchema** no nó `FileScan parquet` — lista apenas as colunas necessárias
- Compare com um `explain()` sem select (todas as colunas no ReadSchema)

### Dicas

```python
# Apenas 3 colunas
df_reduzido = df_vendas.select("order_id", "customer_id", "total_amount")
df_reduzido.explain(mode="formatted")

# Compare com todas as colunas
df_vendas.explain(mode="formatted")
```

- No formato Parquet, o column pruning é especialmente eficiente — lê apenas as colunas necessárias do disco
- Procure a linha `ReadSchema: struct<order_id:string,customer_id:string,total_amount:double>`

### Observação esperada

O `ReadSchema` mostrará **apenas as 3 colunas selecionadas**, mesmo que o arquivo Parquet contenha 12+. Isso significa que o Spark nem lê os bytes das colunas não utilizadas — otimização automática em formatos colunares.

---

## Exercício 4.5: Identificar Gargalos em Pipeline Complexo

### O que fazer

Crie um pipeline com join + groupBy + filter + orderBy e identifique quais operações geram `Exchange` (shuffle).

### O que procurar no plano

- Cada `Exchange` indica redistribuição de dados (shuffle) — é a operação mais cara em Spark distribuído
- Conte quantos shuffles o pipeline inteiro produz

### Dicas

```python
df_pipeline = df_vendas \
    .join(df_clientes, "customer_id") \
    .groupBy("shipping_state") \
    .agg(sum("total_amount").alias("faturamento")) \
    .filter(col("faturamento") > 100000) \
    .orderBy(col("faturamento").desc())

df_pipeline.explain(mode="formatted")
```

- **Join** → gera Exchange (shuffle por `customer_id`)
- **groupBy** → gera Exchange (shuffle por `shipping_state`)
- **orderBy** → gera Exchange (shuffle para ordenação global)
- **filter** após groupBy → NÃO gera Exchange (opera localmente)
- Total esperado: 3-4 operações Exchange

### Observação esperada

O pipeline gera múltiplos shuffles. O join redistribui por `customer_id`, o groupBy redistribui por `shipping_state`, e o orderBy redistribui para ordenação global. O filter pós-agregação não gera shuffle. Em produção, reduzir shuffles é a principal alavanca de performance — considere broadcast join e repartição estratégica.

---

## Resumo Rápido

| Conceito | O que indica no explain() | Impacto |
|----------|--------------------------|---------|
| Exchange | Shuffle (redistribuição de dados) | Alto — I/O de rede |
| SortMergeJoin | Join com shuffle em ambos os lados | Alto — 2 shuffles |
| BroadcastHashJoin | Tabela menor copiada para executors | Baixo — sem shuffle da tabela grande |
| PushedFilters | Filtro empurrado para o scan | Positivo — menos dados lidos |
| ReadSchema | Colunas efetivamente lidas | Positivo — column pruning |

> **💡 Dica de Carlos:** "Em produção, sempre rode `explain()` antes de aprovar uma query. Conte os `Exchange` — cada um é um shuffle potencialmente custoso. Se possível, use broadcast para tabelas < 100MB e filtre o mais cedo possível para reduzir dados antes dos joins."

---

## Referências

- [Understanding Spark Query Plans](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Broadcast Join Hints](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)
- [Catalyst Optimizer Internals](https://spark.apache.org/docs/latest/sql-programming-guide.html)
