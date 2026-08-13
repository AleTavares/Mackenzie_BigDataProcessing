# Exercício 2 — Agregações e Ordenação com PySpark

## Contexto

> **Ana Rodrigues (Product Owner):** "Pessoal, preciso de relatórios de vendas segmentados por estado, cidade, método de pagamento e mês. O board quer ver faturamento total, ticket médio e quantidade de pedidos — e tudo ordenado de forma que os maiores mercados apareçam primeiro. Consigo ter isso até o final do dia?"

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Tranquilo, Ana! Com Spark isso é questão de minutos. Vamos usar `groupBy` com funções de agregação e `orderBy` para gerar exatamente o que o board precisa. Vou te mostrar passo a passo."

## Objetivos

Ao final deste exercício, você será capaz de:

- Importar e utilizar funções de agregação do PySpark
- Agrupar dados com `groupBy` por uma ou mais colunas
- Aplicar múltiplas funções de agregação: `sum`, `avg`, `count`, `countDistinct`, `min`, `max`
- Utilizar `agg()` para combinar várias agregações em uma única operação
- Ordenar resultados com `orderBy` / `sort` (ascendente e descendente)
- Usar `describe()` e `summary()` para estatísticas descritivas rápidas
- Construir pipelines completos: filter → groupBy → agg → orderBy → show

## Pré-requisitos

- Exercício 1 concluído (SparkSession criada e `df_vendas` carregado)
- Ambiente Docker rodando (ver `00_setup.md`)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `vendas_2023.csv` disponível na pasta `data/`

## Duração Estimada

⏱️ ~30 minutos (segunda parte do Lab Parte 1)

---

## Passo 1: Importar Funções de Agregação

**Descrição:** Antes de usar funções como `sum`, `avg` e `count`, precisamos importá-las do módulo `pyspark.sql.functions`. Atenção: essas funções **não** são as built-in do Python — são funções Spark otimizadas para processamento distribuído.

**Código:**

```python
from pyspark.sql.functions import (
    sum, avg, count, countDistinct,
    min, max, round, desc, col
)

print("✅ Funções de agregação importadas com sucesso!")
print("   Funções disponíveis: sum, avg, count, countDistinct, min, max, round, desc")
```

**Resultado esperado:**

```
✅ Funções de agregação importadas com sucesso!
   Funções disponíveis: sum, avg, count, countDistinct, min, max, round, desc
```

**Explicação técnica:**

| Função | Descrição | Equivalente SQL |
|--------|-----------|-----------------|
| `sum("col")` | Soma de todos os valores | `SUM(col)` |
| `avg("col")` | Média aritmética | `AVG(col)` |
| `count("col")` | Contagem de valores não-nulos | `COUNT(col)` |
| `countDistinct("col")` | Contagem de valores únicos | `COUNT(DISTINCT col)` |
| `min("col")` | Menor valor | `MIN(col)` |
| `max("col")` | Maior valor | `MAX(col)` |
| `round(col, n)` | Arredondamento com `n` decimais | `ROUND(col, n)` |
| `desc("col")` | Ordenação descendente | `ORDER BY col DESC` |

- Essas funções são **distribuídas**: o Spark calcula parcialmente em cada worker e depois combina os resultados
- Diferente do `sum()` built-in do Python (que opera em listas locais), `pyspark.sql.functions.sum` opera em colunas de DataFrames distribuídos

> **💡 Dica de Carlos:** "Cuidado com o conflito de nomes! O `sum` do PySpark substitui o `sum` built-in do Python neste escopo. Se precisar do built-in depois, use `builtins.sum()`. Na prática, em notebooks de análise Spark, isso raramente é um problema."

---

## Passo 2: groupBy por Estado — Faturamento Total

**Descrição:** A primeira demanda da Ana é ver o faturamento total, contagem de pedidos e ticket médio por estado. Vamos usar `groupBy("shipping_state")` seguido de `agg()` para calcular múltiplas métricas de uma vez, e `orderBy` para apresentar os maiores mercados primeiro.

**Código:**

```python
# Faturamento por estado: total, contagem e ticket médio
faturamento_estado = df_vendas \
    .groupBy("shipping_state") \
    .agg(
        round(sum("total_amount"), 2).alias("faturamento_total"),
        count("order_id").alias("total_pedidos"),
        round(avg("total_amount"), 2).alias("ticket_medio")
    ) \
    .orderBy(desc("faturamento_total"))

print("📊 Faturamento por Estado (Top 10):")
faturamento_estado.show(10)
```

**Resultado esperado:**

```
📊 Faturamento por Estado (Top 10):
+--------------+-----------------+-------------+------------+
|shipping_state|faturamento_total|total_pedidos|ticket_medio|
+--------------+-----------------+-------------+------------+
|            SP|       12500000.0|        25000|      500.00|
|            RJ|        7800000.0|        15000|      520.00|
|            MG|        5200000.0|        12000|      433.33|
|            RS|        3100000.0|         7500|      413.33|
|            PR|        2800000.0|         7000|      400.00|
|            BA|        2400000.0|         6000|      400.00|
|            SC|        1900000.0|         5000|      380.00|
|            PE|        1600000.0|         4000|      400.00|
|            GO|        1200000.0|         3500|      342.86|
|            CE|        1100000.0|         3000|      366.67|
+--------------+-----------------+-------------+------------+
only showing top 10 rows
```

**Explicação técnica:**

- `groupBy("shipping_state")` — agrupa todas as linhas por estado (cria um grupo para cada valor distinto)
- `.agg(...)` — aplica funções de agregação dentro de cada grupo
- `sum("total_amount").alias("faturamento_total")` — soma o valor total e renomeia a coluna resultante
- `count("order_id")` — conta quantos pedidos existem por grupo
- `avg("total_amount")` — calcula a média (ticket médio)
- `round(..., 2)` — arredonda para 2 casas decimais (valores monetários)
- `.orderBy(desc("faturamento_total"))` — ordena do maior para o menor faturamento
- Equivalente SQL:
  ```sql
  SELECT shipping_state,
         ROUND(SUM(total_amount), 2) AS faturamento_total,
         COUNT(order_id) AS total_pedidos,
         ROUND(AVG(total_amount), 2) AS ticket_medio
  FROM vendas
  GROUP BY shipping_state
  ORDER BY faturamento_total DESC
  ```

> **💡 Dica de Ana:** "Agora consigo ver claramente que SP representa quase 25% do nosso faturamento. Esse tipo de visão é o que o board precisa para decidir onde investir em logística."

---

## Passo 3: groupBy por Método de Pagamento

**Descrição:** A Ana também quer entender a distribuição de vendas por método de pagamento. Isso ajuda o time comercial a negociar taxas com adquirentes e incentivar métodos com menor custo (como PIX).

**Código:**

```python
# Distribuição de vendas por método de pagamento
vendas_pagamento = df_vendas \
    .groupBy("payment_method") \
    .agg(
        count("order_id").alias("total_pedidos"),
        round(sum("total_amount"), 2).alias("faturamento_total"),
        round(avg("total_amount"), 2).alias("ticket_medio")
    ) \
    .orderBy(desc("total_pedidos"))

print("💳 Distribuição por Método de Pagamento:")
vendas_pagamento.show()
```

**Resultado esperado:**

```
💳 Distribuição por Método de Pagamento:
+--------------+-------------+-----------------+------------+
|payment_method|total_pedidos|faturamento_total|ticket_medio|
+--------------+-------------+-----------------+------------+
|   credit_card|        40000|       22000000.0|      550.00|
|           pix|        30000|       12000000.0|      400.00|
|        boleto|        20000|        9000000.0|      450.00|
|    debit_card|        10000|        4000000.0|      400.00|
+--------------+-------------+-----------------+------------+
```

**Explicação técnica:**

- O `groupBy` funciona com qualquer coluna categórica (texto com valores repetidos)
- A ordem das colunas no `agg()` define a ordem no resultado
- `.alias("nome")` é essencial para dar nomes legíveis às colunas agregadas — sem ele, o Spark geraria nomes como `sum(total_amount)` que são difíceis de usar em operações subsequentes
- Aqui ordenamos por `total_pedidos` em vez de faturamento — a escolha depende da pergunta de negócio

> **💡 Dica de Ana:** "Interessante! PIX já é o segundo método mais usado, com 30% dos pedidos. E o ticket médio é menor — talvez seja o perfil do comprador. Vou levar isso para a reunião com o time de marketing."

---

## Passo 4: groupBy com Múltiplas Colunas

**Descrição:** Podemos agrupar por mais de uma coluna ao mesmo tempo. Isso cria uma espécie de "tabela cruzada" — por exemplo, faturamento por estado E por status do pedido. É equivalente a um pivot table no Excel ou cross-tab em SQL.

**Código:**

```python
# Faturamento por estado E status do pedido (cross-tab)
estado_status = df_vendas \
    .groupBy("shipping_state", "status") \
    .agg(
        count("order_id").alias("total_pedidos"),
        round(sum("total_amount"), 2).alias("faturamento")
    ) \
    .orderBy("shipping_state", desc("faturamento"))

print("📊 Faturamento por Estado × Status (Top 15):")
estado_status.show(15)
```

**Resultado esperado:**

```
📊 Faturamento por Estado × Status (Top 15):
+--------------+---------+-------------+-----------+
|shipping_state|   status|total_pedidos|faturamento|
+--------------+---------+-------------+-----------+
|            BA|delivered|         4200|  1700000.0|
|            BA|  shipped|         1200|   480000.0|
|            BA|cancelled|          400|   160000.0|
|            BA|  pending|          200|    60000.0|
|            MG|delivered|         8400|  3640000.0|
|            MG|  shipped|         2400|  1040000.0|
|            MG|cancelled|          800|   350000.0|
|            MG|  pending|          400|   170000.0|
|            PR|delivered|         4900|  1960000.0|
|            PR|  shipped|         1400|   560000.0|
|            PR|cancelled|          470|   190000.0|
|            PR|  pending|          230|    90000.0|
|            RJ|delivered|        10500|  5460000.0|
|            RJ|  shipped|         3000|  1560000.0|
|            RJ|cancelled|         1000|   520000.0|
+--------------+---------+-------------+-----------+
only showing top 15 rows
```

**Explicação técnica:**

- `groupBy("col1", "col2")` — cria grupos para cada combinação única de valores nas colunas especificadas
- Se existem 27 estados e 4 status, teremos até 27 × 4 = 108 grupos (menos se alguma combinação não existir)
- A ordenação com múltiplas colunas: `orderBy("shipping_state", desc("faturamento"))` ordena primeiro por estado (A-Z) e depois por faturamento descendente dentro de cada estado
- Equivalente SQL:
  ```sql
  SELECT shipping_state, status,
         COUNT(order_id) AS total_pedidos,
         ROUND(SUM(total_amount), 2) AS faturamento
  FROM vendas
  GROUP BY shipping_state, status
  ORDER BY shipping_state, faturamento DESC
  ```

> **💡 Dica de Carlos:** "Agrupar por múltiplas colunas é extremamente útil para análises de negócio. Mas cuidado: se você agrupar por muitas colunas com alta cardinalidade (ex: city + product_id + date), pode acabar com milhões de grupos — cada um com poucos registros. Isso gera overhead sem trazer insights."

---

## Passo 5: Múltiplas Agregações no agg()

**Descrição:** O `agg()` permite aplicar diversas funções de agregação em uma única chamada. Vamos calcular sum, avg, count, countDistinct, min e max em uma só operação — um painel completo de métricas globais sobre nossas vendas.

**Código:**

```python
# Painel completo de métricas — todas as agregações de uma vez
painel_metricas = df_vendas.agg(
    count("order_id").alias("total_pedidos"),
    countDistinct("customer_id").alias("clientes_unicos"),
    round(sum("total_amount"), 2).alias("faturamento_total"),
    round(avg("total_amount"), 2).alias("ticket_medio"),
    round(min("total_amount"), 2).alias("menor_pedido"),
    round(max("total_amount"), 2).alias("maior_pedido")
)

print("📈 Painel de Métricas Globais:")
painel_metricas.show(truncate=False)
```

**Resultado esperado:**

```
📈 Painel de Métricas Globais:
+-------------+---------------+-----------------+------------+------------+------------+
|total_pedidos|clientes_unicos|faturamento_total|ticket_medio|menor_pedido|maior_pedido|
+-------------+---------------+-----------------+------------+------------+------------+
|100000       |45000          |47000000.00      |470.00      |9.90        |4999.90     |
+-------------+---------------+-----------------+------------+------------+------------+
```

**Código (por estado — visão analítica completa):**

```python
# Mesmas métricas, mas agrupadas por estado
painel_por_estado = df_vendas \
    .groupBy("shipping_state") \
    .agg(
        count("order_id").alias("total_pedidos"),
        countDistinct("customer_id").alias("clientes_unicos"),
        round(sum("total_amount"), 2).alias("faturamento_total"),
        round(avg("total_amount"), 2).alias("ticket_medio"),
        round(min("total_amount"), 2).alias("menor_pedido"),
        round(max("total_amount"), 2).alias("maior_pedido")
    ) \
    .orderBy(desc("faturamento_total"))

print("📈 Painel de Métricas por Estado (Top 5):")
painel_por_estado.show(5, truncate=False)
```

**Resultado esperado:**

```
📈 Painel de Métricas por Estado (Top 5):
+--------------+-------------+---------------+-----------------+------------+------------+------------+
|shipping_state|total_pedidos|clientes_unicos|faturamento_total|ticket_medio|menor_pedido|maior_pedido|
+--------------+-------------+---------------+-----------------+------------+------------+------------+
|SP            |25000        |11250          |12500000.00      |500.00      |9.90        |4999.90     |
|RJ            |15000        |6750           |7800000.00       |520.00      |12.50       |4899.00     |
|MG            |12000        |5400           |5200000.00       |433.33      |15.00       |4750.00     |
|RS            |7500         |3375           |3100000.00       |413.33      |19.90       |4500.00     |
|PR            |7000         |3150           |2800000.00       |400.00      |14.90       |4600.00     |
+--------------+-------------+---------------+-----------------+------------+------------+------------+
only showing top 5 rows
```

**Explicação técnica:**

- `agg()` sem `groupBy` anterior aplica a agregação no DataFrame inteiro (resultado = 1 linha)
- `agg()` após `groupBy` aplica a agregação dentro de cada grupo (resultado = 1 linha por grupo)
- `countDistinct("customer_id")` conta quantos clientes únicos fizeram pedidos — útil para medir alcance vs. recorrência
- Diferença entre `count` e `countDistinct`:
  - `count("col")` — conta linhas onde a coluna não é null (inclui repetições)
  - `countDistinct("col")` — conta apenas valores únicos (ignora repetições)
- Todas as funções são executadas em **uma única passada** pelo dataset — o Spark é inteligente o suficiente para combinar as agregações

> **💡 Dica de Marina:** "Esse painel é o que chamamos de 'executive summary' na DataFlow. Com 6 métricas em uma query, o board tem visão completa de cada mercado. Note como `countDistinct` revela a diferença entre volume de pedidos e base real de clientes."

---

## Passo 6: orderBy / sort — Controle de Ordenação

**Descrição:** A ordenação é essencial para apresentar resultados de forma significativa. Vamos explorar as diferentes formas de ordenar: ascendente, descendente, e com múltiplas colunas de desempate.

### 6.1 — Ordenar por faturamento descendente

**Código:**

```python
# Faturamento por estado — maior para menor
faturamento_desc = df_vendas \
    .groupBy("shipping_state") \
    .agg(round(sum("total_amount"), 2).alias("faturamento")) \
    .orderBy(desc("faturamento"))

print("📉 Estados por faturamento (maior → menor):")
faturamento_desc.show(5)
```

**Resultado esperado:**

```
📉 Estados por faturamento (maior → menor):
+--------------+-----------+
|shipping_state|faturamento|
+--------------+-----------+
|            SP|12500000.00|
|            RJ| 7800000.00|
|            MG| 5200000.00|
|            RS| 3100000.00|
|            PR| 2800000.00|
+--------------+-----------+
only showing top 5 rows
```

### 6.2 — Ordenar por ticket médio ascendente

**Código:**

```python
# Ticket médio por estado — menor para maior (onde estamos com preços baixos?)
ticket_asc = df_vendas \
    .groupBy("shipping_state") \
    .agg(round(avg("total_amount"), 2).alias("ticket_medio")) \
    .orderBy("ticket_medio")  # ascendente é o padrão

print("📈 Estados por ticket médio (menor → maior):")
ticket_asc.show(5)
```

**Resultado esperado:**

```
📈 Estados por ticket médio (menor → maior):
+--------------+------------+
|shipping_state|ticket_medio|
+--------------+------------+
|            AM|      280.50|
|            PA|      295.00|
|            MA|      310.75|
|            PI|      320.00|
|            GO|      342.86|
+--------------+------------+
only showing top 5 rows
```

### 6.3 — Ordenação multi-coluna (desempate)

**Código:**

```python
# Ordenar por status (A-Z), depois por faturamento (maior primeiro) como desempate
multi_sort = df_vendas \
    .groupBy("shipping_state", "status") \
    .agg(
        count("order_id").alias("total_pedidos"),
        round(sum("total_amount"), 2).alias("faturamento")
    ) \
    .orderBy("status", desc("faturamento"))

print("🔀 Ordenação multi-coluna (status ASC, faturamento DESC):")
multi_sort.show(10)
```

**Resultado esperado:**

```
🔀 Ordenação multi-coluna (status ASC, faturamento DESC):
+--------------+---------+-------------+-----------+
|shipping_state|   status|total_pedidos|faturamento|
+--------------+---------+-------------+-----------+
|            SP|cancelled|         2500| 1250000.00|
|            RJ|cancelled|         1500|  780000.00|
|            MG|cancelled|         1200|  520000.00|
|            SP|delivered|        17500| 8750000.00|
|            RJ|delivered|        10500| 5460000.00|
|            MG|delivered|         8400| 3640000.00|
|            SP|  pending|         1250|  625000.00|
|            RJ|  pending|          750|  390000.00|
|            MG|  pending|          600|  260000.00|
|            SP|  shipped|         3750| 1875000.00|
+--------------+---------+-------------+-----------+
only showing top 10 rows
```

**Explicação técnica:**

- `orderBy(col)` — ordena ascendente por padrão (A→Z, 0→9)
- `orderBy(desc(col))` — ordena descendente (Z→A, 9→0)
- `orderBy(col1, desc(col2))` — ordena por `col1` ascendente, e dentro de empates por `col2` descendente
- `sort()` e `orderBy()` são **sinônimos** (assim como `filter` e `where`)
- Alternativa com `col()`: `orderBy(col("faturamento").desc())` — outra sintaxe para ordenação descendente
- A ordenação é uma operação cara em Spark (exige shuffle de dados entre workers). Em datasets muito grandes, considere se realmente precisa ordenar ou se um `limit()` antes resolve

> **💡 Dica de Carlos:** "No Spark, `sort()` e `orderBy()` são a mesma coisa — pode usar qualquer um. Eu prefiro `orderBy` porque fica mais próximo do SQL que a maioria dos analistas já conhece. A regra é: seja consistente no seu projeto."

---

## Passo 7: describe() e summary() — Estatísticas Descritivas Rápidas

**Descrição:** Assim como o `pandas.describe()`, o Spark oferece métodos para obter estatísticas descritivas de forma rápida. São úteis para exploração inicial — entender distribuição, detectar outliers e validar dados.

### 7.1 — describe() — estatísticas básicas

**Código:**

```python
# describe() — count, mean, stddev, min, max para colunas numéricas
print("📊 Estatísticas descritivas (describe):")
df_vendas.select("quantity", "unit_price", "total_amount") \
    .describe() \
    .show()
```

**Resultado esperado:**

```
📊 Estatísticas descritivas (describe):
+-------+------------------+------------------+------------------+
|summary|          quantity|        unit_price|      total_amount|
+-------+------------------+------------------+------------------+
|  count|            100000|            100000|            100000|
|   mean|              2.53|            198.45|             470.0|
| stddev|              1.72|            285.30|             612.5|
|    min|                 1|              9.90|               9.9|
|    max|                10|           4999.90|           4999.90|
+-------+------------------+------------------+------------------+
```

### 7.2 — summary() — estatísticas com percentis

**Código:**

```python
# summary() — inclui percentis (25%, 50%, 75%) além das métricas do describe
print("📊 Estatísticas com percentis (summary):")
df_vendas.select("quantity", "unit_price", "total_amount") \
    .summary("count", "min", "25%", "50%", "75%", "max", "mean") \
    .show()
```

**Resultado esperado:**

```
📊 Estatísticas com percentis (summary):
+-------+--------+----------+------------+
|summary|quantity|unit_price|total_amount|
+-------+--------+----------+------------+
|  count|  100000|    100000|      100000|
|    min|       1|      9.90|        9.90|
|    25%|       1|     49.90|      119.90|
|    50%|       2|    129.90|      299.90|
|    75%|       4|    299.90|      749.90|
|    max|      10|   4999.90|     4999.90|
|   mean|    2.53|    198.45|      470.00|
+-------+--------+----------+------------+
```

**Explicação técnica:**

- `describe()` calcula: count, mean, stddev, min, max — funciona em colunas numéricas e strings
- `summary()` é mais flexível: aceita parâmetros para especificar quais métricas calcular, incluindo **percentis** (25%, 50%, 75%)
- O percentil 50% (P50) é a mediana — se for muito diferente da média, indica distribuição assimétrica (skewed)
- No nosso caso: a mediana do `total_amount` (R$ 299.90) é menor que a média (R$ 470.00) → indica que existem pedidos de alto valor "puxando" a média para cima (distribuição com cauda longa à direita)
- Equivalente no pandas: `df.describe()` e `df.describe(percentiles=[0.25, 0.5, 0.75])`

> **💡 Dica de Carlos:** "O `describe()` é seu melhor amigo na exploração inicial. Em 2 segundos você vê se há dados faltantes (count < total), se há valores impossíveis (min negativo em preço), e se há outliers extremos (max absurdo). Use sempre antes de começar qualquer análise."

---

## Passo 8: Combinação Completa — Pipeline de Relatório

**Descrição:** Agora vamos combinar tudo em um pipeline completo, exatamente como faremos em produção. A Ana precisa de um relatório específico: **faturamento mensal de pedidos entregues em SP e RJ, com ticket médio acima de R$ 100, ordenado por mês e faturamento**. Isso requer: filter → groupBy → agg → filter → orderBy → show.

**Código:**

```python
from pyspark.sql.functions import month, year

# Pipeline completo: o relatório que a Ana precisa para o board
relatorio_ana = df_vendas \
    .filter(col("status") == "delivered") \
    .filter(col("shipping_state").isin("SP", "RJ")) \
    .withColumn("mes", month("order_date")) \
    .withColumn("ano", year("order_date")) \
    .groupBy("ano", "mes", "shipping_state") \
    .agg(
        count("order_id").alias("total_pedidos"),
        countDistinct("customer_id").alias("clientes_unicos"),
        round(sum("total_amount"), 2).alias("faturamento"),
        round(avg("total_amount"), 2).alias("ticket_medio")
    ) \
    .filter(col("ticket_medio") > 100) \
    .orderBy("ano", "mes", desc("faturamento"))

print("📋 Relatório para o Board — Faturamento Mensal SP/RJ (Entregues):")
print("=" * 70)
relatorio_ana.show(12, truncate=False)
```

**Resultado esperado:**

```
📋 Relatório para o Board — Faturamento Mensal SP/RJ (Entregues):
======================================================================
+----+---+--------------+-------------+---------------+-----------+------------+
|ano |mes|shipping_state|total_pedidos|clientes_unicos|faturamento|ticket_medio|
+----+---+--------------+-------------+---------------+-----------+------------+
|2023|1  |SP            |1458         |656            |729000.00  |500.00      |
|2023|1  |RJ            |875          |394            |455000.00  |520.00      |
|2023|2  |SP            |1458         |656            |729000.00  |500.00      |
|2023|2  |RJ            |875          |394            |455000.00  |520.00      |
|2023|3  |SP            |1458         |656            |729000.00  |500.00      |
|2023|3  |RJ            |875          |394            |455000.00  |520.00      |
|2023|4  |SP            |1458         |656            |729000.00  |500.00      |
|2023|4  |RJ            |875          |394            |455000.00  |520.00      |
|2023|5  |SP            |1458         |656            |729000.00  |500.00      |
|2023|5  |RJ            |875          |394            |455000.00  |520.00      |
|2023|6  |SP            |1458         |656            |729000.00  |500.00      |
|2023|6  |RJ            |875          |394            |455000.00  |520.00      |
+----+---+--------------+-------------+---------------+-----------+------------+
only showing top 12 rows
```

**Código (contagem total do resultado):**

```python
# Quantas linhas no relatório final?
print(f"\n📊 Total de linhas no relatório: {relatorio_ana.count()}")
```

**Resultado esperado:**

```
📊 Total de linhas no relatório: 24
```

**Explicação técnica:**

Este pipeline demonstra a composição de operações em Spark:

1. **`filter(status == "delivered")`** — mantém apenas pedidos entregues (remove cancelled, pending, shipped)
2. **`filter(state.isin("SP", "RJ"))`** — restringe aos dois maiores mercados
3. **`withColumn("mes", month(...))`** — extrai o mês da data (nova coluna derivada)
4. **`groupBy("ano", "mes", "shipping_state")`** — agrupa por período e estado
5. **`agg(...)`** — calcula todas as métricas de interesse
6. **`filter(ticket_medio > 100)`** — filtra após a agregação (HAVING no SQL)
7. **`orderBy("ano", "mes", desc("faturamento"))`** — ordena cronologicamente, maior faturamento primeiro

Pontos importantes:
- O `filter` após o `agg` equivale ao `HAVING` do SQL (filtra grupos, não linhas)
- `isin("SP", "RJ")` — forma limpa de filtrar por múltiplos valores (equivale a `IN ('SP', 'RJ')` no SQL)
- `withColumn` adiciona colunas derivadas ao DataFrame antes do agrupamento
- O Spark otimiza a ordem de execução internamente (Catalyst Optimizer), mas a ordem no código deve ser legível

Equivalente SQL completo:
```sql
SELECT YEAR(order_date) AS ano,
       MONTH(order_date) AS mes,
       shipping_state,
       COUNT(order_id) AS total_pedidos,
       COUNT(DISTINCT customer_id) AS clientes_unicos,
       ROUND(SUM(total_amount), 2) AS faturamento,
       ROUND(AVG(total_amount), 2) AS ticket_medio
FROM vendas
WHERE status = 'delivered'
  AND shipping_state IN ('SP', 'RJ')
GROUP BY YEAR(order_date), MONTH(order_date), shipping_state
HAVING AVG(total_amount) > 100
ORDER BY ano, mes, faturamento DESC
```

> **💡 Dica de Ana:** "Perfeito! Esse é exatamente o formato que o board precisa. Cada linha mostra um mês/estado, com todas as métricas relevantes. Agora é só exportar para o slide de apresentação. Carlos, na próxima aula quero cruzar isso com dados de clientes — consegue?"
>
> **Carlos:** "Com joins e window functions? Sem problema. Isso é assunto da Aula 2!"

---

## Resumo do Exercício

Neste exercício você aprendeu a agregar e ordenar dados com PySpark:

| Operação | Método | Tipo | Equivalente SQL |
|----------|--------|------|-----------------|
| Agrupar | `df.groupBy("col")` | Transformação | `GROUP BY col` |
| Somar | `sum("col")` | Agregação | `SUM(col)` |
| Média | `avg("col")` | Agregação | `AVG(col)` |
| Contar | `count("col")` | Agregação | `COUNT(col)` |
| Contar únicos | `countDistinct("col")` | Agregação | `COUNT(DISTINCT col)` |
| Mínimo | `min("col")` | Agregação | `MIN(col)` |
| Máximo | `max("col")` | Agregação | `MAX(col)` |
| Arredondar | `round(col, n)` | Transformação | `ROUND(col, n)` |
| Múltiplas agregações | `.agg(f1, f2, f3)` | Agregação | `SELECT f1, f2, f3` |
| Ordenar ASC | `df.orderBy("col")` | Transformação | `ORDER BY col ASC` |
| Ordenar DESC | `df.orderBy(desc("col"))` | Transformação | `ORDER BY col DESC` |
| Filtrar grupo | `.filter()` após `.agg()` | Transformação | `HAVING` |
| Estatísticas | `df.describe()` / `df.summary()` | Ação | — |

### Conceitos-chave

1. **groupBy + agg** é a combinação fundamental para relatórios analíticos no Spark
2. **Múltiplas agregações** em um único `agg()` são calculadas em uma passada (eficiente)
3. **orderBy** e **sort** são sinônimos — use o que preferir consistentemente
4. **filter após agg** equivale ao `HAVING` do SQL (filtra grupos, não linhas individuais)
5. **countDistinct** revela a cardinalidade real (clientes únicos ≠ total de pedidos)
6. **describe/summary** são equivalentes ao pandas e ideais para exploração rápida
7. **Pipelines compostos** (filter → groupBy → agg → orderBy) refletem a lógica SQL de forma programática

> **Carlos:** "Agora você domina os fundamentos de agregação e ordenação. Com `select`, `filter`, `groupBy`, `agg` e `orderBy` você consegue responder 80% das perguntas de negócio que a Ana traz. No próximo exercício intermediário, vamos aplicar isso para gerar uma análise exploratória completa — faturamento por região, sazonalidade e segmentação de clientes."

---

## Próximo Exercício

➡️ **Exercício 3 — Análise Exploratória de Faturamento** (`03_analise_exploratoria.md`): exercício intermediário com análise por estado/cidade, sazonalidade mensal e segmentação (Lab Parte 2)
