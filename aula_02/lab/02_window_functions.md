# Exercício 2 — Window Functions: Ranking e Tendências

## Contexto

> **Ana Rodrigues (Product Owner):** "Carlos, a Black Friday está chegando e o marketing quer enviar promoções personalizadas. Precisamos dos top 100 clientes por faturamento em cada estado — os melhores clientes de SP recebem uma oferta, os de RJ outra, e assim por diante. Além disso, o time de retenção precisa identificar clientes com risco de churn: aqueles que estão comprando cada vez menos. Consegue montar essas análises?"

> **Carlos Mendes:** "Perfeito, Ana. Para o ranking por estado vamos usar **Window Functions** com `dense_rank` — é como criar um 'mini-ranking' dentro de cada partição de dados. Para a análise de tendência, usamos `lag` e `lead` que acessam a compra anterior e posterior de cada cliente. Assim conseguimos calcular a variação percentual entre compras consecutivas e identificar quem está em declínio."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender o conceito de Window Functions e WindowSpec
- Criar janelas particionadas por coluna (partitionBy) com ordenação
- Aplicar funções de ranking: `row_number`, `rank`, `dense_rank`
- Usar funções de deslocamento temporal: `lag` e `lead`
- Calcular variações percentuais entre registros consecutivos
- Implementar running totals (soma acumulada) com janelas
- Identificar padrões de negócio (churn risk) com análise de tendência

## Pré-requisitos

- Exercício 1 concluído (o `df_completo` deve estar cacheado na sessão)
- Jupyter Notebook acessível em http://localhost:8888
- SparkSession ativa com os dados de vendas + clientes + categorias

## Duração Estimada

⏱️ ~30 minutos

---

## Passo 1: Importar Window e Funções Necessárias

**Descrição:** Window Functions operam sobre um "quadro" (janela) de linhas relacionadas à linha atual — sem colapsar o resultado como `groupBy` faz. Para usá-las, precisamos importar a classe `Window` (que define a janela) e as funções específicas (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `sum`). Vamos também verificar que o `df_completo` do exercício anterior está disponível.

**Código:**

```python
from pyspark.sql import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank,
    lag, lead,
    sum as spark_sum,
    col, desc, datediff, round as spark_round,
    count, avg
)

# Verificar que df_completo está cacheado do exercício anterior
print(f"✅ df_completo disponível: {df_completo.is_cached}")
print(f"   Registros: {df_completo.count():,}")
print()

# Visualizar colunas disponíveis para nosso trabalho
print("📋 Colunas disponíveis:")
for i, c in enumerate(df_completo.columns, 1):
    print(f"   {i:2d}. {c}")
```

**Resultado esperado:**

```
✅ df_completo disponível: True
   Registros: 1,000,000

📋 Colunas disponíveis:
    1. customer_id
    2. category_id
    3. order_id
    4. product_id
    5. quantity
    6. unit_price
    7. total_amount
    8. order_date
    9. payment_method
   10. shipping_city
   11. shipping_state
   12. status
   13. partner_source
   14. customer_name
   15. customer_email
   16. segment
   17. state
   18. registration_date
   19. category_name
```

**Explicação técnica:**

- **Window Functions vs groupBy:** o `groupBy` colapsa N linhas em 1 resultado por grupo. Window Functions calculam um valor para **cada linha**, usando informações das linhas "vizinhas" na janela
- **Classe `Window`:** define o "quadro" — quais linhas pertencem à mesma janela (`partitionBy`) e em que ordem são processadas (`orderBy`)
- **`row_number`, `rank`, `dense_rank`** — funções de ranking (atribuem posição)
- **`lag`, `lead`** — funções de deslocamento (acessam linhas anteriores/posteriores)
- **`spark_sum` sobre janela** — soma acumulada (running total)
- Importamos `sum as spark_sum` para evitar conflito com a built-in `sum` do Python

> **💡 Dica de Carlos:** "Window Functions são o recurso mais poderoso e subutilizado do Spark SQL. Elas resolvem problemas que, sem elas, exigiriam self-joins caros ou múltiplos groupBys. Quando você dominar janelas, vai resolver 80% dos problemas analíticos de forma elegante."

---

## Passo 2: Criar Window de Ranking — Particionada por Estado

**Descrição:** Para criar o ranking de clientes por estado, precisamos primeiro agregar o faturamento total de cada cliente e depois aplicar o ranking dentro de cada estado. A janela é definida com `partitionBy("shipping_state")` — isso cria "mini-rankings" independentes para SP, RJ, MG, etc. A ordenação é decrescente por faturamento (quem gastou mais fica em primeiro).

**Código:**

```python
# Primeiro: agregar faturamento total por cliente e estado
df_faturamento_cliente = df_completo \
    .groupBy("customer_id", "customer_name", "shipping_state", "segment") \
    .agg(
        spark_sum("total_amount").alias("faturamento_total"),
        count("order_id").alias("total_pedidos"),
        avg("total_amount").alias("ticket_medio")
    )

print(f"📊 Clientes únicos com faturamento calculado: {df_faturamento_cliente.count():,}")
print()

# Definir a janela de ranking: particionar por estado, ordenar por faturamento desc
window_ranking = Window \
    .partitionBy("shipping_state") \
    .orderBy(desc("faturamento_total"))

print("✅ WindowSpec de ranking criada:")
print("   Partição:  shipping_state (cada estado tem ranking independente)")
print("   Ordenação: faturamento_total DESC (maior faturamento = rank 1)")
print()

# Visualizar dados antes do ranking (amostra de SP)
print("📋 Amostra de clientes de SP (antes do ranking):")
df_faturamento_cliente \
    .filter(col("shipping_state") == "SP") \
    .orderBy(desc("faturamento_total")) \
    .show(5)
```

**Resultado esperado:**

```
📊 Clientes únicos com faturamento calculado: ~50,000

✅ WindowSpec de ranking criada:
   Partição:  shipping_state (cada estado tem ranking independente)
   Ordenação: faturamento_total DESC (maior faturamento = rank 1)

📋 Amostra de clientes de SP (antes do ranking):
+-----------+------------------+--------------+-------+-----------------+-------------+------------+
|customer_id|     customer_name|shipping_state|segment|faturamento_total|total_pedidos|ticket_medio|
+-----------+------------------+--------------+-------+-----------------+-------------+------------+
|  CUST_00123|Ana Maria Santos  |            SP|   Ouro|         12450.00|           25|      498.00|
|  CUST_00456|Pedro Costa       |            SP|   Ouro|         11230.50|           22|      510.48|
|  CUST_00789|Juliana Lima      |            SP|  Prata|         10890.75|           20|      544.54|
|...                                                                                               |
+-----------+------------------+--------------+-------+-----------------+-------------+------------+
```

**Explicação técnica:**

- **`Window.partitionBy("shipping_state")`** — divide os dados em partições lógicas. Cada estado é processado independentemente, como se fossem tabelas separadas
- **`.orderBy(desc("faturamento_total"))`** — dentro de cada partição, ordena do maior para o menor faturamento
- **Nota:** definir a `WindowSpec` não executa nada — é uma especificação (como um blueprint). A execução acontece quando aplicamos uma função sobre ela
- **Agregação prévia:** antes de rankear, precisamos calcular o faturamento total de cada cliente. Isso é necessário porque cada cliente pode ter múltiplas vendas
- **Escolha do `partitionBy`:** se não usarmos `partitionBy`, o ranking seria global (todos os estados juntos). Com partição, temos N rankings independentes (um por estado)

> **💡 Dica de Marina (CTO):** "A escolha da janela reflete a pergunta de negócio. 'Top clientes por estado' = partição por estado. 'Top clientes por categoria' = partição por categoria. A WindowSpec é a tradução direta do requisito de negócio para código."

---

## Passo 3: Aplicar dense_rank — Ranking de Clientes por Estado

**Descrição:** Agora aplicamos `dense_rank()` sobre a janela definida. A diferença entre `row_number`, `rank` e `dense_rank` é sutil mas importante: `dense_rank` não pula posições em caso de empate. Se dois clientes têm o mesmo faturamento, ambos recebem rank 2, e o próximo recebe rank 3 (não 4). Isso é ideal para "top N" porque garante que tenhamos exatamente N posições preenchidas.

**Código:**

```python
# Aplicar as 3 funções de ranking para comparação
df_ranking = df_faturamento_cliente \
    .withColumn("row_num", row_number().over(window_ranking)) \
    .withColumn("rank", rank().over(window_ranking)) \
    .withColumn("dense_rank", dense_rank().over(window_ranking))

# Demonstrar a diferença entre as 3 funções
# (usar um estado com possíveis empates)
print("📊 Comparação: row_number vs rank vs dense_rank (SP, top 15):")
print("   (Observe o comportamento quando há faturamentos iguais)")
print()

df_ranking \
    .filter(col("shipping_state") == "SP") \
    .select(
        "customer_id", "customer_name", "faturamento_total",
        "row_num", "rank", "dense_rank"
    ) \
    .orderBy("row_num") \
    .show(15)
```

**Resultado esperado:**

```
📊 Comparação: row_number vs rank vs dense_rank (SP, top 15):
   (Observe o comportamento quando há faturamentos iguais)

+-----------+------------------+-----------------+-------+----+----------+
|customer_id|     customer_name|faturamento_total|row_num|rank|dense_rank|
+-----------+------------------+-----------------+-------+----+----------+
|  CUST_00123|Ana Maria Santos  |         12450.00|      1|   1|         1|
|  CUST_00456|Pedro Costa       |         11230.50|      2|   2|         2|
|  CUST_00789|Juliana Lima      |         10890.75|      3|   3|         3|
|  CUST_01234|Ricardo Souza     |         10890.75|      4|   3|         3|
|  CUST_01567|Fernanda Dias     |          9870.00|      5|   5|         4|
|  CUST_01890|Lucas Oliveira    |          9540.25|      6|   6|         5|
|...                                                                      |
+-----------+------------------+-----------------+-------+----+----------+
```

**Explicação técnica:**

- **`row_number()`** — sempre sequencial (1, 2, 3, 4, 5...), mesmo com empates. A ordem de empate é arbitrária
- **`rank()`** — empates recebem mesmo rank, mas pula posições. Ex: (1, 2, 3, 3, **5**, 6...) — pulou o 4
- **`dense_rank()`** — empates recebem mesmo rank, sem pular. Ex: (1, 2, 3, 3, **4**, 5...) — não pula
- **Qual usar?**
  - `row_number`: quando precisa de numeração única (paginação, deduplicação)
  - `rank`: rankings esportivos (se 2 empatam em 3°, não há 4° lugar)
  - `dense_rank`: "top N por grupo" — garante que rank N sempre existe
- **Performance:** as 3 funções têm custo similar. A diferença é apenas na lógica de numeração
- Note que `row_num` 4 e 5 correspondem a `rank` 3 e 5, mas `dense_rank` 3 e 4

> **💡 Dica de Carlos:** "Para o pedido da Ana (top 100 por estado para promoções), `dense_rank` é a escolha certa. Se 3 clientes empatam no rank 100, queremos incluir todos eles — afinal, não faz sentido excluir alguém com o mesmo faturamento só por critério arbitrário de desempate."

---

## Passo 4: Filtrar Top 10 por Estado — Melhores Clientes para Black Friday

**Descrição:** Agora filtramos apenas os clientes com `dense_rank <= 10` — os 10 melhores de cada estado. Esse resultado é exatamente o que o marketing precisa para a campanha segmentada de Black Friday. Vamos também ver a distribuição entre estados e o valor total que esses top clientes representam.

**Código:**

```python
# Filtrar top 10 por estado usando dense_rank
df_top10_estado = df_ranking \
    .filter(col("dense_rank") <= 10) \
    .select(
        "shipping_state", "dense_rank", "customer_id",
        "customer_name", "segment", "faturamento_total",
        "total_pedidos", "ticket_medio"
    ) \
    .orderBy("shipping_state", "dense_rank")

# Contagem total
count_top10 = df_top10_estado.count()
print(f"📊 Total de clientes top 10 (todos os estados): {count_top10}")
print()

# Quantos estados distintos temos?
estados_distintos = df_top10_estado.select("shipping_state").distinct().count()
print(f"📊 Estados com ranking: {estados_distintos}")
print()

# Mostrar top 10 de São Paulo
print("🏆 TOP 10 Clientes — São Paulo (SP):")
df_top10_estado \
    .filter(col("shipping_state") == "SP") \
    .show(10, truncate=20)

# Mostrar top 5 do Rio de Janeiro para comparação
print("🏆 TOP 5 Clientes — Rio de Janeiro (RJ):")
df_top10_estado \
    .filter(col("shipping_state") == "RJ") \
    .show(5, truncate=20)
```

**Resultado esperado:**

```
📊 Total de clientes top 10 (todos os estados): ~270
   (27 estados × 10 posições, pode ser mais por empates)

📊 Estados com ranking: 27

🏆 TOP 10 Clientes — São Paulo (SP):
+--------------+----------+-----------+--------------------+-------+-----------------+-------------+------------+
|shipping_state|dense_rank|customer_id|       customer_name|segment|faturamento_total|total_pedidos|ticket_medio|
+--------------+----------+-----------+--------------------+-------+-----------------+-------------+------------+
|            SP|         1|  CUST_00123|   Ana Maria Santos |   Ouro|         12450.00|           25|      498.00|
|            SP|         2|  CUST_00456|        Pedro Costa |   Ouro|         11230.50|           22|      510.48|
|            SP|         3|  CUST_00789|      Juliana Lima  |  Prata|         10890.75|           20|      544.54|
|...                                                                                                            |
+--------------+----------+-----------+--------------------+-------+-----------------+-------------+------------+

🏆 TOP 5 Clientes — Rio de Janeiro (RJ):
+--------------+----------+-----------+--------------------+-------+-----------------+-------------+------------+
|shipping_state|dense_rank|customer_id|       customer_name|segment|faturamento_total|total_pedidos|ticket_medio|
+--------------+----------+-----------+--------------------+-------+-----------------+-------------+------------+
|            RJ|         1|  CUST_04567|  Marcos Pereira    |   Ouro|         11890.00|           23|      517.00|
|            RJ|         2|  CUST_04890|  Camila Ferreira   |   Ouro|         10560.25|           19|      555.80|
|...                                                                                                            |
+--------------+----------+-----------+--------------------+-------+-----------------+-------------+------------+
```

**Código (valor total dos top clientes):**

```python
# Quanto os top 10 de cada estado representam do total?
valor_top10 = df_top10_estado.agg(
    spark_sum("faturamento_total").alias("valor_tops")
).collect()[0]["valor_tops"]

valor_total = df_faturamento_cliente.agg(
    spark_sum("faturamento_total").alias("valor_total")
).collect()[0]["valor_total"]

print(f"💰 Faturamento dos top 10 por estado: R$ {valor_top10:,.2f}")
print(f"💰 Faturamento total:                  R$ {valor_total:,.2f}")
print(f"📊 Top 10 representam:                 {(valor_top10/valor_total)*100:.1f}% do total")
```

**Resultado esperado:**

```
💰 Faturamento dos top 10 por estado: R$ ~X,XXX,XXX.XX
💰 Faturamento total:                  R$ ~XX,XXX,XXX.XX
📊 Top 10 representam:                 ~XX.X% do total
```

**Explicação técnica:**

- **Filtro pós-ranking:** `.filter(col("dense_rank") <= 10)` — elegante e eficiente. O Spark aplica o filtro após calcular o ranking na janela
- **Empates no limiar:** com `dense_rank`, se 3 clientes empatam no rank 10, todos são incluídos (resultado pode ter mais de 10 por estado)
- **Alternativa com `row_number`:** se quiséssemos exatamente 10 por estado (sem empates), usaríamos `row_number() <= 10`
- **Pareto (80/20):** é comum que os top 10-20% dos clientes representem 50-80% do faturamento. Essa análise confirma (ou refuta) essa hipótese para os dados da DataFlow
- **Performance:** o filtro reduz drasticamente o DataFrame (de ~50K para ~270 linhas), tornando operações subsequentes instantâneas

> **💡 Dica de Ana (PO):** "Esse tipo de análise é ouro puro para o marketing. Com a lista dos top clientes por estado, podemos criar campanhas hiper-segmentadas: 'Olá Ana Maria, como uma das nossas melhores clientes em SP, preparamos uma oferta exclusiva de Black Friday para você.' Taxa de conversão vai ao teto!"

---

## Passo 5: Janela Temporal — Partição por Cliente, Ordem por Data

**Descrição:** Agora mudamos de perspectiva: em vez de comparar clientes entre si (ranking), vamos analisar o **histórico de cada cliente ao longo do tempo**. A nova janela é particionada por `customer_id` e ordenada por `order_date`. Isso nos permite "olhar para trás" (compra anterior) e "para frente" (próxima compra) de cada transação, mantendo cada cliente isolado em sua própria janela.

**Código:**

```python
# Definir janela temporal: cada cliente como partição, ordenado por data
window_temporal = Window \
    .partitionBy("customer_id") \
    .orderBy("order_date")

print("✅ WindowSpec temporal criada:")
print("   Partição:  customer_id (cada cliente tem histórico independente)")
print("   Ordenação: order_date ASC (mais antiga → mais recente)")
print()

# Preparar dataset para análise temporal
# Selecionar colunas relevantes e filtrar pedidos entregues
df_historico = df_completo \
    .filter(col("status") == "delivered") \
    .select(
        "customer_id", "customer_name", "order_id",
        "order_date", "total_amount", "shipping_state"
    ) \
    .orderBy("customer_id", "order_date")

# Verificar quantos clientes têm múltiplas compras (necessário para lag/lead)
from pyspark.sql.functions import countDistinct

stats_compras = df_historico \
    .groupBy("customer_id") \
    .agg(count("order_id").alias("num_compras")) \
    .agg(
        avg("num_compras").alias("media_compras_por_cliente"),
        count("*").alias("total_clientes"),
        spark_sum(
            (col("num_compras") >= 3).cast("int")
        ).alias("clientes_3plus_compras")
    )

stats_compras.show(truncate=False)
```

**Resultado esperado:**

```
✅ WindowSpec temporal criada:
   Partição:  customer_id (cada cliente tem histórico independente)
   Ordenação: order_date ASC (mais antiga → mais recente)

+-------------------------+--------------+----------------------+
|media_compras_por_cliente|total_clientes|clientes_3plus_compras|
+-------------------------+--------------+----------------------+
|                     ~15 |       ~50000 |                ~45000|
+-------------------------+--------------+----------------------+
```

**Explicação técnica:**

- **Janela temporal vs ranking:**
  - Ranking: `partitionBy("shipping_state").orderBy(desc("faturamento"))` — compara clientes entre si
  - Temporal: `partitionBy("customer_id").orderBy("order_date")` — compara compras de um mesmo cliente ao longo do tempo
- **Por que filtrar `status == "delivered"`?** — Pedidos cancelados ou pendentes distorceriam a análise de tendência. Só queremos transações efetivamente concluídas
- **Múltiplas compras:** `lag` e `lead` precisam de pelo menos 2 registros por partição para produzir valores não-null. Clientes com apenas 1 compra terão `null` no lag/lead
- **Ordenação ASC:** mais antiga primeiro → `lag(1)` = compra imediatamente anterior, `lead(1)` = próxima compra
- A contagem de clientes com 3+ compras indica quantos terão dados suficientes para análise de tendência robusta

> **💡 Dica de Carlos:** "A beleza das Window Functions é que a mesma estrutura (`Window.partitionBy().orderBy()`) serve para problemas completamente diferentes. Mudou a partição e a ordenação? Mudou a pergunta de negócio. A sintaxe é idêntica."

---

## Passo 6: Aplicar lag/lead — Compra Anterior e Próxima Compra

**Descrição:** `lag(col, n)` acessa o valor de uma coluna **n linhas antes** da linha atual (dentro da janela). `lead(col, n)` acessa **n linhas depois**. Com isso, para cada compra de um cliente, conseguimos ver quanto ele gastou na compra anterior e na próxima — sem self-join! Também calculamos o intervalo em dias entre compras consecutivas.

**Código:**

```python
# Aplicar lag e lead sobre a janela temporal
df_tendencia = df_historico \
    .withColumn(
        "valor_compra_anterior",
        lag("total_amount", 1).over(window_temporal)
    ) \
    .withColumn(
        "valor_proxima_compra",
        lead("total_amount", 1).over(window_temporal)
    ) \
    .withColumn(
        "data_compra_anterior",
        lag("order_date", 1).over(window_temporal)
    ) \
    .withColumn(
        "data_proxima_compra",
        lead("order_date", 1).over(window_temporal)
    ) \
    .withColumn(
        "dias_desde_ultima_compra",
        datediff(col("order_date"), col("data_compra_anterior"))
    )

# Visualizar resultado para um cliente específico
print("📋 Histórico de compras de um cliente (com lag/lead):")
df_tendencia \
    .filter(col("customer_id") == "CUST_00001") \
    .select(
        "order_date", "total_amount",
        "valor_compra_anterior", "valor_proxima_compra",
        "dias_desde_ultima_compra"
    ) \
    .orderBy("order_date") \
    .show(10, truncate=False)
```

**Resultado esperado:**

```
📋 Histórico de compras de um cliente (com lag/lead):
+-------------------+------------+---------------------+--------------------+------------------------+
|order_date         |total_amount|valor_compra_anterior |valor_proxima_compra|dias_desde_ultima_compra|
+-------------------+------------+---------------------+--------------------+------------------------+
|2023-01-15 10:30:00|      250.00|                 null|              380.50|                    null|
|2023-02-28 14:15:00|      380.50|               250.00|              190.75|                      44|
|2023-04-10 09:45:00|      190.75|               380.50|              420.00|                      41|
|2023-05-22 16:20:00|      420.00|               190.75|              310.25|                      42|
|2023-07-03 11:00:00|      310.25|               420.00|              150.00|                      42|
|2023-08-15 08:30:00|      150.00|               310.25|              275.50|                      43|
|2023-09-20 13:45:00|      275.50|               150.00|              445.00|                      36|
|2023-10-30 17:10:00|      445.00|               275.50|              180.00|                      40|
|2023-11-25 10:00:00|      180.00|               445.00|              520.75|                      26|
|2023-12-20 15:30:00|      520.75|               180.00|                null|                      25|
+-------------------+------------+---------------------+--------------------+------------------------+
```

**Explicação técnica:**

- **`lag("total_amount", 1)`** — valor da compra 1 posição antes na janela. A primeira compra de cada cliente retorna `null` (não há "anterior")
- **`lead("total_amount", 1)`** — valor da compra 1 posição depois. A última compra retorna `null` (não há "próxima")
- **`datediff(col_a, col_b)`** — diferença em dias entre duas datas. Útil para medir frequência de compra
- **Sem self-join:** em SQL tradicional, para acessar a "linha anterior" precisaríamos de um self-join com lógica complexa de ordenação. Window Functions fazem isso nativamente e com performance muito melhor
- **`lag(col, 2)`** — acessaria 2 posições antes (a compra antes da anterior). Útil para médias móveis
- **Null na primeira/última linha:** é o comportamento esperado. Podemos tratar com `coalesce()` ou filtrar posteriormente
- **Dias entre compras:** padrão de ~40 dias indica frequência mensal. Variações grandes podem indicar mudança de comportamento

> **💡 Dica de Carlos:** "O `lag` e `lead` são como 'olhar pelo retrovisor' e 'olhar pelo para-brisa' em cada linha. Sem window functions, você precisaria de um self-join do tipo `WHERE t1.row = t2.row - 1` — que é lento, complexo e propenso a erros. Aqui, é uma linha de código."

---

## Passo 7: Calcular Variação Percentual entre Compras Consecutivas

**Descrição:** Com o valor anterior disponível via `lag`, podemos calcular a variação percentual entre compras consecutivas: `(atual - anterior) / anterior × 100`. Variação positiva = cliente gastando mais. Variação negativa = cliente gastando menos. Esse é o indicador-chave para detectar tendências de crescimento ou declínio no comportamento de compra.

**Código:**

```python
# Calcular variação percentual entre compras consecutivas
df_variacao = df_tendencia \
    .withColumn(
        "variacao_percentual",
        spark_round(
            ((col("total_amount") - col("valor_compra_anterior")) /
             col("valor_compra_anterior")) * 100,
            2
        )
    )

# Visualizar variação para um cliente
print("📋 Variação percentual entre compras (CUST_00001):")
df_variacao \
    .filter(col("customer_id") == "CUST_00001") \
    .select(
        "order_date", "total_amount",
        "valor_compra_anterior", "variacao_percentual"
    ) \
    .orderBy("order_date") \
    .show(10, truncate=False)

print()

# Estatísticas gerais da variação
print("📊 Estatísticas da variação percentual (todos os clientes):")
df_variacao \
    .filter(col("variacao_percentual").isNotNull()) \
    .select("variacao_percentual") \
    .summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max") \
    .show(truncate=False)
```

**Resultado esperado:**

```
📋 Variação percentual entre compras (CUST_00001):
+-------------------+------------+---------------------+-------------------+
|order_date         |total_amount|valor_compra_anterior |variacao_percentual|
+-------------------+------------+---------------------+-------------------+
|2023-01-15 10:30:00|      250.00|                 null|               null|
|2023-02-28 14:15:00|      380.50|               250.00|              52.20|
|2023-04-10 09:45:00|      190.75|               380.50|             -49.87|
|2023-05-22 16:20:00|      420.00|               190.75|             120.21|
|2023-07-03 11:00:00|      310.25|               420.00|             -26.13|
|2023-08-15 08:30:00|      150.00|               310.25|             -51.65|
|2023-09-20 13:45:00|      275.50|               150.00|              83.67|
|2023-10-30 17:10:00|      445.00|               275.50|              61.52|
|2023-11-25 10:00:00|      180.00|               445.00|             -59.55|
|2023-12-20 15:30:00|      520.75|               180.00|             189.31|
+-------------------+------------+---------------------+-------------------+

📊 Estatísticas da variação percentual (todos os clientes):
+-------+-------------------+
|summary|variacao_percentual|
+-------+-------------------+
|  count|           ~900,000|
|   mean|              ~5.00|
| stddev|            ~120.00|
|    min|            ~-95.00|
|    25%|            ~-40.00|
|    50%|              ~0.00|
|    75%|             ~50.00|
|    max|           ~1000.00|
+-------+-------------------+
```

**Explicação técnica:**

- **Fórmula:** `(atual - anterior) / anterior × 100` — variação percentual clássica
- **Null na primeira compra:** não há "anterior" para calcular variação, então resultado é `null`
- **`spark_round(..., 2)`** — arredonda para 2 casas decimais (legibilidade)
- **Interpretação:**
  - `+52.20%` → cliente gastou 52% a mais que na compra anterior (bom sinal!)
  - `-49.87%` → cliente gastou ~50% a menos (sinal de alerta)
  - Variações extremas (>200% ou <-80%) podem indicar compras atípicas (presentes, promoções)
- **Mediana ~0%:** indica que, no geral, os clientes mantêm gasto estável entre compras
- **Desvio padrão alto (~120%):** muita variabilidade — normal em e-commerce (compras esporádicas de valores diferentes)
- **Cuidado com divisão por zero:** se `valor_compra_anterior = 0`, teríamos divisão por zero. No nosso dataset `total_amount > 0` sempre, mas em produção adicione tratamento com `when(col != 0, ...)`

> **💡 Dica de Marina:** "A variação percentual isolada pode ser enganosa. Um cliente que gastou R$ 10 e depois R$ 20 tem +100% de variação, mas não é tão relevante quanto um que foi de R$ 5.000 para R$ 10.000. No Passo 8, vamos combinar variação com valor absoluto para criar o indicador de churn real."

---

## Passo 8: Identificar Risco de Churn — Clientes em Declínio

**Descrição:** O time de retenção definiu que um cliente está em risco de churn quando apresenta **variação negativa superior a 30%** nas últimas compras. Vamos identificar esses clientes analisando suas últimas 3 compras: se a média de variação recente for < -30%, o cliente está em declínio preocupante. Combinamos variação percentual com número da compra (via `row_number` reverso) para pegar apenas compras recentes.

**Código:**

```python
# Janela reversa: mais recentes primeiro (para pegar últimas compras)
window_recente = Window \
    .partitionBy("customer_id") \
    .orderBy(desc("order_date"))

# Adicionar numeração reversa (1 = compra mais recente)
df_churn_analysis = df_variacao \
    .withColumn("compra_recente_num", row_number().over(window_recente))

# Filtrar apenas últimas 3 compras de cada cliente (que tenham variação calculada)
df_ultimas_compras = df_churn_analysis \
    .filter(
        (col("compra_recente_num") <= 3) &
        (col("variacao_percentual").isNotNull())
    )

# Calcular média de variação das últimas compras por cliente
df_risco_churn = df_ultimas_compras \
    .groupBy("customer_id", "customer_name", "shipping_state") \
    .agg(
        avg("variacao_percentual").alias("variacao_media_recente"),
        count("*").alias("compras_analisadas"),
        spark_sum("total_amount").alias("valor_recente_total")
    ) \
    .withColumn(
        "risco_churn",
        (col("variacao_media_recente") < -30).cast("string")
    ) \
    .withColumn(
        "risco_churn",
        col("risco_churn").isin("true")
    )

# Separar clientes em risco
df_em_risco = df_risco_churn.filter(col("risco_churn") == True)
df_saudaveis = df_risco_churn.filter(col("risco_churn") == False)

count_total = df_risco_churn.count()
count_risco = df_em_risco.count()

print(f"📊 Análise de Churn — Resultados:")
print(f"   Total de clientes analisados: {count_total:,}")
print(f"   Clientes em RISCO (variação < -30%): {count_risco:,}")
print(f"   Clientes saudáveis: {count_total - count_risco:,}")
print(f"   Taxa de risco: {(count_risco/count_total)*100:.1f}%")
print()

# Top 10 clientes com MAIOR risco (variação mais negativa)
print("🚨 TOP 10 Clientes com Maior Risco de Churn:")
df_em_risco \
    .orderBy("variacao_media_recente") \
    .select(
        "customer_id", "customer_name", "shipping_state",
        "variacao_media_recente", "compras_analisadas", "valor_recente_total"
    ) \
    .show(10, truncate=20)
```

**Resultado esperado:**

```
📊 Análise de Churn — Resultados:
   Total de clientes analisados: ~48,000
   Clientes em RISCO (variação < -30%): ~12,000
   Clientes saudáveis: ~36,000
   Taxa de risco: ~25.0%

🚨 TOP 10 Clientes com Maior Risco de Churn:
+-----------+--------------------+--------------+----------------------+------------------+-------------------+
|customer_id|       customer_name|shipping_state|variacao_media_recente|compras_analisadas|valor_recente_total|
+-----------+--------------------+--------------+----------------------+------------------+-------------------+
|  CUST_23456|Roberto Almeida    |            SP|                -85.30|                 3|             120.50|
|  CUST_34567|Carla Nascimento   |            RJ|                -78.15|                 3|             230.00|
|  CUST_45678|André Batista      |            MG|                -72.40|                 3|             180.75|
|...                                                                                                         |
+-----------+--------------------+--------------+----------------------+------------------+-------------------+
```

**Código (distribuição por estado):**

```python
# Distribuição de risco por estado
print("📊 Risco de churn por estado:")
df_em_risco \
    .groupBy("shipping_state") \
    .agg(
        count("*").alias("clientes_em_risco"),
        spark_round(avg("variacao_media_recente"), 2).alias("variacao_media")
    ) \
    .orderBy(desc("clientes_em_risco")) \
    .show(10)
```

**Resultado esperado:**

```
📊 Risco de churn por estado:
+--------------+-----------------+--------------+
|shipping_state|clientes_em_risco|variacao_media|
+--------------+-----------------+--------------+
|            SP|            ~3000|        -48.50|
|            RJ|            ~1800|        -45.20|
|            MG|            ~1200|        -44.80|
|            BA|             ~800|        -46.10|
|            RS|             ~750|        -43.90|
|...                                            |
+--------------+-----------------+--------------+
```

**Explicação técnica:**

- **Janela reversa:** `orderBy(desc("order_date"))` + `row_number()` = numeração onde 1 = mais recente. Filtramos `<= 3` para pegar apenas as 3 últimas compras
- **Critério de churn:** média de variação das últimas 3 compras < -30%. Esse threshold é configurável pelo negócio
- **Por que últimas 3?** — Uma única compra baixa pode ser pontual (promoção, presente barato). Três compras em declínio seguidas indicam tendência real
- **Falsos positivos:** clientes com apenas 1-2 compras analisáveis podem ser menos confiáveis. Em produção, exigiríamos mínimo de 3 compras para considerar a análise válida
- **Ação de negócio:** esses clientes em risco recebem:
  - Cupom de desconto personalizado
  - Email de reativação
  - Ligação do time de customer success (para os de maior valor)
- **Combinação ranking + churn:** os clientes que são simultaneamente "top por estado" E "em risco de churn" são prioridade máxima de retenção

> **💡 Dica de Ana:** "Essa lista é ouro para o time de retenção! Um cliente que está no top 10 do estado mas com variação negativa de -50% precisa de atenção imediata. Perder um top cliente custa 10x mais que adquirir um novo. Vou pedir pro time de CRM ligar para os top 5 de cada estado que estão em risco."

---

## Passo 9: Running Totals — Soma Acumulada por Cliente

**Descrição:** A última Window Function que vamos explorar é a **soma acumulada** (running total). Para cada compra de um cliente, calculamos o faturamento acumulado até aquele momento. Isso é útil para identificar quando um cliente atingiu determinados marcos de fidelidade (ex: R$ 5.000 acumulados = upgrade para segmento Ouro). A janela usa `rowsBetween` para definir o "frame" da soma: do início da partição até a linha atual.

**Código:**

```python
# Definir janela com frame para soma acumulada
# unboundedPreceding = desde o início da partição
# currentRow = até a linha atual (inclusiva)
window_acumulada = Window \
    .partitionBy("customer_id") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calcular running totals
df_acumulado = df_historico \
    .withColumn(
        "faturamento_acumulado",
        spark_sum("total_amount").over(window_acumulada)
    ) \
    .withColumn(
        "pedidos_acumulados",
        count("order_id").over(window_acumulada)
    ) \
    .withColumn(
        "ticket_medio_acumulado",
        spark_round(
            spark_sum("total_amount").over(window_acumulada) /
            count("order_id").over(window_acumulada),
            2
        )
    )

# Visualizar evolução de um cliente
print("📋 Evolução acumulada de um cliente (CUST_00001):")
df_acumulado \
    .filter(col("customer_id") == "CUST_00001") \
    .select(
        "order_date", "total_amount",
        "faturamento_acumulado", "pedidos_acumulados",
        "ticket_medio_acumulado"
    ) \
    .orderBy("order_date") \
    .show(10, truncate=False)
```

**Resultado esperado:**

```
📋 Evolução acumulada de um cliente (CUST_00001):
+-------------------+------------+---------------------+------------------+----------------------+
|order_date         |total_amount|faturamento_acumulado |pedidos_acumulados|ticket_medio_acumulado|
+-------------------+------------+---------------------+------------------+----------------------+
|2023-01-15 10:30:00|      250.00|               250.00|                 1|                250.00|
|2023-02-28 14:15:00|      380.50|               630.50|                 2|                315.25|
|2023-04-10 09:45:00|      190.75|               821.25|                 3|                273.75|
|2023-05-22 16:20:00|      420.00|              1241.25|                 4|                310.31|
|2023-07-03 11:00:00|      310.25|              1551.50|                 5|                310.30|
|2023-08-15 08:30:00|      150.00|              1701.50|                 6|                283.58|
|2023-09-20 13:45:00|      275.50|              1977.00|                 7|                282.43|
|2023-10-30 17:10:00|      445.00|              2422.00|                 8|                302.75|
|2023-11-25 10:00:00|      180.00|              2602.00|                 9|                289.11|
|2023-12-20 15:30:00|      520.75|              3122.75|                10|                312.28|
+-------------------+------------+---------------------+------------------+----------------------+
```

**Código (identificar clientes que atingiram marcos de fidelidade):**

```python
# Identificar quando cada cliente atingiu R$ 5.000 acumulados
df_marco_5k = df_acumulado \
    .filter(col("faturamento_acumulado") >= 5000) \
    .withColumn(
        "primeiro_acima_5k",
        row_number().over(
            Window.partitionBy("customer_id").orderBy("order_date")
        )
    ) \
    .filter(col("primeiro_acima_5k") == 1) \
    .select(
        "customer_id", "customer_name", "order_date",
        "faturamento_acumulado", "pedidos_acumulados"
    )

count_atingiram = df_marco_5k.count()
print(f"\n🏆 Clientes que atingiram R$ 5.000 acumulados: {count_atingiram:,}")
print()
print("📋 Primeiros 10 clientes a atingir o marco:")
df_marco_5k \
    .orderBy("order_date") \
    .show(10, truncate=20)

# Distribuição: quantas compras para atingir R$ 5.000?
print("📊 Quantas compras até atingir R$ 5.000:")
df_marco_5k \
    .select("pedidos_acumulados") \
    .summary("count", "mean", "min", "25%", "50%", "75%", "max") \
    .show(truncate=False)
```

**Resultado esperado:**

```
🏆 Clientes que atingiram R$ 5.000 acumulados: ~15,000

📋 Primeiros 10 clientes a atingir o marco:
+-----------+--------------------+-------------------+---------------------+------------------+
|customer_id|       customer_name|         order_date|faturamento_acumulado|pedidos_acumulados|
+-----------+--------------------+-------------------+---------------------+------------------+
|  CUST_00042|Maria Gonçalves    |2023-03-15 08:20:00|              5120.00|                 8|
|  CUST_00108|Paulo Ribeiro      |2023-03-18 14:30:00|              5240.50|                 9|
|...                                                                                         |
+-----------+--------------------+-------------------+---------------------+------------------+

📊 Quantas compras até atingir R$ 5.000:
+-------+------------------+
|summary|pedidos_acumulados|
+-------+------------------+
|  count|           ~15,000|
|   mean|              ~12 |
|    min|                 3 |
|    25%|                 9 |
|    50%|               12 |
|    75%|               15 |
|    max|               25 |
+-------+------------------+
```

**Explicação técnica:**

- **`rowsBetween(Window.unboundedPreceding, Window.currentRow)`** — define o frame da janela:
  - `unboundedPreceding` = desde a primeira linha da partição
  - `currentRow` = até a linha atual (inclusiva)
  - Resultado: soma de todas as linhas **até** a atual (soma acumulada crescente)
- **Alternativas de frame:**
  - `rowsBetween(-3, 0)` = últimas 3 linhas + atual (média móvel de 4 períodos)
  - `rowsBetween(0, Window.unboundedFollowing)` = da atual até o fim (soma reversa)
  - `rangeBetween(-7, 0)` = intervalo de valores (ex: últimos 7 dias)
- **`rows` vs `range`:**
  - `rows` = conta linhas físicas (posição)
  - `range` = usa valores da coluna de ordenação (útil para janelas baseadas em tempo)
- **Running total é monotonicamente crescente** (se todos os valores são positivos) — ideal para marcos de fidelidade
- **Performance:** soma acumulada sobre janela é eficiente — o Spark mantém estado parcial e incrementa a cada linha

> **💡 Dica de Carlos:** "Running totals são fundamentais para programas de fidelidade. Em vez de recalcular do zero toda vez ('SELECT SUM(...) WHERE date <= current_date'), a window function mantém o acumulado incrementalmente. É a diferença entre O(N²) e O(N). Em 1 milhão de transações, isso importa muito."

---

## Resumo do Exercício

Neste exercício você dominou **Window Functions** — uma das ferramentas mais poderosas do Spark SQL para análises avançadas sem colapsar dados:

| Função | Tipo | Uso | Exemplo |
|--------|------|-----|---------|
| `row_number()` | Ranking | Numeração única sequencial | Deduplicação, paginação |
| `rank()` | Ranking | Com gaps em empates | Rankings esportivos |
| `dense_rank()` | Ranking | Sem gaps em empates | Top N por grupo |
| `lag(col, n)` | Deslocamento | Valor N linhas antes | Compra anterior |
| `lead(col, n)` | Deslocamento | Valor N linhas depois | Próxima compra |
| `sum().over(window)` | Agregação | Soma acumulada | Running total, marcos |

### Conceitos-chave

1. **WindowSpec** define a janela: `partitionBy` (agrupa) + `orderBy` (ordena dentro do grupo)
2. **Ranking functions** criam posições dentro de cada partição — ideais para "top N por grupo"
3. **`dense_rank` é preferível** para top-N de negócio (não pula posições em empates)
4. **`lag`/`lead`** acessam linhas vizinhas sem self-join — elegante e performático
5. **Variação percentual** com `lag` detecta tendências de crescimento ou declínio
6. **Churn risk** = variação negativa consistente nas últimas compras (threshold configurável)
7. **Running totals** com `rowsBetween` calculam acumulados incrementais (marcos de fidelidade)
8. **Frame specification** (`rowsBetween`, `rangeBetween`) controla quais linhas entram na "janela"

### Tabela de Referência — Window Frames

| Frame | Significado | Uso |
|-------|-------------|-----|
| `rowsBetween(unboundedPreceding, currentRow)` | Início → atual | Soma acumulada |
| `rowsBetween(-N, 0)` | N linhas antes → atual | Média móvel |
| `rowsBetween(0, unboundedFollowing)` | Atual → fim | Soma reversa |
| `rowsBetween(unboundedPreceding, unboundedFollowing)` | Toda a partição | Total da partição |
| `rangeBetween(-7, 0)` | Valores no range [-7, 0] | Janela temporal |

### Aplicações de Negócio Construídas

| Análise | Window | Função | Valor |
|---------|--------|--------|-------|
| Top clientes por estado | `partitionBy(state)` | `dense_rank` | Promoções segmentadas |
| Tendência de compra | `partitionBy(customer)` | `lag` | Detecção de declínio |
| Risco de churn | `partitionBy(customer)` | `lag` + média | Retenção proativa |
| Marcos de fidelidade | `partitionBy(customer)` | `sum` acumulada | Programa de loyalty |

> **Carlos:** "Excelente trabalho! Você agora tem um arsenal analítico completo: joins para cruzar fontes e window functions para análises sofisticadas dentro de cada grupo. No próximo exercício, vamos criar **UDFs** (User Defined Functions) para encapsular lógica de negócio customizada que o Spark não oferece nativamente — como classificação de ticket e segmentação de clientes."

---

## Próximo Exercício

➡️ **Exercício 3 — UDFs e Plano de Execução** (`03_udfs_explain.md`): User Defined Functions para lógica customizada + análise do plano de execução com explain()
