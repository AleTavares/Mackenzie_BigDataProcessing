# Exercício 3 — Análise Exploratória de Faturamento (Intermediário)

## Contexto

> **Marina Silva (CTO):** "Ana, preciso que você prepare um relatório completo de análise regional para eu apresentar ao Roberto na reunião de board de sexta-feira. Ele quer entender quais cidades e estados são nossos mercados-chave, como o faturamento se comporta ao longo dos meses e onde estão nossas oportunidades de crescimento."

> **Ana Rodrigues (Product Owner):** "Entendido, Marina! Já temos os dados carregados no Spark. Vou montar as análises usando os conceitos de `groupBy`, `agg` e `orderBy` que aprendemos. Mas desta vez sem roteiro pronto — vou precisar pensar na melhor abordagem para cada pergunta de negócio."

## Objetivos

Ao final deste exercício, você será capaz de:

- Construir análises exploratórias completas de forma autônoma (sem código pronto)
- Combinar múltiplas operações Spark para responder perguntas de negócio
- Extrair e agrupar dados por dimensões temporais (mês, trimestre)
- Calcular métricas derivadas (percentuais, rankings, correlações)
- Produzir DataFrames consolidados prontos para apresentação executiva

## Pré-requisitos

- Exercícios 1 e 2 concluídos (SparkSession criada, `df_vendas` carregado, funções de agregação importadas)
- Ambiente Docker rodando (ver `00_setup.md`)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `vendas_2023.csv` disponível na pasta `data/`
- Imports necessários já realizados:
  ```python
  from pyspark.sql.functions import (
      sum, avg, count, countDistinct, min, max,
      round, desc, col, month, year, when, lit
  )
  ```

## Duração Estimada

⏱️ ~25 minutos (primeira parte do Lab Parte 2 — 50 min total)

## Nível de Dificuldade

🟡 **Intermediário** — As instruções descrevem O QUE fazer e fornecem dicas, mas NÃO incluem a solução completa. Você deve construir o código usando os conceitos dos Exercícios 1 e 2.

---

## Exercício 3.1: Top 10 Cidades por Faturamento

### Pergunta de Negócio

> **Marina:** "Quero saber quais são as 10 cidades que mais faturam para a DataFlow. Não basta olhar só por estado — preciso da granularidade por cidade para decidir onde abrir centros de distribuição."

### O que fazer

Calcule o faturamento total (`total_amount`) agrupado por cidade (`shipping_city`). Ordene do maior para o menor e mostre apenas as 10 primeiras.

### Dicas

1. Use `groupBy("shipping_city")` seguido de `.agg(sum(...).alias(...))` para calcular o total por cidade
2. Aplique `.orderBy(desc("..."))` para ordenar do maior para o menor
3. Use `.show(10)` ou `.limit(10).show()` para exibir apenas o top 10

### Formato Esperado do Resultado

```
+-------------+-----------------+-------------+------------+
|shipping_city|faturamento_total|total_pedidos|ticket_medio|
+-------------+-----------------+-------------+------------+
|   São Paulo |     XXXXXXXX.XX |       XXXXX |     XXX.XX |
|Rio de Jan...|     XXXXXXXX.XX |       XXXXX |     XXX.XX |
| ...         |             ... |         ... |        ... |
+-------------+-----------------+-------------+------------+
only showing top 10 rows
```

Colunas obrigatórias:
- `shipping_city` — nome da cidade
- `faturamento_total` — soma de `total_amount` (arredondado 2 casas)
- `total_pedidos` — contagem de `order_id`
- `ticket_medio` — média de `total_amount` (arredondado 2 casas)

### Critérios de Validação

- [ ] O DataFrame resultante contém exatamente 4 colunas
- [ ] Está ordenado por `faturamento_total` descendente
- [ ] As 10 cidades com maior faturamento estão presentes
- [ ] Valores monetários estão arredondados para 2 casas decimais

---

## Exercício 3.2: Análise de Sazonalidade Mensal

### Pergunta de Negócio

> **Marina:** "Roberto quer ver a tendência de faturamento mês a mês ao longo de 2023. Houve algum mês com queda brusca? Algum pico sazonal? Esse padrão vai guiar nosso planejamento de capacidade para 2024."

### O que fazer

Calcule o faturamento total e a quantidade de pedidos para cada mês de 2023. Extraia o mês da coluna `order_date` e agrupe por ele. Ordene cronologicamente (mês 1 ao 12).

### Dicas

1. Use a função `month("order_date")` para extrair o número do mês — combine com `.withColumn("mes", month("order_date"))` antes do `groupBy`
2. Agrupe por `"mes"` e calcule `sum("total_amount")` e `count("order_id")`
3. Ordene por `"mes"` ascendente para visualizar a tendência cronológica (janeiro → dezembro)

### Formato Esperado do Resultado

```
+---+-----------------+-------------+------------+
|mes|faturamento_total|total_pedidos|ticket_medio|
+---+-----------------+-------------+------------+
|  1|      XXXXXXX.XX |       XXXXX |     XXX.XX |
|  2|      XXXXXXX.XX |       XXXXX |     XXX.XX |
| ..|             ... |         ... |        ... |
| 12|      XXXXXXX.XX |       XXXXX |     XXX.XX |
+---+-----------------+-------------+------------+
```

Colunas obrigatórias:
- `mes` — número do mês (1 a 12)
- `faturamento_total` — soma de `total_amount`
- `total_pedidos` — contagem de pedidos
- `ticket_medio` — média de `total_amount`

### Critérios de Validação

- [ ] O DataFrame contém exatamente 12 linhas (uma por mês)
- [ ] Está ordenado cronologicamente (mês 1 ao 12)
- [ ] A soma de `total_pedidos` de todos os meses é igual ao `df_vendas.count()` total
- [ ] É possível identificar visualmente meses de pico e queda

---

## Exercício 3.3: Distribuição de Status por Estado

### Pergunta de Negócio

> **Ana:** "Marina, o Roberto pediu para entender a taxa de cancelamento por estado. Será que alguns estados têm problemas logísticos que causam mais cancelamentos? Preciso ver o percentual de cada status (delivered, shipped, pending, cancelled) para cada estado."

### O que fazer

Para cada estado (`shipping_state`), calcule a contagem de pedidos em cada status. Em seguida, calcule o percentual que cada status representa dentro daquele estado. Por exemplo: se SP tem 25.000 pedidos e 2.500 são cancelled, a taxa de cancelamento de SP é 10%.

### Dicas

1. Primeiro, agrupe por `shipping_state` e `status` para obter a contagem: `groupBy("shipping_state", "status").agg(count("order_id").alias("qtd_pedidos"))`
2. Para calcular o percentual, você precisa do total de pedidos por estado. Uma abordagem: use uma Window Function (`Window.partitionBy("shipping_state")`) com `sum("qtd_pedidos")` para calcular o total do estado, e depois divida `qtd_pedidos / total_estado * 100`. Alternativa: faça dois `groupBy` separados e depois um `join` por estado
3. Para usar Window: importe `from pyspark.sql import Window` e use `.withColumn("total_estado", sum("qtd_pedidos").over(Window.partitionBy("shipping_state")))`

### Formato Esperado do Resultado

```
+--------------+---------+-----------+-----------+
|shipping_state|   status|qtd_pedidos|percentual |
+--------------+---------+-----------+-----------+
|            SP|delivered|      17500|      70.00|
|            SP|  shipped|       3750|      15.00|
|            SP|cancelled|       2500|      10.00|
|            SP|  pending|       1250|       5.00|
|            RJ|delivered|      10500|      70.00|
|            RJ|  shipped|       3000|      20.00|
| ...          |     ... |       ... |       ... |
+--------------+---------+-----------+-----------+
```

Colunas obrigatórias:
- `shipping_state` — sigla do estado
- `status` — status do pedido
- `qtd_pedidos` — contagem absoluta
- `percentual` — percentual dentro do estado (arredondado 2 casas)

### Critérios de Validação

- [ ] Cada estado tem 4 linhas (uma por status: delivered, shipped, pending, cancelled)
- [ ] Os percentuais de cada estado somam ~100% (tolerância de arredondamento)
- [ ] O resultado está ordenado por estado e depois por percentual descendente
- [ ] É possível identificar estados com taxas de cancelamento acima da média

---

## Exercício 3.4: Top 5 Estados por Ticket Médio

### Pergunta de Negócio

> **Marina:** "Interessante... quais estados têm o maior ticket médio? Não estou falando de faturamento total — quero saber onde cada pedido individual vale mais. Às vezes estados menores surpreendem com tickets altos. Isso pode indicar oportunidade de crescimento."

### O que fazer

Calcule o ticket médio (média de `total_amount`) por estado. Ordene do maior ticket para o menor e mostre os top 5. Inclua também o total de pedidos para contextualizar — um ticket médio alto com poucos pedidos tem significado diferente de um ticket alto com muito volume.

### Dicas

1. Use `groupBy("shipping_state")` com `.agg(round(avg("total_amount"), 2).alias("ticket_medio"), count("order_id").alias("total_pedidos"))`
2. Ordene por `ticket_medio` descendente com `.orderBy(desc("ticket_medio"))`
3. Mostre os 5 primeiros com `.show(5)` — observe se os estados do topo são mercados grandes (SP, RJ) ou mercados menores com pedidos de alto valor

### Formato Esperado do Resultado

```
+--------------+------------+-------------+-----------------+
|shipping_state|ticket_medio|total_pedidos|faturamento_total|
+--------------+------------+-------------+-----------------+
|            XX|      XXX.XX|         XXXX|      XXXXXXXX.XX|
|            XX|      XXX.XX|         XXXX|      XXXXXXXX.XX|
|            XX|      XXX.XX|         XXXX|      XXXXXXXX.XX|
|            XX|      XXX.XX|         XXXX|      XXXXXXXX.XX|
|            XX|      XXX.XX|         XXXX|      XXXXXXXX.XX|
+--------------+------------+-------------+-----------------+
```

Colunas obrigatórias:
- `shipping_state` — sigla do estado
- `ticket_medio` — média de `total_amount` (arredondado 2 casas)
- `total_pedidos` — volume de pedidos (para contextualizar)
- `faturamento_total` — soma total (para comparar com ranking por faturamento)

### Critérios de Validação

- [ ] Exatamente 5 estados no resultado
- [ ] Ordenado por `ticket_medio` descendente
- [ ] Inclui contexto de volume (total_pedidos) para interpretação
- [ ] Consegue responder: os estados com maior ticket são grandes ou pequenos em volume?

---

## Exercício 3.5: Correlação Quantidade vs Preço Unitário

### Pergunta de Negócio

> **Ana:** "Tenho uma hipótese: será que pedidos com mais unidades tendem a ter preço unitário menor? Tipo desconto por quantidade? Ou será que clientes que compram em grande quantidade são justamente os que compram produtos mais caros? Preciso de dados para validar."

### O que fazer

Agrupe os pedidos por faixas de quantidade (`quantity`) e calcule o preço unitário médio (`unit_price`) para cada faixa. Isso revelará se há correlação entre volume do pedido e preço unitário.

### Dicas

1. Crie faixas de quantidade usando `when`: por exemplo, 1 unidade, 2-3 unidades, 4-5 unidades, 6+ unidades. Use `.withColumn("faixa_qtd", when(col("quantity") == 1, "1 unidade").when(col("quantity") <= 3, "2-3 unidades").when(col("quantity") <= 5, "4-5 unidades").otherwise("6+ unidades"))`
2. Agrupe por `"faixa_qtd"` e calcule `avg("unit_price")` e `count("order_id")`
3. Ordene as faixas de forma lógica (menor para maior) — pode ser necessário usar `.orderBy("faixa_qtd")` ou criar uma coluna auxiliar para ordenação correta

### Formato Esperado do Resultado

```
+--------------+-------------------+-------------+------------------+
|     faixa_qtd|preco_unitario_medio|total_pedidos|total_amount_medio|
+--------------+-------------------+-------------+------------------+
|    1 unidade |             XXX.XX|        XXXXX|            XXX.XX|
| 2-3 unidades |             XXX.XX|        XXXXX|            XXX.XX|
| 4-5 unidades |             XXX.XX|        XXXXX|            XXX.XX|
|  6+ unidades |             XXX.XX|        XXXXX|            XXX.XX|
+--------------+-------------------+-------------+------------------+
```

Colunas obrigatórias:
- `faixa_qtd` — faixa de quantidade
- `preco_unitario_medio` — média de `unit_price` (arredondado 2 casas)
- `total_pedidos` — contagem para representatividade estatística
- `total_amount_medio` — média de `total_amount` (para comparação)

### Critérios de Validação

- [ ] Pelo menos 3 faixas de quantidade no resultado
- [ ] Cada faixa tem representatividade estatística (>100 pedidos)
- [ ] É possível identificar tendência: preço sobe, desce ou fica estável com aumento de quantidade?
- [ ] A conclusão da análise responde à pergunta de negócio da Ana

---

## Exercício 3.6: Relatório Consolidado Final

### Pergunta de Negócio

> **Marina:** "Ana, preciso de um DataFrame único e consolidado que eu possa mandar direto para o Roberto. Uma tabela com todas as métricas-chave por estado: faturamento total, quantidade de pedidos, clientes únicos, ticket médio e o método de pagamento dominante. Se possível, ordene pelos maiores mercados. Esse é o relatório executivo da DataFlow."

### O que fazer

Crie um único DataFrame que consolide as principais métricas por estado. Este é o "relatório executivo" — uma visão completa de cada mercado em uma única tabela. O desafio extra aqui é incluir o método de pagamento mais frequente (moda) por estado.

### Dicas

1. Comece com `groupBy("shipping_state")` e calcule: `sum("total_amount")`, `count("order_id")`, `countDistinct("customer_id")`, `avg("total_amount")`
2. Para o método de pagamento dominante: calcule separadamente a contagem de cada `payment_method` por estado (groupBy estado + payment_method + count), depois use uma Window Function com `row_number()` particionada por estado e ordenada por contagem descendente para pegar o top 1. Importe: `from pyspark.sql.functions import row_number` e `from pyspark.sql import Window`
3. Faça um `join` do DataFrame de métricas com o DataFrame de pagamento dominante (filtrado para rank == 1) pela coluna `shipping_state`

### Formato Esperado do Resultado

```
+--------------+-----------------+-------------+----------------+------------+--------------------+
|shipping_state|faturamento_total|total_pedidos|clientes_unicos |ticket_medio|pagamento_dominante |
+--------------+-----------------+-------------+----------------+------------+--------------------+
|            SP|     12500000.00 |       25000 |          11250 |     500.00 |        credit_card |
|            RJ|      7800000.00 |       15000 |           6750 |     520.00 |        credit_card |
|            MG|      5200000.00 |       12000 |           5400 |     433.33 |               pix  |
| ...          |             ... |         ... |            ... |        ... |                ... |
+--------------+-----------------+-------------+----------------+------------+--------------------+
```

Colunas obrigatórias:
- `shipping_state` — sigla do estado
- `faturamento_total` — soma de `total_amount` (arredondado 2 casas)
- `total_pedidos` — contagem de pedidos
- `clientes_unicos` — contagem distinta de `customer_id`
- `ticket_medio` — média de `total_amount` (arredondado 2 casas)
- `pagamento_dominante` — método de pagamento mais frequente no estado

### Critérios de Validação

- [ ] Todos os estados presentes no dataset aparecem no relatório (uma linha por estado)
- [ ] Contém todas as 6 colunas obrigatórias
- [ ] Ordenado por `faturamento_total` descendente
- [ ] O `pagamento_dominante` é de fato o mais frequente (não o de maior ticket)
- [ ] A soma de `total_pedidos` de todos os estados é igual ao total geral do dataset
- [ ] Marina poderia enviar este DataFrame diretamente ao Roberto como relatório executivo

---

## Resumo e Próximos Passos

Neste exercício intermediário você praticou:

| Habilidade | Exercício | Complexidade |
|------------|-----------|--------------|
| Agrupamento + Top N | 3.1 Top 10 cidades | 🟢 Básico+ |
| Extração temporal + tendência | 3.2 Sazonalidade mensal | 🟡 Intermediário |
| Percentual + Window ou Join | 3.3 Distribuição de status | 🟡 Intermediário |
| Ranking por métrica derivada | 3.4 Ticket médio por estado | 🟢 Básico+ |
| Faixas + correlação | 3.5 Quantidade vs preço | 🟡 Intermediário |
| Consolidação multi-métrica | 3.6 Relatório executivo | 🔴 Avançado |

### Conceitos-chave praticados

1. **Análise sem roteiro** — pensar na estratégia antes de escrever código
2. **Combinação de operações** — filter, withColumn, groupBy, agg, orderBy em sequência
3. **Métricas derivadas** — percentuais, faixas, rankings
4. **Window Functions introdutórias** — particionamento para cálculos dentro de grupos
5. **DataFrames consolidados** — joins para combinar análises separadas

> **Marina:** "Excelente trabalho! Com esse relatório, Roberto terá uma visão completa dos nossos mercados. Na Aula 2, vamos aprender a cruzar esses dados com informações de clientes e categorias usando joins e window functions mais avançadas. Prepare-se!"

---

## Próximo Exercício

➡️ **Exercício 4 — Desafio: Pandas vs Spark** (`04_desafio_pandas_vs_spark.md`): exercício avançado comparando performance entre pandas e PySpark no mesmo dataset
