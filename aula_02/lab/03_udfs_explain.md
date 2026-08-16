# Exercício 3 — UDFs e Análise de Plano de Execução (Intermediário)

## Contexto

> **Ana Rodrigues (Product Owner):** "Carlos, o time comercial precisa categorizar nossos pedidos por faixa de ticket — baixo, médio, alto e premium — e também segmentar os clientes em perfis como VIP, Gold, Silver, Bronze e 'Em Risco'. As regras de negócio são específicas e não existem como funções prontas no Spark. Além disso, o time de infraestrutura reclamou que algumas queries estão demorando demais. Preciso dessas classificações rodando rápido em produção."

> **Carlos Mendes:** "Entendido, Ana. Para regras de negócio customizadas temos duas opções: **UDFs** (User Defined Functions) que permitem Python puro, ou combinações de **when/otherwise** que são nativas do Spark. A escolha impacta diretamente a performance — UDFs serializam dados entre JVM e Python, enquanto funções nativas rodam direto no Catalyst Optimizer. Vamos implementar ambas e usar o **explain()** para entender o que acontece por baixo dos panos."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar e aplicar UDFs Python para regras de negócio customizadas
- Implementar lógica equivalente com when/otherwise (abordagem nativa)
- Comparar performance entre UDF Python, Pandas UDF e funções nativas
- Criar UDFs que recebem múltiplas colunas como entrada
- Interpretar planos de execução com `explain()`
- Identificar gargalos de performance relacionados a UDFs no plano de execução

## Pré-requisitos

- Exercícios 1 e 2 concluídos (`df_completo` cacheado na sessão)
- Jupyter Notebook acessível em http://localhost:8888
- SparkSession ativa com dados de vendas + clientes + categorias

## Duração Estimada

⏱️ ~20 minutos

## Nível

🟡 **Intermediário** — Este exercício fornece contexto, regras de negócio e dicas, mas NÃO a solução completa. Você deve implementar as soluções com base nos conceitos apresentados nos exercícios guiados e nas dicas fornecidas.

---

## Exercício 3.1: Criar UDF Python para Classificação de Ticket

### Pergunta de negócio

O time comercial precisa categorizar cada pedido pela faixa de valor do ticket para direcionar campanhas de marketing segmentadas. As regras são:

| Faixa de Valor | Classificação |
|---------------|---------------|
| < R$ 50 | `"baixo"` |
| R$ 50 a R$ 200 | `"médio"` |
| R$ 200 a R$ 500 | `"alto"` |
| > R$ 500 | `"premium"` |

### O que implementar

1. Crie uma função Python que receba um valor numérico e retorne a classificação como string
2. Registre-a como UDF do Spark com retorno `StringType()`
3. Aplique a UDF ao `df_completo` criando a coluna `faixa_ticket`
4. Mostre a distribuição (contagem e percentual) de pedidos por faixa

### Dicas

- Use o decorator `@udf(returnType=StringType())` para registrar a UDF
- Import necessário: `from pyspark.sql.functions import udf` e `from pyspark.sql.types import StringType`
- Lembre-se de tratar o caso `None` — valores nulos podem chegar na UDF
- Para a distribuição, use `groupBy("faixa_ticket").count()` seguido de cálculo percentual

### Formato esperado da saída

```
📊 Distribuição de pedidos por faixa de ticket:
+------------+-------+-----------+
|faixa_ticket|  count|percentual |
+------------+-------+-----------+
|       baixo|  XXXXX|      XX.X%|
|       médio|  XXXXX|      XX.X%|
|        alto|  XXXXX|      XX.X%|
|     premium|  XXXXX|      XX.X%|
+------------+-------+-----------+
```

### Critério de validação

- A UDF deve classificar corretamente os valores nos limites (R$ 50 exato → "médio", R$ 200 exato → "alto", R$ 500 exato → "alto")
- A soma de todos os percentuais deve ser 100%
- Nenhum pedido deve ter classificação `null` (exceto se `total_amount` for null)

---

## Exercício 3.2: Implementar Classificação com when/otherwise (Sem UDF)

### Pergunta de negócio

Mesma classificação de ticket do exercício 3.1, porém sem usar UDF — apenas funções nativas do Spark.

### O que implementar

1. Recrie a mesma lógica de classificação usando `when(...).when(...).otherwise(...)`
2. Aplique ao `df_completo` criando a coluna `faixa_ticket_nativo`
3. Verifique que os resultados são idênticos ao da UDF (compare contagens por faixa)
4. Observe as diferenças de legibilidade entre as duas abordagens

### Dicas

- Import necessário: `from pyspark.sql.functions import when, lit`
- Encadeie `.when(condition, value)` para cada faixa
- Use `.otherwise("categoria_default")` para o último caso
- Para comparar resultados: faça `groupBy` de ambas as colunas e verifique se os counts batem

### Formato esperado da saída

```
📊 Comparação UDF vs when/otherwise:
+------------+-------------------+-----------+
|faixa_ticket|faixa_ticket_nativo|    match  |
+------------+-------------------+-----------+
|       baixo|              baixo|      True |
|       médio|              médio|      True |
|        alto|               alto|      True |
|     premium|            premium|      True |
+------------+-------------------+-----------+
✅ Resultados idênticos entre UDF e when/otherwise
```

### Critério de validação

- Ambas as colunas devem produzir exatamente a mesma distribuição
- O código com `when/otherwise` deve ter as mesmas regras de limites do exercício 3.1

---

## Exercício 3.3: Comparação de Performance — UDF vs when/otherwise

### Pergunta de negócio

O time de infraestrutura precisa saber qual abordagem usar em produção. Quando temos 1 milhão de registros sendo classificados diariamente, a escolha entre UDF e funções nativas faz diferença?

### O que implementar

1. Meça o tempo de execução da abordagem UDF sobre o `df_completo` (1M registros)
2. Meça o tempo de execução da abordagem `when/otherwise` sobre os mesmos dados
3. Compare os tempos e calcule o speedup (quantas vezes mais rápido)
4. Explique por que há diferença de performance

### Dicas

- Use `import time` e calcule `time.time()` antes e depois
- **Importante:** Spark é lazy! Apenas definir a coluna não executa nada. Force a execução com uma **action** como `.count()` ou `.collect()`
- Para medição justa, execute cada abordagem 3 vezes e use a média
- Lembre-se de que a UDF precisa serializar dados Python ↔ JVM a cada registro

### Formato esperado da saída

```
⏱️ Performance sobre 1M registros:
   UDF Python:      X.XX segundos (média de 3 execuções)
   when/otherwise:  X.XX segundos (média de 3 execuções)
   
📊 Speedup: when/otherwise é ~Xx mais rápido que UDF Python

💡 Por quê? [sua explicação aqui]
```

### Critério de validação

- Os tempos devem ser medidos com `.count()` forçando a materialização
- A abordagem `when/otherwise` deve ser consistentemente mais rápida
- A explicação deve mencionar a serialização Python ↔ JVM como causa principal

---

## Exercício 3.4: Criar Pandas UDF (Vetorizada)

### Pergunta de negócio

Sabendo que UDFs Python são lentas por causa da serialização registro-a-registro, existe um meio-termo? **Pandas UDFs** processam dados em lotes (batches) usando Apache Arrow, mantendo a flexibilidade do Python com performance muito melhor.

### O que implementar

1. Crie uma Pandas UDF que implemente a mesma classificação de ticket
2. A função deve receber uma `pd.Series` e retornar uma `pd.Series`
3. Aplique ao `df_completo` e meça o tempo de execução
4. Compare a performance: UDF Python vs Pandas UDF vs when/otherwise

### Dicas

- Import: `from pyspark.sql.functions import pandas_udf` e `from pyspark.sql.types import StringType`
- Decorator: `@pandas_udf(StringType())`
- Dentro da função, use `pd.cut()` ou operações vetorizadas do pandas/numpy para classificar
- A entrada é uma `pd.Series` de valores numéricos, a saída é uma `pd.Series` de strings
- Pandas UDFs usam Apache Arrow para transferir dados em formato colunar (muito mais eficiente)

### Formato esperado da saída

```
⏱️ Comparação de performance (1M registros):
   UDF Python:      X.XX s
   Pandas UDF:      X.XX s
   when/otherwise:  X.XX s

📊 Rankings de velocidade:
   1° when/otherwise (Xx mais rápido que UDF)
   2° Pandas UDF     (Xx mais rápido que UDF)
   3° UDF Python     (baseline)
```

### Critério de validação

- A Pandas UDF deve produzir os mesmos resultados que a UDF Python
- A Pandas UDF deve ser mais rápida que a UDF Python convencional
- `when/otherwise` deve ainda ser a mais rápida (pois roda 100% na JVM)

---

## Exercício 3.5: UDF de Segmentação de Clientes (Múltiplas Colunas)

### Pergunta de negócio

Ana precisa de uma segmentação mais sofisticada dos clientes que leve em conta múltiplos fatores: quanto gastam no total, quantas compras fazem e há quanto tempo não compram. As regras de segmentação são:

| Segmento | Critérios |
|----------|-----------|
| **VIP** | `faturamento_total > R$ 5.000` E `num_compras >= 20` E `dias_desde_ultima < 30` |
| **Gold** | `faturamento_total > R$ 2.000` E `num_compras >= 10` E `dias_desde_ultima < 60` |
| **Silver** | `faturamento_total > R$ 500` E `num_compras >= 5` |
| **Bronze** | `faturamento_total > R$ 100` OU `num_compras >= 2` |
| **Em Risco** | `dias_desde_ultima > 90` (independente de outros critérios — tem prioridade) |

> **Nota:** A regra "Em Risco" tem prioridade — se o cliente não compra há mais de 90 dias, é "Em Risco" mesmo que seja VIP em faturamento.

### O que implementar

1. Primeiro, crie um DataFrame agregado por cliente com: `faturamento_total`, `num_compras`, `dias_desde_ultima_compra`
2. Crie uma UDF que receba essas 3 métricas e retorne o segmento
3. Aplique a UDF ao DataFrame agregado
4. Mostre a distribuição de clientes por segmento

### Dicas

- Para passar múltiplas colunas à UDF: `classificar(col("fat_total"), col("num_compras"), col("dias"))` — a UDF recebe múltiplos parâmetros
- Para `dias_desde_ultima_compra`: use `datediff(current_date(), max("order_date"))` na agregação
- A ordem de avaliação importa! Verifique "Em Risco" primeiro, depois VIP → Gold → Silver → Bronze
- Import: `from pyspark.sql.functions import current_date, max as spark_max, datediff`
- Cuidado com valores `None` nos parâmetros — trate todos

### Formato esperado da saída

```
📊 Segmentação de Clientes DataFlow:
+---------+--------+-----------+-----------------------+
| segmento|clientes|percentual |ticket_medio_segmento  |
+---------+--------+-----------+-----------------------+
|      VIP|    XXXX|      X.X% |           R$ X,XXX.XX |
|     Gold|    XXXX|     XX.X% |           R$ X,XXX.XX |
|   Silver|    XXXX|     XX.X% |             R$ XXX.XX |
|   Bronze|    XXXX|     XX.X% |             R$ XXX.XX |
| Em Risco|    XXXX|     XX.X% |             R$ XXX.XX |
+---------+--------+-----------+-----------------------+
```

### Critério de validação

- Todos os clientes devem ter um segmento atribuído (sem nulls)
- A regra "Em Risco" deve ter prioridade sobre as demais
- O percentual total deve somar 100%
- VIP deve ter o maior ticket médio e Em Risco pode ter ticket alto (eram bons clientes que pararam)

---

## Exercício 3.6: Analisar Plano de Execução com explain()

### Pergunta de negócio

O time de infraestrutura reportou que o pipeline de classificação está lento em produção. Para diagnosticar, precisamos entender o que o Spark está fazendo "por baixo dos panos". O plano de execução revela como o Spark traduz nossas transformações em operações físicas.

### O que implementar

1. Execute `df.explain(mode="extended")` no DataFrame com a UDF Python aplicada
2. Execute `df.explain(mode="extended")` no DataFrame com `when/otherwise` aplicado
3. Compare os dois planos de execução
4. Identifique a diferença principal: onde aparece `BatchEvalPython`?
5. Explique o impacto de `BatchEvalPython` na performance

### Dicas

- Use `df.explain(mode="extended")` para ver o plano completo (logical + physical)
- No plano da UDF, procure pelo nó **`BatchEvalPython`** ou **`ArrowEvalPython`** — ele indica serialização Python
- No plano com `when/otherwise`, NÃO haverá esse nó (tudo roda na JVM)
- O modo `"formatted"` mostra o plano de forma mais legível: `df.explain(mode="formatted")`
- Observe também se há **`Exchange`** (shuffle) — isso indica redistribuição de dados entre nós

### Formato esperado da saída

```
📋 Plano de execução COM UDF:
== Physical Plan ==
*(1) Project [... classificar_ticket(total_amount) AS faixa_ticket]
+- BatchEvalPython [classificar_ticket(...)], [...]    ← ⚠️ SERIALIZAÇÃO!
   +- *(1) ColumnarToRow
      +- FileScan parquet [...]

📋 Plano de execução com when/otherwise:
== Physical Plan ==
*(1) Project [... CASE WHEN ... AS faixa_ticket_nativo]
+- *(1) FileScan parquet [...]

💡 Diferença: [sua análise aqui]
```

### Critério de validação

- Identificar corretamente o nó `BatchEvalPython` (ou `ArrowEvalPython` para Pandas UDF)
- Explicar que esse nó representa a serialização de dados entre JVM e processo Python
- Entender que funções nativas (`when/otherwise`) são otimizadas pelo Catalyst Optimizer e não precisam dessa serialização
- Bonus: identificar se há `Exchange` (shuffle) no plano e explicar por quê

---

## Resumo e Reflexão

Após completar este exercício, você deve ser capaz de responder:

1. **Quando usar UDF Python?** → Para regras de negócio complexas que não podem ser expressas com funções nativas, quando a legibilidade é prioritária e o volume de dados é moderado.

2. **Quando usar when/otherwise?** → Sempre que possível! Classificações simples baseadas em faixas de valor são perfeitas para `when/otherwise`. Performance será sempre melhor.

3. **Quando usar Pandas UDF?** → Quando precisa de lógica Python mas com volume alto de dados. A vetorização via Apache Arrow oferece um bom meio-termo entre flexibilidade e performance.

4. **Como diagnosticar performance?** → Use `explain()` para ver o plano de execução. Procure por `BatchEvalPython` como indicador de gargalo potencial.

> **💡 Regra de ouro de Carlos:** "Em produção, evite UDFs Python sempre que possível. Se a lógica cabe em `when/otherwise`, use. Se precisa de Python, use Pandas UDF. UDF Python convencional é último recurso — reservada para lógica que realmente não tem como ser vetorizada."

---

## Referências

- [Spark SQL Functions — when](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.when.html)
- [PySpark UDF Guide](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Pandas UDF (Vectorized UDFs)](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#pandas-udfs-a-k-a-vectorized-udfs)
- [Understanding Spark Query Plans](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
