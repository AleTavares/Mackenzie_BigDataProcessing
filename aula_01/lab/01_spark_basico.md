# Exercício 1 — Primeiros Passos com PySpark

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Bem-vindo ao seu primeiro dia como engenheiro de dados na DataFlow Analytics! A Marina me pediu para te mostrar as ferramentas que usamos no dia a dia. Nosso primeiro grande cliente de e-commerce nos enviou mais de 100 mil registros de vendas de 2023. Pandas já engasga com esse volume — e só vai crescer. Por isso adotamos o Apache Spark. Vamos começar pelo básico: conectar no cluster, carregar os dados e explorar o que temos."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar e configurar uma SparkSession
- Ler arquivos CSV com inferência de schema
- Explorar a estrutura de um DataFrame (schema, tipos)
- Visualizar e contar registros
- Selecionar colunas específicas
- Filtrar dados com condições simples e compostas
- Encadear operações no estilo funcional

## Pré-requisitos

- Ambiente Docker rodando (ver `00_setup.md`)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `vendas_2023.csv` disponível na pasta `data/`

## Duração Estimada

⏱️ ~30 minutos

---

## Passo 1: Criar a SparkSession

**Descrição:** A SparkSession é o ponto de entrada para qualquer interação com o Apache Spark. Pense nela como a "conexão" entre seu notebook e o cluster de processamento distribuído. Precisamos criá-la antes de fazer qualquer operação com dados.

**Código:**

```python
from pyspark.sql import SparkSession

# Criar SparkSession conectada ao cluster Spark
spark = SparkSession.builder \
    .appName("DataFlow-Aula01-Basico") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

print(f"✅ SparkSession criada com sucesso!")
print(f"   Versão do Spark: {spark.version}")
print(f"   Aplicação: {spark.sparkContext.appName}")
print(f"   Master: {spark.sparkContext.master}")
```

**Resultado esperado:**

```
✅ SparkSession criada com sucesso!
   Versão do Spark: 3.5.x
   Aplicação: DataFlow-Aula01-Basico
   Master: spark://spark-master:7077
```

**Explicação técnica:**

- `.appName(...)` — define o nome da aplicação que aparecerá no Spark UI (http://localhost:8080)
- `.master(...)` — indica onde o Spark deve executar os jobs. Usamos o endereço do container `spark-master`
- `.config("spark.executor.memory", "1g")` — aloca 1 GB de RAM para cada executor (processo que faz o trabalho)
- `.getOrCreate()` — cria uma nova sessão ou reutiliza uma existente (evita erro ao re-executar a célula)

> **💡 Dica de Carlos:** "Em produção, o `master` apontaria para um cluster com dezenas de máquinas. Aqui no lab usamos um cluster local com Docker, mas o código é idêntico — essa é a beleza do Spark."

---

## Passo 2: Ler o Arquivo vendas_2023.csv

**Descrição:** Vamos carregar o arquivo de vendas do nosso primeiro grande cliente. O Spark suporta diversos formatos de dados — começamos com CSV, o mais universal. Usaremos inferência automática de schema para que o Spark detecte os tipos de cada coluna.

**Código:**

```python
# Ler o arquivo CSV de vendas
df_vendas = spark.read.csv(
    "data/vendas_2023.csv",
    header=True,
    inferSchema=True
)

print(f"✅ Arquivo carregado com sucesso!")
print(f"   Tipo do objeto: {type(df_vendas)}")
```

**Resultado esperado:**

```
✅ Arquivo carregado com sucesso!
   Tipo do objeto: <class 'pyspark.sql.dataframe.DataFrame'>
```

**Explicação técnica:**

- `spark.read.csv(...)` — método para leitura de arquivos CSV (aceita um path ou padrão glob)
- `header=True` — indica que a primeira linha do CSV contém os nomes das colunas
- `inferSchema=True` — o Spark faz uma passada extra no arquivo para detectar automaticamente os tipos (string, int, double, timestamp). Sem isso, tudo seria lido como `string`
- O resultado é um **DataFrame** — a estrutura principal de dados no Spark, equivalente a uma tabela com linhas e colunas tipadas

> **💡 Dica de Carlos:** "O `inferSchema=True` é ótimo para exploração, mas em produção preferimos definir o schema explicitamente. Inferir tipos exige ler o arquivo duas vezes — com 1 bilhão de linhas, isso faz diferença. Na próxima aula veremos como definir schemas fixos."

---

## Passo 3: Explorar o Schema

**Descrição:** Antes de analisar os dados, precisamos entender sua estrutura. O `printSchema()` mostra todas as colunas, seus tipos e se aceitam valores nulos. É o equivalente a um `DESCRIBE TABLE` em SQL.

**Código:**

```python
# Exibir o schema (estrutura) do DataFrame
df_vendas.printSchema()
```

**Resultado esperado:**

```
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- order_date: timestamp (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- shipping_city: string (nullable = true)
 |-- shipping_state: string (nullable = true)
 |-- status: string (nullable = true)
 |-- partner_source: string (nullable = true)
```

**Explicação técnica:**

O dataset possui **12 colunas** com os seguintes tipos:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `order_id` | string | Identificador único do pedido (UUID) |
| `customer_id` | string | ID do cliente (ex: CUST_00123) |
| `product_id` | string | ID do produto (ex: PROD_0456) |
| `quantity` | integer | Quantidade de itens no pedido |
| `unit_price` | double | Preço unitário do produto (R$) |
| `total_amount` | double | Valor total = quantity × unit_price |
| `order_date` | timestamp | Data/hora do pedido |
| `payment_method` | string | Meio de pagamento (credit_card, pix, etc.) |
| `shipping_city` | string | Cidade de entrega |
| `shipping_state` | string | Estado de entrega (sigla UF) |
| `status` | string | Status do pedido (pending, shipped, delivered, cancelled) |
| `partner_source` | string | Parceiro de origem dos dados |

O `inferSchema` detectou corretamente: `quantity` como inteiro, `unit_price`/`total_amount` como double (decimal), e `order_date` como timestamp.

> **💡 Dica de Carlos:** "Sempre comece explorando o schema. Se uma coluna numérica for inferida como string, suas agregações vão falhar silenciosamente. Aqui está tudo certo, mas na Aula 3 veremos dados 'sujos' onde isso acontece."

---

## Passo 4: Visualizar os Primeiros Registros

**Descrição:** Agora vamos olhar os dados reais. O `show()` exibe registros no formato tabular. Veremos duas variações: com truncamento (padrão) e sem truncamento (para ver valores completos).

**Código (com truncamento — padrão):**

```python
# Mostrar as 5 primeiras linhas (colunas longas são truncadas em 20 caracteres)
df_vendas.show(5)
```

**Resultado esperado:**

```
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+---------------+--------------+---------+--------------+
|            order_id|customer_id|product_id|quantity|unit_price|total_amount|         order_date|payment_method|  shipping_city|shipping_state|   status|partner_source|
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+---------------+--------------+---------+--------------+
|a1b2c3d4-e5f6-789...|  CUST_00342|  PROD_1247|       3|    149.99|      449.97|2023-03-15 00:00:00|   credit_card|      São Paulo|            SP|delivered|   parceiro_a|
|f7e8d9c0-b1a2-345...|  CUST_12045|  PROD_0893|       1|    899.90|      899.90|2023-05-22 00:00:00|           pix|Rio de Janeiro|            RJ|  shipped|   parceiro_b|
|...                 |        ... |      ... |     ...|       ...|         ...|                ...|           ...|            ...|           ...|      ...|           ...|
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+---------------+--------------+---------+--------------+
only showing top 5 rows
```

**Código (sem truncamento):**

```python
# Mostrar sem truncar valores — útil para ver UUIDs completos e nomes de cidades
df_vendas.show(5, truncate=False)
```

**Resultado esperado:**

```
+------------------------------------+-----------+----------+--------+----------+------------+-------------------+--------------+------------------+--------------+---------+--------------+
|order_id                            |customer_id|product_id|quantity|unit_price|total_amount|order_date         |payment_method|shipping_city     |shipping_state|status   |partner_source|
+------------------------------------+-----------+----------+--------+----------+------------+-------------------+--------------+------------------+--------------+---------+--------------+
|a1b2c3d4-e5f6-7890-abcd-ef1234567890|CUST_00342 |PROD_1247 |3       |149.99    |449.97      |2023-03-15 00:00:00|credit_card   |São Paulo         |SP            |delivered|parceiro_a    |
|f7e8d9c0-b1a2-3456-7890-abcdef012345|CUST_12045 |PROD_0893 |1       |899.90    |899.90      |2023-05-22 00:00:00|pix           |Rio de Janeiro    |RJ            |shipped  |parceiro_b    |
|...                                                                                                                                                                                      |
+------------------------------------+-----------+----------+--------+----------+------------+-------------------+--------------+------------------+--------------+---------+--------------+
only showing top 5 rows
```

**Explicação técnica:**

- `show(n)` — exibe as primeiras `n` linhas em formato tabular no console
- Por padrão, `truncate=True` limita cada célula a 20 caracteres (útil para tabelas largas)
- `truncate=False` mostra o conteúdo completo — importante para verificar UUIDs, nomes completos, etc.
- O `show()` é uma **ação** no Spark (não é lazy) — ele efetivamente executa a leitura dos dados

> **💡 Dica de Marina:** "O `show()` é ótimo para debug rápido, mas evite usá-lo em produção com DataFrames grandes. Para análise mais rica, considere `df.limit(5).toPandas()` que permite visualizações melhores no Jupyter."

---

## Passo 5: Contar Registros

**Descrição:** Uma das primeiras perguntas ao receber um dataset é: "quantos registros temos?". O `count()` percorre todo o DataFrame e retorna o número total de linhas. Esperamos ~100 mil registros conforme o cliente informou.

**Código:**

```python
# Contar o total de registros no DataFrame
total_registros = df_vendas.count()
print(f"📊 Total de registros: {total_registros:,}")
```

**Resultado esperado:**

```
📊 Total de registros: 100,000
```

**Explicação técnica:**

- `count()` é uma **ação** — dispara a execução real da leitura e contagem
- No Spark, operações são divididas em **transformações** (lazy, apenas definem o plano) e **ações** (executam de fato). `count()` é uma ação
- Para 100K registros, o `count()` deve ser instantâneo no nosso cluster local. Com bilhões de registros em produção, o Spark distribui a contagem entre workers e soma os resultados
- A formatação `{total_registros:,}` adiciona separador de milhares para melhor legibilidade

> **💡 Dica de Carlos:** "O `count()` precisa varrer todo o dataset. Se você só quer saber se o DataFrame está vazio, use `df.head(1)` ou `df.isEmpty()` — são muito mais rápidos porque param no primeiro registro encontrado."

---

## Passo 6: Selecionar Colunas Específicas

**Descrição:** Raramente precisamos de todas as 12 colunas ao mesmo tempo. O `select()` permite projetar apenas as colunas de interesse — isso melhora a legibilidade e pode otimizar a performance em datasets com muitas colunas.

**Código:**

```python
# Selecionar apenas as colunas de interesse para análise de faturamento
df_resumo = df_vendas.select("order_id", "total_amount", "shipping_state")

# Verificar o schema do novo DataFrame
df_resumo.printSchema()

# Visualizar os dados selecionados
df_resumo.show(5)
```

**Resultado esperado:**

```
root
 |-- order_id: string (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- shipping_state: string (nullable = true)

+--------------------+------------+--------------+
|            order_id|total_amount|shipping_state|
+--------------------+------------+--------------+
|a1b2c3d4-e5f6-789...|      449.97|            SP|
|f7e8d9c0-b1a2-345...|      899.90|            RJ|
|1a2b3c4d-5e6f-789...|      124.50|            MG|
|9f8e7d6c-5b4a-321...|     2399.80|            SP|
|0a1b2c3d-4e5f-678...|       79.90|            BA|
+--------------------+------------+--------------+
only showing top 5 rows
```

**Explicação técnica:**

- `select("col1", "col2", ...)` — cria um **novo** DataFrame com apenas as colunas especificadas
- O DataFrame original (`df_vendas`) permanece inalterado — DataFrames no Spark são **imutáveis**
- Selecionar menos colunas pode melhorar performance: menos dados são transferidos pela rede entre workers
- Equivalente SQL: `SELECT order_id, total_amount, shipping_state FROM vendas`

> **💡 Dica de Carlos:** "Você também pode usar `select()` com objetos `col()` para renomear ou transformar colunas no mesmo passo. Ex: `df.select(col('total_amount').alias('valor'))`. Veremos isso mais adiante."

---

## Passo 7: Filtrar Registros

**Descrição:** Agora vamos selecionar subconjuntos de dados com base em condições. A Ana (PO) pediu uma análise focada em São Paulo e em pedidos de alto valor. Vamos usar `filter()` para atender essas demandas.

### 7.1 — Filtrar por estado (São Paulo)

**Código:**

```python
from pyspark.sql.functions import col

# Filtrar apenas pedidos com entrega em São Paulo
df_sp = df_vendas.filter(col("shipping_state") == "SP")

print(f"📍 Pedidos em São Paulo: {df_sp.count():,}")
df_sp.show(5)
```

**Resultado esperado:**

```
📍 Pedidos em São Paulo: ~25,000
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
|            order_id|customer_id|product_id|quantity|unit_price|total_amount|         order_date|payment_method| shipping_city|shipping_state|   status|partner_source|
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
|...(registros de SP)                                                                                                                                                  |
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
only showing top 5 rows
```

### 7.2 — Filtrar por valor (pedidos acima de R$ 500)

**Código:**

```python
# Filtrar pedidos com valor total superior a R$ 500
df_alto_valor = df_vendas.filter(col("total_amount") > 500)

print(f"💰 Pedidos acima de R$ 500: {df_alto_valor.count():,}")
df_alto_valor.show(5)
```

**Resultado esperado:**

```
💰 Pedidos acima de R$ 500: ~XX,XXX
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
|            order_id|customer_id|product_id|quantity|unit_price|total_amount|         order_date|payment_method| shipping_city|shipping_state|   status|partner_source|
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
|...(pedidos > R$500)                                                                                                                                                  |
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
only showing top 5 rows
```

### 7.3 — Combinar filtros (AND)

**Código:**

```python
# Filtrar pedidos de SP com valor acima de R$ 500
df_sp_alto_valor = df_vendas.filter(
    (col("shipping_state") == "SP") & (col("total_amount") > 500)
)

print(f"📍💰 Pedidos em SP acima de R$ 500: {df_sp_alto_valor.count():,}")
df_sp_alto_valor.show(5)
```

**Resultado esperado:**

```
📍💰 Pedidos em SP acima de R$ 500: ~X,XXX
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+---------+--------------+---------+--------------+
|            order_id|customer_id|product_id|quantity|unit_price|total_amount|         order_date|payment_method|shipping_city|shipping_state|   status|partner_source|
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+---------+--------------+---------+--------------+
|...(pedidos SP > 500)                                                                                                                                               |
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+---------+--------------+---------+--------------+
only showing top 5 rows
```

**Explicação técnica:**

- `filter(condição)` — retorna um novo DataFrame contendo apenas as linhas que satisfazem a condição
- `col("nome_coluna")` — referência tipada a uma coluna (importado de `pyspark.sql.functions`)
- Operadores de comparação: `==`, `!=`, `>`, `<`, `>=`, `<=`
- Combinação de condições: `&` (AND), `|` (OR), `~` (NOT)
- **Importante:** cada condição individual deve estar entre parênteses ao combinar: `(cond1) & (cond2)`
- Equivalente SQL: `SELECT * FROM vendas WHERE shipping_state = 'SP' AND total_amount > 500`

> **💡 Dica de Carlos:** "Note os parênteses obrigatórios ao combinar condições com `&` e `|`. Isso é porque o Python resolve a precedência de operadores de forma diferente — sem parênteses, você recebe um erro confuso sobre 'ambiguous truth value'. Sempre use parênteses!"

---

## Passo 8: Usar where vs filter

**Descrição:** Você pode encontrar exemplos online usando `where()` em vez de `filter()`. Vamos verificar que ambos são exatamente equivalentes — são aliases (sinônimos) no Spark.

**Código:**

```python
# filter() e where() são a mesma coisa!
df_filtrado_1 = df_vendas.filter(col("shipping_state") == "RJ")
df_filtrado_2 = df_vendas.where(col("shipping_state") == "RJ")

# Comparar contagens — devem ser idênticas
count_filter = df_filtrado_1.count()
count_where = df_filtrado_2.count()

print(f"Usando filter(): {count_filter:,} registros")
print(f"Usando where():  {count_where:,} registros")
print(f"São iguais? {count_filter == count_where} ✅")
```

**Resultado esperado:**

```
Usando filter(): ~15,000 registros
Usando where():  ~15,000 registros
São iguais? True ✅
```

**Código (where com string SQL):**

```python
# where() também aceita expressões SQL como string
df_sql_style = df_vendas.where("shipping_state = 'MG' AND total_amount > 1000")

print(f"🔍 Pedidos MG > R$1000 (sintaxe SQL): {df_sql_style.count():,}")
df_sql_style.show(3)
```

**Resultado esperado:**

```
🔍 Pedidos MG > R$1000 (sintaxe SQL): ~X,XXX
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
|            order_id|customer_id|product_id|quantity|unit_price|total_amount|         order_date|payment_method| shipping_city|shipping_state|   status|partner_source|
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
|...                                                                                                                                                                   |
+--------------------+-----------+----------+--------+----------+------------+-------------------+--------------+--------------+--------------+---------+--------------+
only showing top 3 rows
```

**Explicação técnica:**

- `filter()` e `where()` são **100% idênticos** — `where` é apenas um alias para `filter`
- A escolha é puramente estilística. `where` pode ser mais intuitivo para quem vem de SQL
- `where()` aceita tanto expressões `col()` quanto strings SQL (ex: `"coluna > 100"`)
- Na DataFlow, padronizamos o uso de `filter()` com `col()` por ser mais explícito e ter melhor suporte de IDE (autocomplete)

> **💡 Dica de Marina:** "Use a convenção que sua equipe definir e seja consistente. Na DataFlow, preferimos `filter(col(...))` porque o editor ajuda com autocomplete e detecta erros de nome de coluna antes de executar. Mas em prototipação rápida, a sintaxe SQL de `where('coluna > 100')` é bastante prática."

---

## Passo 9: Encadear Operações

**Descrição:** Um dos pontos fortes do Spark é a API fluente (method chaining). Podemos encadear `select`, `filter` e `show` em uma única expressão legível. Isso produz código mais limpo e é o estilo padrão em projetos profissionais.

**Código:**

```python
# Encadear operações: selecionar colunas → filtrar → mostrar
# "Quero ver o ID e valor dos pedidos de SP pagos com PIX"
df_vendas \
    .select("order_id", "total_amount", "shipping_state", "payment_method") \
    .filter(col("shipping_state") == "SP") \
    .filter(col("payment_method") == "pix") \
    .show(10)
```

**Resultado esperado:**

```
+--------------------+------------+--------------+--------------+
|            order_id|total_amount|shipping_state|payment_method|
+--------------------+------------+--------------+--------------+
|a1b2c3d4-e5f6-789...|      199.90|            SP|           pix|
|d4e5f6a7-b8c9-012...|     1249.50|            SP|           pix|
|7a8b9c0d-1e2f-345...|       89.90|            SP|           pix|
|...                 |         ...|            SP|           pix|
+--------------------+------------+--------------+--------------+
only showing top 10 rows
```

**Código (variação — tudo em um pipeline com contagem):**

```python
# Pipeline completo: quantos pedidos cancelados tiveram valor > R$ 1000?
resultado = df_vendas \
    .select("order_id", "total_amount", "status", "shipping_state") \
    .filter(col("status") == "cancelled") \
    .filter(col("total_amount") > 1000)

print(f"🚫 Pedidos cancelados acima de R$ 1000: {resultado.count():,}")
resultado.show(5)
```

**Resultado esperado:**

```
🚫 Pedidos cancelados acima de R$ 1000: ~X,XXX
+--------------------+------------+---------+--------------+
|            order_id|total_amount|   status|shipping_state|
+--------------------+------------+---------+--------------+
|...(cancelados > 1k)                                      |
+--------------------+------------+---------+--------------+
only showing top 5 rows
```

**Explicação técnica:**

- O encadeamento funciona porque cada operação (`select`, `filter`) retorna um **novo DataFrame**
- A ordem importa: se você fizer `select("order_id").filter(col("shipping_state") == "SP")` dará erro, pois `shipping_state` não existe mais após o select
- **Regra prática:** faça `filter` antes de `select` quando possível, ou inclua as colunas usadas no filtro dentro do `select`
- Internamente, o Spark otimiza a ordem das operações (Catalyst Optimizer) — mas para legibilidade, mantenha uma ordem lógica no código
- Equivalente SQL: `SELECT order_id, total_amount FROM vendas WHERE state = 'SP' AND payment = 'pix' LIMIT 10`

> **💡 Dica de Carlos:** "Essa é a forma como escrevemos código de produção na DataFlow. Cada linha da cadeia representa uma etapa lógica da transformação. Use `\` no final da linha para quebrar pipelines longos — fica muito mais legível que uma linha gigante."

---

## Resumo do Exercício

Neste exercício você aprendeu os fundamentos para trabalhar com dados no Apache Spark:

| Operação | Método | Tipo | Equivalente SQL |
|----------|--------|------|-----------------|
| Conectar ao Spark | `SparkSession.builder...getOrCreate()` | Setup | `CONNECT` |
| Ler dados | `spark.read.csv(...)` | Leitura | `LOAD DATA` |
| Ver estrutura | `df.printSchema()` | Exploração | `DESCRIBE TABLE` |
| Ver dados | `df.show(n)` | Ação | `SELECT * LIMIT n` |
| Contar linhas | `df.count()` | Ação | `SELECT COUNT(*)` |
| Selecionar colunas | `df.select(...)` | Transformação | `SELECT col1, col2` |
| Filtrar linhas | `df.filter(...)` / `df.where(...)` | Transformação | `WHERE condição` |
| Encadear | `df.select(...).filter(...).show()` | Pipeline | Query completa |

### Conceitos-chave

1. **SparkSession** é o ponto de entrada para toda interação com o Spark
2. **DataFrames** são imutáveis — cada operação cria um novo DataFrame
3. **Transformações** (select, filter) são lazy — só executam quando uma **ação** (show, count) é chamada
4. **filter()** e **where()** são sinônimos — use o que sua equipe preferir
5. **Encadeamento** de operações produz código limpo e profissional

> **Carlos:** "Excelente! Agora você já sabe abrir uma conexão Spark, carregar dados e fazer operações básicas de exploração. No próximo exercício, vamos usar `groupBy` e agregações para gerar os relatórios que a Ana precisa — faturamento por estado, ticket médio, total de pedidos. Isso é o que realmente gera valor pro negócio."

---

## Próximo Exercício

➡️ **Exercício 2 — Agregações e Ordenação** (`02_agregacoes.md`): groupBy, agg, sum, avg, count, orderBy
