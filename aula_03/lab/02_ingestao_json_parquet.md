# Exercício 2 — Ingestão de JSON e Parquet (Parceiros B e C)

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Muito bem! Parceiro A resolvido. Agora vamos aos outros dois. O Parceiro B é uma fintech moderna — eles têm uma REST API que gera dumps em JSON. Os arquivos vêm com metadados de paginação, timestamps em UTC e os dados de vendas dentro de um array `data`. Já o Parceiro C é mais avançado: eles já têm um data lake próprio e nos enviam arquivos Parquet com compressão Snappy. É o formato mais fácil de ler — mas o schema deles tem colunas extras que os outros não têm. Vamos ingerir os dois e depois comparar os schemas dos 3 parceiros."

## Objetivos

Ao final deste exercício, você será capaz de:

- Ler arquivos JSON multi-file com `multiLine=True`
- Identificar e navegar em schemas hierárquicos/aninhados
- Acessar campos aninhados com a sintaxe `col("struct.field")`
- Usar `explode()` para transformar arrays em linhas
- Ler arquivos Parquet (schema embutido, sem configuração extra)
- Comparar schemas de diferentes fontes de dados
- Adicionar metadados de ingestão padronizados (_source, _ingestion_ts, _file_origin)

## Pré-requisitos

- SparkSession ativa (criada no Exercício 1)
- Datasets disponíveis:
  - `data/aula_03/parceiro_b/*.json` — dumps de API REST (3 arquivos paginados)
  - `data/aula_03/parceiro_c/vendas_parceiro_c.parquet` — export do data lake do parceiro

## Duração Estimada

⏱️ ~25 minutos

---

# PARTE A — Parceiro B (JSON de API REST)

## Passo 1: Ler JSON e Observar a Estrutura Hierárquica

**Descrição:** O Parceiro B exporta dados de sua REST API em formato JSON. Cada arquivo representa uma "página" da API, com metadados no topo (`api_version`, `exported_at`, `page`, `total_pages`) e os registros de vendas dentro do campo `data`. Vamos ler com `multiLine=True` porque cada arquivo é um JSON completo (não um JSON-por-linha).

**Código:**

```python
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    explode, from_utc_timestamp, to_timestamp
)

# Ler JSON multi-line (cada arquivo é um documento JSON completo)
df_json_raw = spark.read.json(
    "data/aula_03/parceiro_b/*.json",
    multiLine=True
)

# Quantos "documentos" JSON foram lidos?
print(f"📊 Documentos JSON lidos: {df_json_raw.count()}")
print(f"📊 Colunas no nível raiz: {df_json_raw.columns}")
print()

# Mostrar schema hierárquico
print("Schema hierárquico do JSON:")
df_json_raw.printSchema()
```

**Resultado esperado:**

```
📊 Documentos JSON lidos: 3
📊 Colunas no nível raiz: ['api_version', 'data', 'exported_at', 'page', 'total_pages']

Schema hierárquico do JSON:
root
 |-- api_version: string (nullable = true)
 |-- data: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- customer_id: string (nullable = true)
 |    |    |-- order_date: string (nullable = true)
 |    |    |-- order_id: string (nullable = true)
 |    |    |-- partner_source: string (nullable = true)
 |    |    |-- payment_method: string (nullable = true)
 |    |    |-- product_id: string (nullable = true)
 |    |    |-- quantity: long (nullable = true)
 |    |    |-- shipping_city: string (nullable = true)
 |    |    |-- shipping_state: string (nullable = true)
 |    |    |-- status: string (nullable = true)
 |    |    |-- total_amount: double (nullable = true)
 |    |    |-- unit_price: double (nullable = true)
 |-- exported_at: string (nullable = true)
 |-- page: long (nullable = true)
 |-- total_pages: long (nullable = true)
```

**Explicação técnica:**

- **`multiLine=True`** — essencial para JSON "documento completo". Sem isso, o Spark espera um JSON por linha (formato JSON Lines / NDJSON). Com `multiLine=True`, ele lê cada arquivo como um único documento
- **3 documentos** — cada arquivo JSON é um "documento" (uma página da API). Temos 3 arquivos: `api_dump_page_001.json`, `api_dump_page_002.json`, `api_dump_page_003.json`
- **Schema hierárquico** — note que `data` é um `array` de `struct`. Cada struct tem os campos de venda. Os metadados da API (`api_version`, `page`, etc.) ficam no nível raiz
- **Diferença de CSV** — no CSV cada linha é um registro. No JSON, a estrutura pode ter múltiplos níveis de aninhamento. É mais expressivo, mas requer "achatamento" (flatten) para trabalhar como tabela

> **💡 Dica de Carlos:** "JSON é o formato preferido de APIs modernas. Mas cuidado: `multiLine=True` carrega o arquivo inteiro na memória de cada executor. Para arquivos muito grandes (>1GB), prefira JSON Lines (um JSON por linha) que permite leitura paralela."

---

## Passo 2: Extrair Metadados da API

**Descrição:** Antes de explodir o array `data`, vamos inspecionar os metadados da API. Isso nos ajuda a validar que todos os arquivos foram lidos e entender a paginação.

**Código:**

```python
# Verificar metadados de cada página da API
print("📋 Metadados das páginas da API:")
df_json_raw.select(
    "api_version", "page", "total_pages", "exported_at"
).show(truncate=False)
```

**Resultado esperado:**

```
📋 Metadados das páginas da API:
+-----------+----+-----------+------------------------+
|api_version|page|total_pages|exported_at             |
+-----------+----+-----------+------------------------+
|2.1        |1   |3          |2023-12-15T10:30:00Z    |
|2.1        |2   |3          |2023-12-15T10:30:00Z    |
|2.1        |3   |3          |2023-12-15T10:30:00Z    |
+-----------+----+-----------+------------------------+
```

**Explicação técnica:**

- **`api_version: 2.1`** — versão da API. Útil para lidar com schema evolution (versões diferentes podem ter campos diferentes)
- **`page` e `total_pages`** — confirmam que lemos todas as 3 páginas. Se `total_pages` fosse 5 e tivéssemos apenas 3 arquivos, saberíamos que há dados faltando
- **`exported_at`** — timestamp UTC do export. Vamos usar isso como metadado de quando o parceiro gerou os dados
- Na prática, esses metadados são úteis para auditoria e reconciliação com o parceiro

---

## Passo 3: Explodir o Array `data` — Transformar Linhas Aninhadas em Registros

**Descrição:** O campo `data` é um array com ~10.000 registros por página. Precisamos "explodir" esse array para que cada elemento vire uma linha independente no DataFrame. Usamos `explode()` — uma das funções mais importantes para trabalhar com JSON no Spark.

**Código:**

```python
from pyspark.sql.functions import explode, size

# Quantos registros tem em cada array "data"?
print("📊 Tamanho do array 'data' por página:")
df_json_raw.select("page", size("data").alias("registros_na_pagina")).show()

# Explodir o array: cada elemento de "data" vira uma linha
df_exploded = df_json_raw.select(
    "page",
    "exported_at",
    explode("data").alias("venda")
)

print(f"\n📊 Total de registros após explode: {df_exploded.count():,}")
print(f"📊 Schema após explode:")
df_exploded.printSchema()
```

**Resultado esperado:**

```
📊 Tamanho do array 'data' por página:
+----+-------------------+
|page|registros_na_pagina|
+----+-------------------+
|   1|              10000|
|   2|              10000|
|   3|              10000|
+----+-------------------+

📊 Total de registros após explode: 30,000
📊 Schema após explode:
root
 |-- page: long (nullable = true)
 |-- exported_at: string (nullable = true)
 |-- venda: struct (nullable = true)
 |    |-- customer_id: string (nullable = true)
 |    |-- order_date: string (nullable = true)
 |    |-- order_id: string (nullable = true)
 |    |-- partner_source: string (nullable = true)
 |    |-- payment_method: string (nullable = true)
 |    |-- product_id: string (nullable = true)
 |    |-- quantity: long (nullable = true)
 |    |-- shipping_city: string (nullable = true)
 |    |-- shipping_state: string (nullable = true)
 |    |-- status: string (nullable = true)
 |    |-- total_amount: double (nullable = true)
 |    |-- unit_price: double (nullable = true)
```

**Explicação técnica:**

- **`explode("data")`** — transforma cada elemento do array em uma linha separada. Se tínhamos 3 linhas (páginas) com 10.000 elementos cada, agora temos 30.000 linhas
- **`alias("venda")`** — dá um nome ao struct resultante. Sem alias, o Spark usa `col` como nome padrão
- **O resultado é um struct** — cada "venda" ainda é um objeto aninhado (struct). No próximo passo vamos "achatar" (flatten) para colunas individuais
- **Mantivemos `page` e `exported_at`** — metadados da API que podem ser úteis para rastreabilidade

> **💡 Dica de Carlos:** "O `explode()` é o equivalente do `UNNEST` em SQL. Ele multiplica linhas — se um array tem 1000 elementos, você ganha 1000 linhas. Cuidado com explosão de cardinalidade em arrays muito grandes! Sempre verifique o `size()` antes."

---

## Passo 4: Achatar (Flatten) o Struct — Extrair Campos Aninhados

**Descrição:** Agora que cada linha tem um struct `venda`, precisamos extrair os campos internos como colunas individuais. Usamos a sintaxe de ponto: `col("venda.order_id")`. Isso é o "flatten" — transformar uma estrutura hierárquica em tabular.

**Código:**

```python
# Achatar (flatten) o struct "venda" em colunas individuais
df_parceiro_b = df_exploded.select(
    col("venda.order_id").alias("order_id"),
    col("venda.customer_id").alias("customer_id"),
    col("venda.product_id").alias("product_id"),
    col("venda.quantity").alias("quantity"),
    col("venda.unit_price").alias("unit_price"),
    col("venda.total_amount").alias("total_amount"),
    col("venda.order_date").alias("order_date"),
    col("venda.payment_method").alias("payment_method"),
    col("venda.shipping_city").alias("shipping_city"),
    col("venda.shipping_state").alias("shipping_state"),
    col("venda.status").alias("status"),
    col("venda.partner_source").alias("partner_source"),
    col("exported_at").alias("api_exported_at"),
)

# Verificar resultado
print("Schema achatado (flat):")
df_parceiro_b.printSchema()
print()

# Visualizar primeiros registros
print("Primeiras linhas do Parceiro B (JSON achatado):")
df_parceiro_b.select(
    "order_id", "customer_id", "total_amount", "payment_method", "shipping_city"
).show(5, truncate=30)
```

**Resultado esperado:**

```
Schema achatado (flat):
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: long (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- order_date: string (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- shipping_city: string (nullable = true)
 |-- shipping_state: string (nullable = true)
 |-- status: string (nullable = true)
 |-- partner_source: string (nullable = true)
 |-- api_exported_at: string (nullable = true)

Primeiras linhas do Parceiro B (JSON achatado):
+------------------------------+-----------+------------+--------------+------------------------------+
|order_id                      |customer_id|total_amount|payment_method|shipping_city                 |
+------------------------------+-----------+------------+--------------+------------------------------+
|a1b2c3d4-e5f6-7890-abcd-ef...|CUST_12345 |      1250.0|    credit_card|São Paulo                     |
|b2c3d4e5-f6a7-8901-bcde-f1...|CUST_00789 |       450.5|           pix|Rio de Janeiro                |
|c3d4e5f6-a7b8-9012-cdef-01...|CUST_34567 |       89.90|    debit_card|Belo Horizonte                |
|...                           |...        |         ...|           ...|...                           |
+------------------------------+-----------+------------+--------------+------------------------------+
```

**Explicação técnica:**

- **`col("venda.order_id")`** — sintaxe de ponto para acessar campos dentro de um struct. É a forma mais comum de "achatar" JSON no Spark
- **`.alias("order_id")`** — renomeia a coluna para remover o prefixo `venda.`. Sem alias, a coluna ficaria como `venda.order_id`
- **`api_exported_at`** — mantemos o timestamp de export da API como metadado. Ele indica quando o parceiro gerou o dump, não quando nós ingerimos
- **Alternativa com `venda.*`** — poderia usar `df_exploded.select("venda.*")` para extrair todos os campos do struct de uma vez. Porém, o select explícito dá mais controle sobre quais campos queremos e como renomear

> **💡 Dica de Carlos:** "Em produção com schemas complexos (10+ níveis de aninhamento), considere usar `select("struct.*")` para achatar rápido e depois renomear. Para schemas simples como esse, o select explícito é mais legível e documentável."

---

## Passo 5: Converter Timestamps e Tratar Tipos

**Descrição:** O Parceiro B usa formato ISO 8601 para datas (`2023-01-15`) — diferente do formato brasileiro do Parceiro A. A coluna `order_date` foi inferida como string. Vamos converter para `DateType`. Também vamos converter o `api_exported_at` (formato UTC com timezone) para timestamp local.

**Código:**

```python
from pyspark.sql.functions import to_date, to_timestamp, from_utc_timestamp

# Converter order_date de string ISO para Date
df_parceiro_b = df_parceiro_b.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

# Converter exported_at de UTC para timestamp local (São Paulo)
df_parceiro_b = df_parceiro_b.withColumn(
    "api_exported_at",
    to_timestamp(col("api_exported_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

# Verificar conversões
print("Schema após conversão de tipos:")
df_parceiro_b.select("order_date", "api_exported_at").printSchema()
print()

# Mostrar valores convertidos
print("Amostras de datas convertidas:")
df_parceiro_b.select("order_date", "api_exported_at") \
    .distinct() \
    .orderBy("order_date") \
    .show(5, truncate=False)

# Range de datas
from pyspark.sql.functions import min as spark_min, max as spark_max

print("Range de datas - Parceiro B:")
df_parceiro_b.select(
    spark_min("order_date").alias("primeira_data"),
    spark_max("order_date").alias("ultima_data")
).show()
```

**Resultado esperado:**

```
Schema após conversão de tipos:
root
 |-- order_date: date (nullable = true)
 |-- api_exported_at: timestamp (nullable = true)

Amostras de datas convertidas:
+----------+-------------------+
|order_date|api_exported_at    |
+----------+-------------------+
|2023-01-01|2023-12-15 10:30:00|
|2023-01-02|2023-12-15 10:30:00|
|2023-01-03|2023-12-15 10:30:00|
|2023-01-04|2023-12-15 10:30:00|
|2023-01-05|2023-12-15 10:30:00|
+----------+-------------------+

Range de datas - Parceiro B:
+-------------+-----------+
|primeira_data|ultima_data|
+-------------+-----------+
|   2023-01-01| 2023-12-31|
+-------------+-----------+
```

**Explicação técnica:**

- **`to_date(col, "yyyy-MM-dd")`** — o formato ISO é o padrão do Spark, então a conversão é direta
- **`to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss'Z'")`** — converte o timestamp UTC da API. O `'Z'` literal indica UTC
- **Timezone** — como configuramos `spark.sql.session.timeZone = "America/Sao_Paulo"` na SparkSession, os timestamps são exibidos no horário de São Paulo automaticamente
- **Comparação com Parceiro A** — o Parceiro A usava `dd/MM/yyyy` (brasileiro), o Parceiro B usa `yyyy-MM-dd` (ISO). Essa diferença é comum quando lidamos com múltiplas fontes

---

## Passo 6: Adicionar Metadados de Ingestão — Parceiro B

**Descrição:** Seguindo o mesmo padrão que usamos no Parceiro A, vamos adicionar as 3 colunas de metadados de ingestão. Isso garante rastreabilidade uniforme independente do formato de origem.

**Código:**

```python
# Adicionar metadados de ingestão (mesmo padrão do Parceiro A)
df_parceiro_b_bronze = df_parceiro_b \
    .withColumn("_source", lit("parceiro_b")) \
    .withColumn("_ingestion_ts", current_timestamp()) \
    .withColumn("_file_origin", input_file_name())

# Remover coluna intermediária api_exported_at (já temos _ingestion_ts)
# Mantemos como informação adicional do parceiro
print(f"📊 Total Parceiro B: {df_parceiro_b_bronze.count():,} registros")
print(f"📊 Colunas: {len(df_parceiro_b_bronze.columns)}")
print()

# Visualizar metadados
print("Metadados de ingestão - Parceiro B:")
df_parceiro_b_bronze.select(
    "order_id", "_source", "_ingestion_ts", "_file_origin"
).show(3, truncate=50)
```

**Resultado esperado:**

```
📊 Total Parceiro B: 30,000 registros
📊 Colunas: 16

Metadados de ingestão - Parceiro B:
+------------------------------------+----------+--------------------+--------------------------------------------------+
|order_id                            |_source   |_ingestion_ts       |_file_origin                                      |
+------------------------------------+----------+--------------------+--------------------------------------------------+
|a1b2c3d4-e5f6-7890-abcd-ef123456...|parceiro_b|2024-01-15 10:35:...|file:///data/aula_03/parceiro_b/api_dump_page_001...|
|b2c3d4e5-f6a7-8901-bcde-f12345678..|parceiro_b|2024-01-15 10:35:...|file:///data/aula_03/parceiro_b/api_dump_page_001...|
|c3d4e5f6-a7b8-9012-cdef-012345678..|parceiro_b|2024-01-15 10:35:...|file:///data/aula_03/parceiro_b/api_dump_page_001...|
+------------------------------------+----------+--------------------+--------------------------------------------------+
```

**Explicação técnica:**

- **Padronização** — os 3 metadados (`_source`, `_ingestion_ts`, `_file_origin`) são idênticos em formato aos do Parceiro A. Isso permite unificar depois na camada Silver
- **`_file_origin`** — no caso do JSON, aponta para o arquivo `.json` de onde veio o registro. Note que após o `explode()`, todos os registros de uma mesma página apontam para o mesmo arquivo
- **`api_exported_at`** — mantivemos essa coluna extra que é específica do Parceiro B. Na Silver, podemos decidir se mantemos ou descartamos
- **16 colunas** — 12 de negócio + 1 metadado do parceiro (`api_exported_at`) + 3 metadados de ingestão

> **💡 Dica de Carlos:** "Cada parceiro pode ter seus metadados próprios — timestamps de export, versão da API, IDs de batch. Na Bronze, mantemos tudo. Na Silver, padronizamos e escolhemos o que faz sentido para o modelo unificado."

---

# PARTE B — Parceiro C (Parquet de Data Lake)

## Passo 7: Ler Parquet — O Formato Mais Simples

**Descrição:** O Parceiro C já possui um data lake maduro e nos envia dados em Parquet — o formato colunar padrão do ecossistema Big Data. A grande vantagem: o schema já vem embutido no arquivo. Não precisamos configurar encoding, separador, formato de data... O Spark simplesmente lê e já entende tudo. É a experiência mais "plug and play" que existe.

**Código:**

```python
# Ler Parquet — nenhuma configuração extra necessária!
df_parceiro_c = spark.read.parquet(
    "data/aula_03/parceiro_c/"
)

# Verificar resultado
print(f"📊 Total Parceiro C: {df_parceiro_c.count():,} registros")
print(f"📊 Colunas: {len(df_parceiro_c.columns)}")
print()

# Schema — já vem tipado corretamente!
print("Schema do Parquet (tipos já definidos):")
df_parceiro_c.printSchema()
```

**Resultado esperado:**

```
📊 Total Parceiro C: 80,000 registros
📊 Colunas: 12

Schema do Parquet (tipos já definidos):
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: long (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- order_date: string (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- shipping_city: string (nullable = true)
 |-- shipping_state: string (nullable = true)
 |-- status: string (nullable = true)
 |-- partner_source: string (nullable = true)
```

**Explicação técnica:**

- **Zero configuração** — nenhum `encoding`, `sep`, `header`, `inferSchema`. O Parquet é auto-descritivo: schema, tipos e metadados estão embutidos no arquivo
- **Tipos corretos** — `quantity` como `long`, `unit_price`/`total_amount` como `double`. Não precisa de `inferSchema` porque o schema faz parte do formato
- **Compressão transparente** — Parquet usa compressão interna (Snappy por padrão). O Spark descomprime automaticamente — você nem percebe
- **Leitura colunar** — se você fizer `select("order_id", "total_amount")`, o Spark lê apenas essas 2 colunas do disco, ignorando as outras 10. Isso é impossível com CSV/JSON

> **💡 Dica de Carlos:** "Parquet é o 'formato ideal' para data lakes. Auto-descritivo, comprimido, colunar. Se todos os parceiros enviassem Parquet, meu trabalho seria 10x mais fácil. Na prática, só parceiros maduros tecnologicamente usam Parquet — os outros precisam ser 'educados' a migrar."

---

## Passo 8: Inspecionar Metadados do Parquet (Compressão e Estatísticas)

**Descrição:** Uma das vantagens do Parquet é que podemos inspecionar metadados do arquivo sem ler todos os dados. Vamos verificar o codec de compressão usado e explorar como o Spark aproveita as estatísticas embutidas para otimizar queries.

**Código:**

```python
# Verificar metadados do arquivo Parquet via Spark
# O Spark expõe informações de compressão no plano de execução
print("📋 Plano de leitura do Parquet (observe o formato):")
df_parceiro_c.explain(True)
```

**Resultado esperado (resumido):**

```
== Physical Plan ==
*(1) ColumnarToRow
+- FileScan parquet [order_id#..., customer_id#..., ...]
   Batched: true
   DataFilters: []
   Format: Parquet
   Location: InMemoryFileIndex[file:///data/aula_03/parceiro_c]
   ReadSchema: struct<order_id:string,customer_id:string,...>
```

**Código (demonstrar leitura colunar eficiente):**

```python
# Demonstração: leitura seletiva de colunas (predicate pushdown)
print("📋 Plano com seleção de colunas (apenas 2 colunas lidas do disco):")
df_parceiro_c.select("order_id", "total_amount") \
    .filter(col("total_amount") > 1000) \
    .explain()
```

**Resultado esperado (resumido):**

```
== Physical Plan ==
*(1) Filter (isnotnull(total_amount) AND (total_amount > 1000.0))
+- *(1) ColumnarToRow
   +- FileScan parquet [order_id#...,total_amount#...]
      PushedFilters: [IsNotNull(total_amount), GreaterThan(total_amount,1000.0)]
      ReadSchema: struct<order_id:string,total_amount:double>
```

**Explicação técnica:**

- **`Format: Parquet`** — confirma que o Spark reconheceu o formato
- **`ReadSchema`** — mostra quais colunas serão efetivamente lidas. Com `select("order_id", "total_amount")`, apenas essas 2 colunas são lidas do disco
- **`PushedFilters`** — o filtro `total_amount > 1000` é "empurrado" para a camada de leitura. O Spark usa as estatísticas min/max do Parquet para pular blocos inteiros que não atendem ao filtro
- **Compressão Snappy** — é o padrão do Spark para Parquet. Oferece boa compressão (~4x) com descompressão muito rápida. Alternativas: gzip (melhor compressão, mais lento), zstd (melhor dos dois mundos)

> **💡 Dica de Carlos:** "O Parquet é o único formato que permite 'predicate pushdown' — empurrar filtros para a leitura. Com CSV ou JSON, o Spark precisa ler TUDO para depois filtrar. Com Parquet, ele pode pular blocos inteiros. Em datasets de TB, isso é a diferença entre minutos e horas."

---

## Passo 9: Tratar Colunas Extras e Adicionar Metadados — Parceiro C

**Descrição:** O Parceiro C possui a coluna `partner_source` que indica de onde ele recebeu os dados originalmente. É uma coluna extra que os outros parceiros não têm. Na Bronze, mantemos tudo — a decisão de remover ou não fica para a Silver. Vamos também converter `order_date` para tipo Date e adicionar metadados de ingestão.

**Código:**

```python
# Verificar coluna extra do Parceiro C
print("📊 Valores únicos de 'partner_source' (coluna extra do Parceiro C):")
df_parceiro_c.groupBy("partner_source").count().show()

# Converter order_date para tipo Date (vem como string ISO do Parquet gerado por pandas)
df_parceiro_c = df_parceiro_c.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

# Adicionar metadados de ingestão (mesmo padrão dos outros parceiros)
df_parceiro_c_bronze = df_parceiro_c \
    .withColumn("_source", lit("parceiro_c")) \
    .withColumn("_ingestion_ts", current_timestamp()) \
    .withColumn("_file_origin", input_file_name())

# Resultado final
print(f"\n📊 Total Parceiro C (Bronze): {df_parceiro_c_bronze.count():,} registros")
print(f"📊 Colunas: {len(df_parceiro_c_bronze.columns)}")
print()

# Visualizar resultado com metadados
print("Primeiras linhas - Parceiro C com metadados:")
df_parceiro_c_bronze.select(
    "order_id", "total_amount", "partner_source", "_source", "_ingestion_ts"
).show(5, truncate=30)
```

**Resultado esperado:**

```
📊 Valores únicos de 'partner_source' (coluna extra do Parceiro C):
+--------------+-----+
|partner_source|count|
+--------------+-----+
|    parceiro_a|~32000|
|    parceiro_b|~28000|
|    parceiro_c|~20000|
+--------------+-----+

📊 Total Parceiro C (Bronze): 80,000 registros
📊 Colunas: 15

Primeiras linhas - Parceiro C com metadados:
+------------------------------+------------+--------------+----------+--------------------+
|order_id                      |total_amount|partner_source|_source   |_ingestion_ts       |
+------------------------------+------------+--------------+----------+--------------------+
|a1b2c3d4-e5f6-7890-abcd-ef...|      1250.0|    parceiro_a |parceiro_c|2024-01-15 10:40:...|
|b2c3d4e5-f6a7-8901-bcde-f1...|       450.5|    parceiro_b |parceiro_c|2024-01-15 10:40:...|
|...                           |         ...|           ...|       ...|                 ...|
+------------------------------+------------+--------------+----------+--------------------+
```

**Explicação técnica:**

- **`partner_source`** — coluna extra que o Parceiro C tem mas os outros não. Indica a fonte original do dado dentro do ecossistema do parceiro. Mantemos na Bronze sem alterar
- **Diferença `partner_source` vs `_source`** — `partner_source` é um campo de negócio do parceiro (de onde ELE recebeu os dados). `_source` é nosso metadado de ingestão (de QUEM nós recebemos). São informações diferentes!
- **15 colunas** — 12 de negócio + 3 metadados de ingestão. O Parceiro C não tem metadados extras (como o `api_exported_at` do B)
- **Schema do Parquet** — como o arquivo foi gerado por pandas sem conversão de `order_date` para datetime, ele vem como string. Mesmo Parquet precisa de ajustes quando o produtor não tipou corretamente

> **💡 Dica de Carlos:** "Nem todo Parquet é perfeito. Se quem gera o arquivo usa pandas sem cuidado com tipos, datas podem vir como string e inteiros como float. O schema do Parquet garante consistência DENTRO do arquivo, mas não garante que os tipos sejam os ideais para o seu uso."

---

# COMPARAÇÃO — Schemas dos 3 Parceiros

## Passo 10: Comparar Schemas Side by Side

**Descrição:** Agora que ingerimos os 3 parceiros, vamos comparar seus schemas lado a lado. Essa análise é fundamental para planejar a normalização na camada Silver. Cada parceiro tem suas particularidades: nomes de colunas diferentes, tipos distintos, colunas extras...

**Código:**

```python
print("=" * 70)
print("📋 COMPARAÇÃO DE SCHEMAS — 3 PARCEIROS")
print("=" * 70)

# Schema Parceiro A (CSV legado)
print("\n🅰️  PARCEIRO A (CSV legado — ISO-8859-1, sep=';')")
print("-" * 50)
print(f"   Registros: {df_parceiro_a_bronze.count():,}")
print(f"   Colunas totais: {len(df_parceiro_a_bronze.columns)}")
print("   Colunas de negócio:")
cols_a = [c for c in df_parceiro_a_bronze.columns if not c.startswith("_")]
for c in cols_a:
    dtype = dict(df_parceiro_a_bronze.dtypes)[c]
    print(f"      • {c}: {dtype}")

# Schema Parceiro B (JSON API)
print(f"\n🅱️  PARCEIRO B (JSON multi-file — API REST)")
print("-" * 50)
print(f"   Registros: {df_parceiro_b_bronze.count():,}")
print(f"   Colunas totais: {len(df_parceiro_b_bronze.columns)}")
print("   Colunas de negócio:")
cols_b = [c for c in df_parceiro_b_bronze.columns if not c.startswith("_")]
for c in cols_b:
    dtype = dict(df_parceiro_b_bronze.dtypes)[c]
    print(f"      • {c}: {dtype}")

# Schema Parceiro C (Parquet)
print(f"\n🅾️  PARCEIRO C (Parquet — data lake)")
print("-" * 50)
print(f"   Registros: {df_parceiro_c_bronze.count():,}")
print(f"   Colunas totais: {len(df_parceiro_c_bronze.columns)}")
print("   Colunas de negócio:")
cols_c = [c for c in df_parceiro_c_bronze.columns if not c.startswith("_")]
for c in cols_c:
    dtype = dict(df_parceiro_c_bronze.dtypes)[c]
    print(f"      • {c}: {dtype}")
```

**Resultado esperado:**

```
======================================================================
📋 COMPARAÇÃO DE SCHEMAS — 3 PARCEIROS
======================================================================

🅰️  PARCEIRO A (CSV legado — ISO-8859-1, sep=';')
--------------------------------------------------
   Registros: ~50,000
   Colunas totais: 15
   Colunas de negócio:
      • cod_pedido: string
      • cod_cliente: string
      • cod_produto: string
      • qtd: int
      • preco_unit: double
      • valor_total: double
      • data_pedido: date
      • forma_pagamento: string
      • cidade_entrega: string
      • uf_entrega: string
      • situacao: string
      • origem: string

🅱️  PARCEIRO B (JSON multi-file — API REST)
--------------------------------------------------
   Registros: 30,000
   Colunas totais: 16
   Colunas de negócio:
      • order_id: string
      • customer_id: string
      • product_id: string
      • quantity: bigint
      • unit_price: double
      • total_amount: double
      • order_date: date
      • payment_method: string
      • shipping_city: string
      • shipping_state: string
      • status: string
      • partner_source: string
      • api_exported_at: timestamp

🅾️  PARCEIRO C (Parquet — data lake)
--------------------------------------------------
   Registros: 80,000
   Colunas totais: 15
   Colunas de negócio:
      • order_id: string
      • customer_id: string
      • product_id: string
      • quantity: bigint
      • unit_price: double
      • total_amount: double
      • order_date: date
      • payment_method: string
      • shipping_city: string
      • shipping_state: string
      • status: string
      • partner_source: string
```

**Explicação técnica:**

- **Nomes de colunas diferentes** — Parceiro A usa português (`cod_pedido`, `valor_total`), Parceiros B e C usam inglês (`order_id`, `total_amount`). Na Silver, padronizaremos para um schema único
- **Tipos numéricos** — Parceiro A tem `int` para quantidade (inferido do CSV), B e C têm `bigint` (tipo padrão do JSON/Parquet). Precisaremos unificar
- **Colunas extras** — Parceiro B tem `api_exported_at`, Parceiro C tem `partner_source`. Colunas exclusivas de um parceiro serão tratadas na Silver (nullable ou descartadas)
- **Volumes diferentes** — A tem ~50K, B tem 30K, C tem 80K. No total teremos ~160K registros na Bronze

---

## Passo 11: Resumo das Diferenças e Preview da Normalização

**Descrição:** Vamos criar uma tabela comparativa clara das diferenças entre os 3 parceiros. Isso serve como "especificação" para a normalização que faremos no próximo exercício (camada Silver).

**Código:**

```python
print("=" * 70)
print("📊 TABELA COMPARATIVA — DIFERENÇAS ENTRE PARCEIROS")
print("=" * 70)

comparacao = [
    ("Formato origem",    "CSV (;)",         "JSON (multiLine)",  "Parquet"),
    ("Encoding",          "ISO-8859-1",      "UTF-8",             "N/A (binário)"),
    ("Schema embutido?",  "❌ Não",           "❌ Não",            "✅ Sim"),
    ("Inferência tipos?", "⚠️ Necessária",   "⚠️ Necessária",    "✅ Automática"),
    ("Col. pedido",       "cod_pedido",      "order_id",          "order_id"),
    ("Col. cliente",      "cod_cliente",     "customer_id",       "customer_id"),
    ("Col. qtd",          "qtd (int)",       "quantity (bigint)", "quantity (bigint)"),
    ("Col. valor",        "valor_total",     "total_amount",      "total_amount"),
    ("Col. data",         "data_pedido",     "order_date",        "order_date"),
    ("Formato data",      "dd/MM/yyyy",      "yyyy-MM-dd",        "yyyy-MM-dd"),
    ("Col. cidade",       "cidade_entrega",  "shipping_city",     "shipping_city"),
    ("Col. estado",       "uf_entrega",      "shipping_state",    "shipping_state"),
    ("Cols. exclusivas",  "origem",          "api_exported_at",   "partner_source"),
    ("Registros",         "~50.000",         "~30.000",           "~80.000"),
    ("Nulls",             "'N/A' → null",    "Nulls nativos",     "Nulls nativos"),
]

# Cabeçalho
print(f"\n{'Aspecto':<20} {'Parceiro A':<20} {'Parceiro B':<20} {'Parceiro C':<20}")
print("-" * 80)
for aspecto, a, b, c in comparacao:
    print(f"{aspecto:<20} {a:<20} {b:<20} {c:<20}")

print()
print("🔮 PREVIEW — Camada Silver (próximo exercício)")
print("-" * 70)
print("""
Na Silver, vamos:
1. Renomear colunas do Parceiro A para inglês (cod_pedido → order_id)
2. Unificar tipos (int → bigint para quantidades)
3. Garantir que todos têm as mesmas colunas (nullable para exclusivas)
4. Criar schema unificado com UNION de todos os parceiros
5. Adicionar coluna _source para identificar origem de cada registro

Schema Silver unificado (preview):
   • order_id: string
   • customer_id: string
   • product_id: string
   • quantity: bigint
   • unit_price: double
   • total_amount: double
   • order_date: date
   • payment_method: string
   • shipping_city: string
   • shipping_state: string
   • status: string
   • _source: string
   • _ingestion_ts: timestamp
""")
```

**Resultado esperado:**

```
======================================================================
📊 TABELA COMPARATIVA — DIFERENÇAS ENTRE PARCEIROS
======================================================================

Aspecto              Parceiro A           Parceiro B           Parceiro C          
--------------------------------------------------------------------------------
Formato origem       CSV (;)              JSON (multiLine)     Parquet             
Encoding             ISO-8859-1           UTF-8                N/A (binário)       
Schema embutido?     ❌ Não               ❌ Não               ✅ Sim              
Inferência tipos?    ⚠️ Necessária        ⚠️ Necessária        ✅ Automática       
Col. pedido          cod_pedido           order_id             order_id            
Col. cliente         cod_cliente          customer_id          customer_id         
Col. qtd             qtd (int)            quantity (bigint)    quantity (bigint)   
Col. valor           valor_total          total_amount         total_amount        
Col. data            data_pedido          order_date           order_date          
Formato data         dd/MM/yyyy           yyyy-MM-dd           yyyy-MM-dd          
Col. cidade          cidade_entrega       shipping_city        shipping_city       
Col. estado          uf_entrega           shipping_state       shipping_state      
Cols. exclusivas     origem               api_exported_at      partner_source      
Registros            ~50.000              ~30.000              ~80.000             
Nulls                'N/A' → null         Nulls nativos        Nulls nativos       

🔮 PREVIEW — Camada Silver (próximo exercício)
----------------------------------------------------------------------

Na Silver, vamos:
1. Renomear colunas do Parceiro A para inglês (cod_pedido → order_id)
2. Unificar tipos (int → bigint para quantidades)
3. Garantir que todos têm as mesmas colunas (nullable para exclusivas)
4. Criar schema unificado com UNION de todos os parceiros
5. Adicionar coluna _source para identificar origem de cada registro

Schema Silver unificado (preview):
   • order_id: string
   • customer_id: string
   • product_id: string
   • quantity: bigint
   • unit_price: double
   • total_amount: double
   • order_date: date
   • payment_method: string
   • shipping_city: string
   • shipping_state: string
   • status: string
   • _source: string
   • _ingestion_ts: timestamp
```

**Explicação técnica:**

- **Schema heterogêneo** — a realidade de todo data lake: cada fonte tem seu próprio formato, nomenclatura e convenções. O trabalho do engenheiro de dados é criar uma "camada de tradução"
- **Camada Bronze → Silver** — a Bronze mantém dados "como vieram" (com metadados). A Silver normaliza, limpa e unifica. Isso é o core da arquitetura Medallion
- **Union requer schemas iguais** — para fazer `unionByName()` dos 3 parceiros, precisamos garantir que as colunas tenham os mesmos nomes e tipos compatíveis
- **Colunas exclusivas → null** — se Parceiro A não tem `api_exported_at`, essa coluna ficará como null nos registros dele. Usamos `allowMissingColumns=True` no union

---

## Passo 12: Validação Final e Contagem Consolidada

**Descrição:** Para fechar este exercício, vamos fazer um resumo consolidado de toda a ingestão. Isso valida que os 3 parceiros foram lidos com sucesso e estão prontos para a camada Bronze.

**Código:**

```python
print("=" * 70)
print("📋 RESUMO CONSOLIDADO — INGESTÃO DOS 3 PARCEIROS")
print("=" * 70)

total_a = df_parceiro_a_bronze.count()
total_b = df_parceiro_b_bronze.count()
total_c = df_parceiro_c_bronze.count()
total_geral = total_a + total_b + total_c

print(f"""
   ┌─────────────────────────────────────────────────────────┐
   │  Parceiro A (CSV legado)     │  {total_a:>8,} registros  │
   │  Parceiro B (JSON API)       │  {total_b:>8,} registros  │
   │  Parceiro C (Parquet)        │  {total_c:>8,} registros  │
   ├─────────────────────────────────────────────────────────┤
   │  TOTAL PARA BRONZE           │ {total_geral:>9,} registros  │
   └─────────────────────────────────────────────────────────┘
""")

print("✅ Problemas tratados neste exercício:")
print("   • JSON multi-line com array aninhado → explode + flatten")
print("   • Timestamps UTC da API → convertidos para tipo timestamp")
print("   • Parquet com colunas extras → mantidas na Bronze")
print("   • Metadados de ingestão padronizados nos 3 parceiros")
print()
print("⏭️  Próximo exercício: gravar tudo na camada Bronze (Parquet")
print("   particionado) e iniciar a normalização na Silver!")
```

**Resultado esperado:**

```
======================================================================
📋 RESUMO CONSOLIDADO — INGESTÃO DOS 3 PARCEIROS
======================================================================

   ┌─────────────────────────────────────────────────────────┐
   │  Parceiro A (CSV legado)     │    50,000 registros  │
   │  Parceiro B (JSON API)       │    30,000 registros  │
   │  Parceiro C (Parquet)        │    80,000 registros  │
   ├─────────────────────────────────────────────────────────┤
   │  TOTAL PARA BRONZE           │   160,000 registros  │
   └─────────────────────────────────────────────────────────┘

✅ Problemas tratados neste exercício:
   • JSON multi-line com array aninhado → explode + flatten
   • Timestamps UTC da API → convertidos para tipo timestamp
   • Parquet com colunas extras → mantidas na Bronze
   • Metadados de ingestão padronizados nos 3 parceiros

⏭️  Próximo exercício: gravar tudo na camada Bronze (Parquet
   particionado) e iniciar a normalização na Silver!
```

---

## Resumo do Exercício

Neste exercício você aprendeu a ingerir dados de duas fontes modernas — JSON de API REST e Parquet de data lake — e a compará-los com a fonte legada do exercício anterior:

| Formato | Complexidade de Leitura | Vantagens | Desvantagens |
|---------|------------------------|-----------|--------------|
| CSV | 🔴 Alta (encoding, sep, tipos) | Universal, legível | Sem schema, lento, grande |
| JSON | 🟡 Média (aninhamento, arrays) | Flexível, hierárquico | Verboso, sem tipos nativos |
| Parquet | 🟢 Baixa (auto-descritivo) | Comprimido, colunar, rápido | Binário (não legível), ecossistema |

### Conceitos-chave

1. **`multiLine=True`** — obrigatório para JSON documento (não JSON Lines)
2. **`explode()`** — transforma arrays em linhas (UNNEST). Cuidado com explosão de cardinalidade
3. **`col("struct.field")`** — sintaxe de ponto para acessar campos aninhados em structs
4. **Parquet é auto-descritivo** — schema, tipos e compressão embutidos. Leitura otimizada
5. **Predicate pushdown** — Parquet permite que filtros sejam aplicados na leitura (pula blocos)
6. **Schemas heterogêneos** — na vida real, cada fonte tem nomenclatura e tipos próprios
7. **Bronze armazena "como veio"** — normalização acontece na Silver

### Tabela de Referência — Leitura JSON no Spark

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `multiLine` | `false` | Cada arquivo é um JSON completo? |
| `mode` | `PERMISSIVE` | Tratamento de registros malformados |
| `columnNameOfCorruptRecord` | `_corrupt_record` | Onde colocar linhas com erro |
| `dateFormat` | `yyyy-MM-dd` | Formato de datas |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Formato de timestamps |
| `allowUnquotedFieldNames` | `false` | Aceitar campos sem aspas? |
| `allowSingleQuotes` | `true` | Aceitar aspas simples? |

### Tabela de Referência — Leitura Parquet no Spark

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `mergeSchema` | `false` | Unificar schemas de múltiplos arquivos? |
| `recursiveFileLookup` | `false` | Buscar arquivos em subdiretórios? |
| `pathGlobFilter` | `null` | Filtro glob para selecionar arquivos |
| `modifiedBefore/After` | `null` | Filtrar por data de modificação |

### Funções-chave para JSON

| Função | Uso | Exemplo |
|--------|-----|---------|
| `explode(col)` | Array → linhas | `explode("data")` |
| `col("a.b")` | Acessar campo de struct | `col("venda.order_id")` |
| `size(col)` | Tamanho do array | `size("data")` |
| `array_contains(col, val)` | Verificar se array contém valor | `array_contains("tags", "vip")` |
| `get_json_object(col, path)` | Extrair de JSON string | `get_json_object(col, "$.name")` |
| `from_json(col, schema)` | Parse JSON string → struct | `from_json(col, schema)` |
| `to_json(col)` | Struct → JSON string | `to_json("dados")` |

> **Carlos:** "Excelente! Três parceiros ingeridos, cada um com seus desafios. O mais importante: todos agora seguem o mesmo padrão de metadados (`_source`, `_ingestion_ts`, `_file_origin`). No próximo exercício, vamos gravar tudo na Bronze e começar a construir a Silver — onde a mágica da unificação acontece. É quando 3 schemas diferentes viram 1 schema limpo e consistente."

---

## Próximo Exercício

➡️ **Exercício 3 — Camada Bronze: Persistência Raw** (`03_camada_bronze.md`): gravar os 3 DataFrames em Parquet particionado, implementando a camada Bronze da arquitetura Medallion
