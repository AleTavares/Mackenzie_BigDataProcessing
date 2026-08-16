# Exercício 1 — Joins com Múltiplas Fontes de Dados

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "A DataFlow cresceu 10x nos últimos meses e agora temos dados espalhados em vários sistemas. A Ana precisa de um relatório consolidado para a campanha de Black Friday: vendas cruzadas com dados de clientes e categorias de produtos. Parece simples, mas com 1 milhão de registros de vendas, 500 mil clientes e uma tabela de categorias em JSON, precisamos saber exatamente qual tipo de join usar em cada situação — e como otimizar para não travar o cluster."

## Objetivos

Ao final deste exercício, você será capaz de:

- Ler dados de múltiplas fontes (Parquet e JSON) em uma sessão Spark
- Executar inner join entre DataFrames grandes
- Usar left join para preservar todos os registros da tabela principal
- Aplicar broadcast join para tabelas pequenas (otimização crítica)
- Verificar o plano de execução com `explain()`
- Construir pipelines com joins encadeados
- Identificar registros órfãos com left anti join
- Cachear resultados para reutilização

## Pré-requisitos

- Ambiente Docker rodando (ver `00_setup.md` da Aula 1)
- Jupyter Notebook acessível em http://localhost:8888
- Datasets disponíveis:
  - `data/aula_02/vendas_2023_completo.parquet` (1M registros)
  - `data/aula_02/clientes.parquet` (500K registros)
  - `data/aula_02/categorias.json` (10 categorias)

## Duração Estimada

⏱️ ~30 minutos

---

## Passo 1: Setup — Criar SparkSession e Ler as 3 Fontes de Dados

**Descrição:** Antes de cruzar informações, precisamos criar a SparkSession e carregar os três datasets que compõem nosso pipeline: vendas (Parquet, 1M registros), clientes (Parquet, 500K registros) e categorias (JSON, 10 registros). Note que cada fonte tem formato e tamanho diferente — isso é realidade em qualquer empresa.

**Código:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, broadcast, explode

# Criar SparkSession
spark = SparkSession.builder \
    .appName("DataFlow-Aula02-Joins") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print("✅ SparkSession criada!")
print(f"   Versão: {spark.version}")
```

**Resultado esperado:**

```
✅ SparkSession criada!
   Versão: 3.5.x
```

**Código (leitura das fontes):**

```python
# 1. Vendas - Parquet com 1M registros
df_vendas = spark.read.parquet("data/aula_02/vendas_2023_completo.parquet")

# 2. Clientes - Parquet com 500K registros
df_clientes = spark.read.parquet("data/aula_02/clientes.parquet")

# 3. Categorias - JSON pequeno (tabela de referência)
df_categorias_raw = spark.read.json(
    "data/aula_02/categorias.json",
    multiLine=True
)

# Explodir o JSON aninhado em formato tabular
df_categorias = df_categorias_raw \
    .select(explode(col("categorias")).alias("cat")) \
    .select(
        col("cat.category_id"),
        col("cat.category_name")
    )

# Verificar contagens
print(f"📊 Vendas:     {df_vendas.count():>10,} registros")
print(f"📊 Clientes:   {df_clientes.count():>10,} registros")
print(f"📊 Categorias: {df_categorias.count():>10,} registros")
print()
print("Schemas carregados:")
print("\n--- Vendas (primeiras colunas-chave) ---")
df_vendas.select("order_id", "customer_id", "product_id", "total_amount").show(3)
print("\n--- Clientes ---")
df_clientes.select("customer_id", "customer_name", "segment", "state").show(3)
print("\n--- Categorias ---")
df_categorias.show()
```

**Resultado esperado:**

```
📊 Vendas:      1,000,000 registros
📊 Clientes:      500,000 registros
📊 Categorias:         10 registros

Schemas carregados:

--- Vendas (primeiras colunas-chave) ---
+--------------------+-----------+----------+------------+
|            order_id|customer_id|product_id|total_amount|
+--------------------+-----------+----------+------------+
|b2c3d4e5-f6a7-890...|  CUST_12345|  PROD_0456|      449.50|
|...                                                      |
+--------------------+-----------+----------+------------+

--- Clientes ---
+-----------+-----------------+-------+-----+
|customer_id|    customer_name|segment|state|
+-----------+-----------------+-------+-----+
|  CUST_00001|João da Silva    | Bronze|   SP|
|...                                        |
+-----------+-----------------+-------+-----+

--- Categorias ---
+-----------+------------------+
|category_id|     category_name|
+-----------+------------------+
|     CAT_01|       Eletrônicos|
|     CAT_02|              Moda|
|     CAT_03|   Casa e Decoração|
|     CAT_04|          Esportes|
|     CAT_05|            Livros|
|     CAT_06|    Saúde e Beleza|
|     CAT_07|         Alimentos|
|     CAT_08|        Brinquedos|
|     CAT_09|        Automotivo|
|     CAT_10|       Informática|
+-----------+------------------+
```

**Explicação técnica:**

- **Parquet** é um formato colunar otimizado — ideal para leitura analítica de grandes volumes. O Spark lê apenas as colunas necessárias (predicate pushdown)
- **JSON com `multiLine=True`** — necessário quando o JSON é um objeto único com indentação (não JSON Lines)
- **`explode()`** — transforma um array JSON em múltiplas linhas. O campo `categorias` é um array de objetos, e precisamos "achatar" para formato tabular
- **`spark.sql.shuffle.partitions = 8`** — configuramos para 8 partições (padrão é 200, excessivo para ambiente local). Em produção, use valores maiores proporcionais ao cluster
- Note a diferença de escala: 1M vendas, 500K clientes, 10 categorias. Essa assimetria vai influenciar nossa estratégia de join

> **💡 Dica de Carlos:** "Sempre comece verificando as contagens. Se o parquet deveria ter 1M e tem 999K, algo deu errado na geração. Validar contagens no início evita debugging doloroso depois de montar todo o pipeline."

---

## Passo 2: Inner Join — Vendas + Clientes

**Descrição:** O inner join é o tipo mais restritivo: retorna apenas registros que existem em **ambas** as tabelas. Vamos cruzar vendas com clientes usando `customer_id` como chave. Se alguma venda referenciar um `customer_id` que não existe na tabela de clientes, essa venda será excluída do resultado.

**Código:**

```python
# Inner join: apenas vendas que têm cliente correspondente
df_inner = df_vendas.join(df_clientes, on="customer_id", how="inner")

# Contagens para comparação
count_vendas = df_vendas.count()
count_inner = df_inner.count()
perdas = count_vendas - count_inner

print(f"📊 Vendas originais:     {count_vendas:>10,}")
print(f"📊 Após inner join:      {count_inner:>10,}")
print(f"📊 Registros perdidos:   {perdas:>10,}")
print(f"📊 % de perda:           {(perdas/count_vendas)*100:.2f}%")
print()

# Visualizar resultado enriquecido
df_inner.select(
    "order_id", "customer_id", "customer_name", 
    "segment", "total_amount", "shipping_state"
).show(5, truncate=False)
```

**Resultado esperado:**

```
📊 Vendas originais:      1,000,000
📊 Após inner join:         ~950,000
📊 Registros perdidos:       ~50,000
📊 % de perda:              ~5.00%

+------------------------------------+-----------+------------------+-------+------------+--------------+
|order_id                            |customer_id|customer_name     |segment|total_amount|shipping_state|
+------------------------------------+-----------+------------------+-------+------------+--------------+
|b2c3d4e5-f6a7-8901-bcde-f12345678901|CUST_12345 |Maria Oliveira    |Prata  |449.50      |SP            |
|...                                                                                                    |
+------------------------------------+-----------+------------------+-------+------------+--------------+
```

**Explicação técnica:**

- `on="customer_id"` — quando a coluna de join tem o **mesmo nome** nas duas tabelas, basta passar o nome. O Spark é esperto o suficiente para não duplicar a coluna no resultado
- `how="inner"` — é o padrão do Spark (pode omitir), mas é boa prática ser explícito
- **Por que perdemos registros?** — A tabela de vendas usa `customer_id` de CUST_00001 a CUST_50000 (50K clientes distintos possíveis), mas a tabela de clientes vai de CUST_00001 a CUST_500000. Nem todos os customer_ids das vendas existem necessariamente na tabela de clientes (depende da geração)
- **Performance:** inner join de 1M × 500K registros envolve shuffle de dados entre partições. O Spark distribui os dados pela chave de join entre os workers

> **💡 Dica de Carlos:** "Inner join é seguro quando você quer apenas dados consistentes. Mas cuidado: se a perda for inesperadamente alta (>10%), pode indicar problema de qualidade nos dados — IDs incorretos, encoding errado, espaços extras. Sempre valide a contagem pós-join!"

---

## Passo 3: Left Join — Vendas + Clientes (Preservar Todas as Vendas)

**Descrição:** A Ana pediu: "Não quero perder nenhuma venda no relatório, mesmo que não tenhamos cadastro completo do cliente." O left join preserva **todos** os registros da tabela da esquerda (vendas), preenchendo com `null` quando não há correspondência na tabela da direita (clientes).

**Código:**

```python
# Left join: manter TODAS as vendas, enriquecer com dados de cliente quando possível
df_left = df_vendas.join(df_clientes, on="customer_id", how="left")

# Contagens
count_left = df_left.count()
count_com_cliente = df_left.filter(col("customer_name").isNotNull()).count()
count_sem_cliente = df_left.filter(col("customer_name").isNull()).count()

print(f"📊 Resultado left join:    {count_left:>10,}")
print(f"📊 Com dados de cliente:   {count_com_cliente:>10,}")
print(f"📊 Sem dados de cliente:   {count_sem_cliente:>10,}")
print(f"📊 Preservou 100%?         {count_left == count_vendas} ✅")
print()

# Mostrar exemplos de vendas SEM cliente (nulls nas colunas de clientes)
print("Exemplos de vendas sem cadastro de cliente:")
df_left.filter(col("customer_name").isNull()) \
    .select("order_id", "customer_id", "customer_name", "segment", "total_amount") \
    .show(5, truncate=False)
```

**Resultado esperado:**

```
📊 Resultado left join:     1,000,000
📊 Com dados de cliente:      ~950,000
📊 Sem dados de cliente:       ~50,000
📊 Preservou 100%?            True ✅

Exemplos de vendas sem cadastro de cliente:
+------------------------------------+-----------+-------------+-------+------------+
|order_id                            |customer_id|customer_name|segment|total_amount|
+------------------------------------+-----------+-------------+-------+------------+
|a1b2c3d4-e5f6-7890-abcd-ef1234567890|CUST_49876 |null         |null   |329.90      |
|...                                                                                |
+------------------------------------+-----------+-------------+-------+------------+
```

**Explicação técnica:**

- `how="left"` — preserva todos os registros da tabela **da esquerda** (vendas)
- Colunas da tabela da direita (clientes) ficam com `null` quando não há match
- **Diferença do inner join:** count do resultado = count da tabela da esquerda (sempre!)
- **Quando usar:** quando a tabela principal (fatos) não pode perder registros. Em data warehousing, é o padrão para tabelas de dimensão parcialmente preenchidas
- **Atenção:** após um left join, você pode ter `null` em colunas que normalmente não teriam — isso precisa ser tratado downstream (com `coalesce()`, `fillna()`, etc.)

> **💡 Dica de Ana (PO):** "Para relatórios de negócio, quase sempre queremos left join a partir da tabela de fatos. Perder vendas = subestimar faturamento. Melhor ter um campo 'cliente desconhecido' do que perder R$ 500K em vendas do relatório!"

---

## Passo 4: Broadcast Join — Vendas + Categorias (Tabela Pequena)

**Descrição:** A tabela de categorias tem apenas 10 registros — minúscula comparada com 1 milhão de vendas. Nesse cenário, o Spark pode usar um **broadcast join**: em vez de embaralhar (shuffle) ambas as tabelas pela rede, ele copia a tabela pequena para cada executor. Isso elimina o shuffle da tabela grande e é dramaticamente mais rápido.

Para fazer o join, precisamos derivar o `category_id` a partir do `product_id`. Na DataFlow, cada produto pertence a uma das 10 categorias (mapeamento via módulo do ID do produto).

**Código:**

```python
from pyspark.sql.functions import (
    broadcast, concat, lit, lpad,
    regexp_extract, ceil as spark_ceil
)

# Derivar category_id do product_id
# Lógica: PROD_0001 a PROD_0500 → CAT_01, PROD_0501 a PROD_1000 → CAT_02, etc.
# (5000 produtos / 10 categorias = 500 produtos por categoria)
df_vendas_cat = df_vendas.withColumn(
    "product_num",
    regexp_extract(col("product_id"), r"PROD_(\d+)", 1).cast("int")
).withColumn(
    "category_id",
    concat(
        lit("CAT_"),
        lpad(
            spark_ceil(col("product_num") / 500).cast("int").cast("string"),
            2, "0"
        )
    )
).drop("product_num")

# Broadcast join: enviar tabela pequena (categorias) para todos os executores
df_com_categoria = df_vendas_cat.join(
    broadcast(df_categorias),
    on="category_id",
    how="left"
)

# Verificar resultado
print(f"📊 Registros com categoria: {df_com_categoria.count():,}")
print()

# Distribuição por categoria
print("📊 Vendas por categoria:")
df_com_categoria.groupBy("category_name") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)
```

**Resultado esperado:**

```
📊 Registros com categoria: 1,000,000

📊 Vendas por categoria:
+------------------+------+
|     category_name| count|
+------------------+------+
|       Eletrônicos|~100000|
|              Moda|~100000|
|   Casa e Decoração|~100000|
|          Esportes|~100000|
|            Livros|~100000|
|    Saúde e Beleza|~100000|
|         Alimentos|~100000|
|        Brinquedos|~100000|
|        Automotivo|~100000|
|       Informática|~100000|
+------------------+------+
```

**Explicação técnica:**

- **`broadcast(df_categorias)`** — instrui o Spark a enviar a tabela inteira para todos os executores, evitando shuffle
- **Quando usar broadcast:** tabela pequena (regra geral: < 10MB). A tabela de categorias tem 10 linhas — caso perfeito!
- **Vantagem:** elimina o shuffle da tabela de 1M de registros. Em vez de redistribuir 1M de linhas pela rede, cada executor já tem a tabela pequena localmente
- **Derivação do `category_id`:** usamos `regexp_extract` para extrair o número do `product_id` e mapeamos para a categoria correspondente (500 produtos por categoria)
- **`spark_ceil(col("product_num") / 500)`** — arredonda para cima, criando grupos de 500 produtos por categoria
- O Spark pode fazer broadcast automaticamente para tabelas < 10MB (`spark.sql.autoBroadcastJoinThreshold`), mas é boa prática ser explícito

> **💡 Dica de Carlos:** "Broadcast join é sua arma secreta para tabelas de dimensão pequenas. Tabelas de estados (27 linhas), categorias (10-50 linhas), tipos de pagamento — sempre use `broadcast()` nesses casos. A diferença pode ser de minutos para segundos em datasets grandes."

---

## Passo 5: Verificar Plano de Execução — Confirmar Broadcast

**Descrição:** Como saber se o Spark realmente está usando broadcast? Usamos `explain()` para inspecionar o plano de execução. O plano mostra exatamente como o Spark pretende executar a query — quais joins usará, se haverá shuffle, quais filtros serão aplicados.

**Código:**

```python
# Recriar o join para análise do plano (com broadcast explícito)
df_broadcast_plan = df_vendas_cat.join(
    broadcast(df_categorias),
    on="category_id",
    how="left"
)

# Exibir plano de execução
print("=" * 60)
print("PLANO DE EXECUÇÃO — COM BROADCAST")
print("=" * 60)
df_broadcast_plan.explain(mode="simple")
```

**Resultado esperado:**

```
============================================================
PLANO DE EXECUÇÃO — COM BROADCAST
============================================================
== Physical Plan ==
*(2) Project [...]
+- *(2) BroadcastHashJoin [category_id#...], [category_id#...], LeftOuter, BuildRight
   :- *(2) Project [...]
   :  +- *(2) FileScan parquet [...]
   +- BroadcastExchange HashedRelationBroadcastMode(...)
      +- *(1) Project [...]
         +- *(1) FileScan json [...]
```

**Código (comparação SEM broadcast):**

```python
# Comparar: mesmo join SEM broadcast hint
df_sem_broadcast = df_vendas_cat.join(
    df_categorias,  # sem broadcast()
    on="category_id",
    how="left"
)

print("=" * 60)
print("PLANO DE EXECUÇÃO — SEM BROADCAST (para comparação)")
print("=" * 60)
df_sem_broadcast.explain(mode="simple")
```

**Resultado esperado:**

```
============================================================
PLANO DE EXECUÇÃO — SEM BROADCAST (para comparação)
============================================================
== Physical Plan ==
*(3) Project [...]
+- *(3) BroadcastHashJoin [...]   ← Spark detectou automaticamente!
   ...
```

**Explicação técnica:**

- **`explain(mode="simple")`** — mostra o plano físico de execução (o que o Spark realmente vai fazer)
- **`BroadcastHashJoin`** — confirma que o Spark usa broadcast. A tabela menor é "transmitida" para todos os nós
- **`SortMergeJoin`** — seria o plano sem broadcast: ambas as tabelas são particionadas (shuffle) e ordenadas antes do join. Muito mais lento para tabelas assimétricas
- **`BroadcastExchange`** — o passo onde a tabela pequena é serializada e enviada a todos os executores
- **Auto-broadcast:** O Spark automaticamente faz broadcast de tabelas menores que `spark.sql.autoBroadcastJoinThreshold` (padrão: 10MB). Por isso mesmo sem `broadcast()` explícito, o Spark pode escolher BroadcastHashJoin
- **Modos de explain:** `simple`, `extended`, `codegen`, `cost`, `formatted` — `formatted` é o mais legível em produção

> **💡 Dica de Carlos:** "SEMPRE verifique o plano de execução quando uma query está lenta. Se você espera broadcast e vê `SortMergeJoin`, algo está errado — talvez a tabela 'pequena' cresceu além do threshold. Em produção, esse tipo de regressão silenciosa pode transformar 2 minutos em 2 horas."

---

## Passo 6: Pipeline Completo — Join de 3 Fontes Encadeado

**Descrição:** Agora vamos montar o pipeline completo que a Ana precisa: vendas → left join clientes → broadcast join categorias. O resultado será um DataFrame enriquecido com nome do cliente, segmento de fidelidade e nome da categoria — tudo em uma cadeia fluente.

**Código:**

```python
from pyspark.sql.functions import (
    regexp_extract, concat, lit, lpad,
    ceil as spark_ceil, coalesce, broadcast
)

# Pipeline completo: vendas → clientes → categorias
df_completo = df_vendas \
    .withColumn(
        "product_num",
        regexp_extract(col("product_id"), r"PROD_(\d+)", 1).cast("int")
    ) \
    .withColumn(
        "category_id",
        concat(
            lit("CAT_"),
            lpad(
                spark_ceil(col("product_num") / 500).cast("int").cast("string"),
                2, "0"
            )
        )
    ) \
    .drop("product_num") \
    .join(df_clientes, on="customer_id", how="left") \
    .join(broadcast(df_categorias), on="category_id", how="left") \
    .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("Cliente Não Cadastrado"))
    ) \
    .withColumn(
        "segment",
        coalesce(col("segment"), lit("Sem Segmento"))
    )

# Verificar resultado final
print(f"📊 Dataset completo: {df_completo.count():,} registros")
print(f"📊 Colunas: {len(df_completo.columns)}")
print()

# Visualizar amostra do resultado enriquecido
df_completo.select(
    "order_id", "customer_name", "segment",
    "category_name", "total_amount", "shipping_state"
).show(10, truncate=20)
```

**Resultado esperado:**

```
📊 Dataset completo: 1,000,000 registros
📊 Colunas: 20

+--------------------+--------------------+-------+------------------+------------+--------------+
|            order_id|       customer_name|segment|     category_name|total_amount|shipping_state|
+--------------------+--------------------+-------+------------------+------------+--------------+
|b2c3d4e5-f6a7-890...|    Maria Oliveira  |  Prata|       Eletrônicos|      449.50|            SP|
|a1b2c3d4-e5f6-789...|      João Santos   |  Bronze|            Moda|      129.90|            RJ|
|f7e8d9c0-b1a2-345...|Cliente Não Cadas...|Sem ...|          Esportes|      899.00|            MG|
|...                                                                                            |
+--------------------+--------------------+-------+------------------+------------+--------------+
only showing top 10 rows
```

**Explicação técnica:**

- **Encadeamento de joins:** cada `.join()` retorna um novo DataFrame, permitindo encadear múltiplos joins em sequência
- **`coalesce(col_a, col_b)`** — retorna o primeiro valor não-null entre os argumentos. Usamos para tratar os `null` do left join com valores default legíveis
- **Ordem dos joins importa?**
  - Logicamente: não (o resultado é o mesmo)
  - Performance: **sim!** Coloque joins com tabelas grandes primeiro (shuffle pesado) e broadcast por último (sem shuffle)
- **Pipeline imutável:** cada operação cria um novo DataFrame. Se algo der errado, `df_vendas` original permanece intacto
- **20 colunas** = 12 de vendas + 7 de clientes (sem duplicar customer_id) + 1 de categorias (category_name, sem duplicar category_id)

> **💡 Dica de Carlos:** "Em produção, esse tipo de pipeline de enriquecimento é chamado de 'denormalization' — pegamos dados normalizados (separados em tabelas) e juntamos tudo em uma visão ampla para análise. É exatamente o que fazemos na camada Gold do data lake."

---

## Passo 7: Left Anti Join — Encontrar Vendas Órfãs

**Descrição:** A equipe de qualidade de dados da DataFlow quer identificar vendas "órfãs" — registros de vendas que referenciam clientes que não existem na base cadastral. O **left anti join** retorna apenas os registros da esquerda que **NÃO** têm correspondência na direita. É o oposto do inner join.

**Código:**

```python
# Left anti join: vendas SEM cliente correspondente na base
df_orfas = df_vendas.join(df_clientes, on="customer_id", how="left_anti")

count_orfas = df_orfas.count()
valor_orfas = df_orfas.agg({"total_amount": "sum"}).collect()[0][0]

print(f"🔍 Vendas órfãs (sem cliente válido): {count_orfas:,}")
print(f"💰 Valor total das vendas órfãs: R$ {valor_orfas:,.2f}")
print(f"📊 % do total de vendas: {(count_orfas/count_vendas)*100:.2f}%")
print()

# Distribuição por estado das vendas órfãs
print("Vendas órfãs por estado:")
df_orfas.groupBy("shipping_state") \
    .agg(
        count("order_id").alias("qtd_vendas"),
    ) \
    .orderBy(col("qtd_vendas").desc()) \
    .show(5)
```

**Resultado esperado:**

```
🔍 Vendas órfãs (sem cliente válido): ~50,000
💰 Valor total das vendas órfãs: R$ ~XX,XXX,XXX.XX
📊 % do total de vendas: ~5.00%

Vendas órfãs por estado:
+--------------+----------+
|shipping_state|qtd_vendas|
+--------------+----------+
|            SP|    ~12500|
|            RJ|     ~7500|
|            MG|     ~5000|
|            BA|     ~3500|
|            RS|     ~3000|
+--------------+----------+
only showing top 5 rows
```

**Explicação técnica:**

- **`how="left_anti"`** — retorna apenas linhas da tabela esquerda que **NÃO** existem na tabela direita
- É o equivalente SQL de: `SELECT * FROM vendas WHERE customer_id NOT IN (SELECT customer_id FROM clientes)`
- Mas `left_anti` é **mais eficiente** que `NOT IN` — não precisa materializar a subconsulta inteira
- **Uso prático:** detecção de dados inconsistentes, validação de integridade referencial, limpeza de dados
- **Contrapartida:** `left_semi` join retorna linhas da esquerda que **TÊM** correspondência na direita (sem duplicar colunas da direita)
- Note que o resultado contém **apenas as colunas da tabela esquerda** (vendas) — nenhuma coluna de clientes é incluída

> **💡 Dica de Carlos:** "Left anti join é fundamental para pipelines de qualidade de dados. Na Aula 6 vamos construir um framework completo de validação, mas o anti join é a base para detectar 'registros órfãos' — um dos problemas mais comuns em data lakes corporativos."

---

## Passo 8: Cache do Resultado — Preparar para Próximos Exercícios

**Descrição:** O DataFrame completo (vendas + clientes + categorias) será reutilizado nos próximos exercícios de window functions e UDFs. Sem cache, cada vez que acessarmos `df_completo` o Spark reexecutaria todo o pipeline (leitura + joins). O `cache()` armazena o DataFrame na memória dos executores para acesso instantâneo.

**Código:**

```python
# Cachear o DataFrame completo para reutilização
df_completo.cache()

# Forçar materialização do cache (o cache é lazy — só executa com uma ação)
count_cached = df_completo.count()
print(f"✅ DataFrame cacheado com sucesso!")
print(f"   Registros em cache: {count_cached:,}")
print()

# Verificar o armazenamento no Spark UI
print("📋 Storage Info:")
print(f"   Nome: {df_completo.storageLevel}")
print(f"   Está cacheado? {df_completo.is_cached}")
print()

# Demonstrar o ganho: segunda execução é instantânea
import time

start = time.time()
df_completo.count()  # Primeira vez pode ser lenta (materialização)
elapsed_1 = time.time() - start

start = time.time()
df_completo.count()  # Segunda vez usa cache
elapsed_2 = time.time() - start

print(f"⏱️  Primeira execução (com cache miss): {elapsed_1:.2f}s")
print(f"⏱️  Segunda execução (cache hit):       {elapsed_2:.2f}s")
print(f"⚡ Speedup: {elapsed_1/elapsed_2:.1f}x mais rápido")
```

**Resultado esperado:**

```
✅ DataFrame cacheado com sucesso!
   Registros em cache: 1,000,000

📋 Storage Info:
   Nome: Disk Memory Deserialized 1x Replicated
   Está cacheado? True

⏱️  Primeira execução (com cache miss): ~15.00s
⏱️  Segunda execução (cache hit):       ~0.50s
⚡ Speedup: ~30.0x mais rápido
```

**Explicação técnica:**

- **`cache()`** = `persist(StorageLevel.MEMORY_AND_DISK)` — armazena na memória; se não couber, usa disco
- **Cache é lazy:** apenas marca o DataFrame para caching. A materialização acontece na próxima **ação** (count, show, etc.)
- **Quando cachear:**
  - DataFrame será reutilizado múltiplas vezes (nosso caso — exercícios seguintes)
  - Pipeline de transformação é caro (múltiplos joins, como o nosso)
  - O resultado cabe na memória dos executores
- **Quando NÃO cachear:**
  - DataFrame usado apenas uma vez
  - DataFrame muito grande (excede memória disponível → spill para disco → pior que recalcular)
  - Dados de origem mudam frequentemente
- **`unpersist()`** — libera o cache quando não for mais necessário (boa prática em notebooks longos)
- **Spark UI** (http://localhost:8080) → aba "Storage" mostra todos os DataFrames cacheados, tamanho em memória e partições

> **💡 Dica de Carlos:** "Cache é poderoso mas tem custo: consome memória dos executores. Em produção, cacheie apenas DataFrames que serão acessados 3+ vezes. Para o nosso lab, é perfeito — vamos usar esse DataFrame enriquecido nos exercícios de window functions e UDFs a seguir."

---

## Resumo do Exercício

Neste exercício você dominou as operações de join no Apache Spark, uma das habilidades mais críticas para engenheiros de dados:

| Tipo de Join | Uso | Registros no Resultado |
|--------------|-----|----------------------|
| `inner` | Apenas matches | ≤ tabela menor |
| `left` | Preservar tabela esquerda | = tabela esquerda |
| `broadcast` | Tabela pequena (otimização) | Depende do tipo de join |
| `left_anti` | Encontrar órfãos | Registros SEM match |

### Conceitos-chave

1. **Inner join** é restritivo — use quando dados inconsistentes devem ser excluídos
2. **Left join** preserva a tabela principal — padrão para enriquecimento de tabelas de fatos
3. **Broadcast join** elimina shuffle — use sempre para tabelas pequenas (< 10MB)
4. **`explain()`** revela o plano real — verifique se o Spark está fazendo o que você espera
5. **Left anti join** detecta registros órfãos — ferramenta essencial de qualidade de dados
6. **Pipeline encadeado** — joins podem ser compostos em cadeia para construir visões desnormalizadas
7. **Cache** acelera reutilização — armazena resultados intermediários na memória

### Tabela de Referência — Tipos de Join no Spark

| Join | SQL Equivalente | Descrição |
|------|----------------|-----------|
| `inner` | `INNER JOIN` | Apenas registros com match em ambas |
| `left` / `left_outer` | `LEFT OUTER JOIN` | Todos da esquerda + match da direita |
| `right` / `right_outer` | `RIGHT OUTER JOIN` | Todos da direita + match da esquerda |
| `full` / `full_outer` | `FULL OUTER JOIN` | Todos de ambas as tabelas |
| `cross` | `CROSS JOIN` | Produto cartesiano (cuidado: N×M linhas!) |
| `left_semi` | `WHERE EXISTS` | Esquerda que tem match (sem colunas da direita) |
| `left_anti` | `WHERE NOT EXISTS` | Esquerda que NÃO tem match |

> **Carlos:** "Muito bem! Agora você tem os dados de vendas, clientes e categorias todos unidos e cacheados na memória. No próximo exercício, vamos usar window functions para criar rankings, análises de tendência e métricas avançadas em cima desse dataset enriquecido. É aí que a análise fica realmente poderosa."

---

## Próximo Exercício

➡️ **Exercício 2 — Window Functions: Ranking e Tendências** (`02_window_functions.md`): row_number, rank, dense_rank, lag, lead, running totals
