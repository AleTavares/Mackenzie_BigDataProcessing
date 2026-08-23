# Exercício 1 — Ingestão de CSV Legado (Parceiro A)

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "A DataFlow fechou parceria com 3 novos fornecedores de dados. O primeiro — Parceiro A — usa um ERP de 2005 que exporta dados em CSV, mas com configurações que vão te surpreender: encoding ISO-8859-1, separador ponto-e-vírgula, datas no formato brasileiro... Enfim, tudo diferente do que o Spark espera por padrão. A Ana trouxe esse parceiro ontem e a Marina já quer os dados no nosso data lake hoje. Vou te guiar nessa ingestão — mas primeiro, quero que você veja o que acontece quando tentamos ler sem configurar nada."

## Objetivos

Ao final deste exercício, você será capaz de:

- Identificar problemas de encoding ao ler CSVs com caracteres especiais
- Configurar leitura de CSV com encoding ISO-8859-1 (Latin-1) no Spark
- Usar separador personalizado (`;`) em leituras CSV
- Tratar formato de data brasileiro (dd/MM/yyyy) com `to_date()`
- Substituir valores sentinela ("N/A") por nulls reais
- Adicionar metadados de ingestão (_source, _ingestion_ts, _file_origin)
- Diagnosticar problemas de ingestão antes de persistir dados

## Pré-requisitos

- Ambiente Docker rodando (ver `00_setup.md` da Aula 1)
- Jupyter Notebook acessível em http://localhost:8888
- Datasets disponíveis:
  - `data/aula_03/parceiro_a/vendas_legacy_01_2023.csv` (Janeiro)
  - `data/aula_03/parceiro_a/vendas_legacy_06_2023.csv` (Junho)
  - `data/aula_03/parceiro_a/vendas_legacy_12_2023.csv` (Dezembro)

## Duração Estimada

⏱️ ~20 minutos

---

## Passo 1: Setup — Criar SparkSession para Aula 03

**Descrição:** Vamos criar a SparkSession para esta aula. Note que usamos o nome `DataFlow-Aula03-Ingestao` para identificar facilmente no Spark UI. Também configuramos o timezone para São Paulo, já que vamos lidar com timestamps brasileiros.

**Código:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, input_file_name,
    to_date, when, count, sum as spark_sum
)

# Criar SparkSession
spark = SparkSession.builder \
    .appName("DataFlow-Aula03-Ingestao") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.session.timeZone", "America/Sao_Paulo") \
    .getOrCreate()

print("✅ SparkSession criada!")
print(f"   Versão: {spark.version}")
print(f"   App: {spark.sparkContext.appName}")
```

**Resultado esperado:**

```
✅ SparkSession criada!
   Versão: 3.5.x
   App: DataFlow-Aula03-Ingestao
```

**Explicação técnica:**

- **`spark.sql.session.timeZone`** — define o timezone padrão para operações com datas/timestamps. Importante quando dados vêm de fontes brasileiras
- O `appName` diferente por aula facilita identificar sessões no Spark UI (http://localhost:8080)

---

## Passo 2: Primeira Tentativa — Ler CSV com Configurações Padrão (VAI FALHAR!)

**Descrição:** Vamos tentar ler os arquivos do Parceiro A usando as configurações padrão do Spark. Isso é **proposital** — queremos ver o que acontece quando encoding e separador estão errados. Na vida real, essa é a primeira coisa que todo engenheiro de dados faz: tenta ler e vê o que quebra.

**Código:**

```python
# ❌ TENTATIVA 1: Leitura com configurações padrão (encoding UTF-8, separador vírgula)
df_errado = spark.read.csv(
    "data/aula_03/parceiro_a/*.csv",
    header=True,
    inferSchema=True
)

# Quantas colunas temos?
print(f"📊 Colunas detectadas: {len(df_errado.columns)}")
print(f"📊 Nomes das colunas: {df_errado.columns}")
print()

# Visualizar os dados
print("Primeiras linhas (observe os problemas!):")
df_errado.show(5, truncate=False)
```

**Resultado esperado:**

```
📊 Colunas detectadas: 1
📊 Nomes das colunas: ['cod_pedido;cod_cliente;cod_produto;qtd;preco_unit;valor_total;data_pedido;forma_pagamento;cidade_entrega;uf_entrega;situacao;origem']

Primeiras linhas (observe os problemas!):
+----------------------------------------------------------------------------------------------------------+
|cod_pedido;cod_cliente;cod_produto;qtd;...                                                                |
+----------------------------------------------------------------------------------------------------------+
|a1b2c3d4-...;CUST_12345;PROD_0456;3;149.83;449.50;15/01/2023;crÃ©dito;SÃ£o Paulo;SP;entregue;parceiro_a |
|...                                                                                                       |
+----------------------------------------------------------------------------------------------------------+
```

**Explicação técnica:**

- **Problema 1 — Separador errado:** o Spark usa vírgula (`,`) como separador padrão. Como o arquivo usa ponto-e-vírgula (`;`), toda a linha é lida como uma única coluna gigante
- **Problema 2 — Encoding errado:** os caracteres acentuados aparecem corrompidos (`crÃ©dito` em vez de `crédito`, `SÃ£o Paulo` em vez de `São Paulo`). Isso acontece porque o Spark lê como UTF-8, mas o arquivo está em ISO-8859-1
- Esses dois problemas são os mais comuns ao lidar com dados de sistemas legados brasileiros

> **💡 Dica de Carlos:** "Quando você vê 'Ã©' onde deveria ter 'é', ou 'Ã£' onde deveria ter 'ã', é quase certeza de que o arquivo está em ISO-8859-1 (Latin-1) e está sendo lido como UTF-8. É o erro #1 de ingestão de dados no Brasil."

---

## Passo 3: Corrigir Encoding e Separador

**Descrição:** Agora que diagnosticamos os problemas, vamos corrigir: especificar encoding `ISO-8859-1` e separador `;`. Esses são os dois parâmetros mais importantes para ler CSVs de sistemas legados brasileiros.

**Código:**

```python
# ✅ TENTATIVA 2: Leitura com encoding e separador corretos
df_parceiro_a = spark.read.csv(
    "data/aula_03/parceiro_a/*.csv",
    header=True,
    sep=";",
    encoding="ISO-8859-1",
    inferSchema=True
)

# Verificar estrutura
print(f"📊 Colunas detectadas: {len(df_parceiro_a.columns)}")
print(f"📊 Total de registros: {df_parceiro_a.count():,}")
print()

# Verificar nomes das colunas
print("Colunas:")
for i, col_name in enumerate(df_parceiro_a.columns, 1):
    print(f"   {i:2d}. {col_name}")
print()

# Visualizar dados — agora com acentos corretos!
print("Primeiras linhas (encoding correto):")
df_parceiro_a.select(
    "cod_pedido", "cod_cliente", "forma_pagamento", "cidade_entrega"
).show(5, truncate=False)
```

**Resultado esperado:**

```
📊 Colunas detectadas: 12
📊 Total de registros: ~50,000

Colunas:
    1. cod_pedido
    2. cod_cliente
    3. cod_produto
    4. qtd
    5. preco_unit
    6. valor_total
    7. data_pedido
    8. forma_pagamento
    9. cidade_entrega
   10. uf_entrega
   11. situacao
   12. origem

Primeiras linhas (encoding correto):
+------------------------------------+-----------+---------------+------------------+
|cod_pedido                          |cod_cliente|forma_pagamento|cidade_entrega    |
+------------------------------------+-----------+---------------+------------------+
|a1b2c3d4-e5f6-7890-abcd-ef123456...|CUST_12345 |crédito        |São Paulo         |
|b2c3d4e5-f6a7-8901-bcde-f12345678..|CUST_00789 |pix            |Belo Horizonte    |
|c3d4e5f6-a7b8-9012-cdef-012345678..|CUST_34567 |boleto         |Florianópolis     |
|...                                                                               |
+------------------------------------+-----------+---------------+------------------+
```

**Explicação técnica:**

- **`encoding="ISO-8859-1"`** — corrige os caracteres acentuados. ISO-8859-1 (Latin-1) era o encoding padrão de sistemas brasileiros até ~2010
- **`sep=";"`** — define ponto-e-vírgula como separador. Muito comum em exports de ERPs brasileiros (SAP, TOTVS, etc.)
- **`inferSchema=True`** — o Spark tenta inferir tipos automaticamente (int, double, string). Veremos no próximo passo que nem sempre isso é suficiente
- **`header=True`** — usa a primeira linha como nomes de colunas
- **Wildcard `*.csv`** — lê todos os arquivos CSV do diretório de uma vez (3 arquivos mensais neste caso)

> **💡 Dica de Carlos:** "Sempre que receber dados de um parceiro novo, pergunte: (1) qual encoding? (2) qual separador? (3) qual formato de data? (4) como representam nulos? Essas 4 perguntas evitam 90% dos problemas de ingestão."

---

## Passo 4: Inspecionar Schema e Identificar Problemas de Tipos

**Descrição:** Mesmo com encoding e separador corretos, precisamos verificar se os tipos inferidos fazem sentido. Datas em formato brasileiro (`dd/MM/yyyy`) não são reconhecidas automaticamente pelo Spark — ele as trata como strings. Vamos diagnosticar isso.

**Código:**

```python
# Verificar schema inferido pelo Spark
print("Schema inferido:")
df_parceiro_a.printSchema()
```

**Resultado esperado:**

```
Schema inferido:
root
 |-- cod_pedido: string (nullable = true)
 |-- cod_cliente: string (nullable = true)
 |-- cod_produto: string (nullable = true)
 |-- qtd: integer (nullable = true)
 |-- preco_unit: double (nullable = true)
 |-- valor_total: double (nullable = true)
 |-- data_pedido: string (nullable = true)    ← ⚠️ Deveria ser date!
 |-- forma_pagamento: string (nullable = true)
 |-- cidade_entrega: string (nullable = true)
 |-- uf_entrega: string (nullable = true)
 |-- situacao: string (nullable = true)
 |-- origem: string (nullable = true)
```

**Código (investigar a coluna de data):**

```python
# Verificar formato da coluna data_pedido
print("Amostras de data_pedido (formato brasileiro dd/MM/yyyy):")
df_parceiro_a.select("data_pedido").distinct().show(10, truncate=False)
```

**Resultado esperado:**

```
Amostras de data_pedido (formato brasileiro dd/MM/yyyy):
+----------+
|data_pedido|
+----------+
|15/01/2023|
|03/06/2023|
|22/12/2023|
|N/A       |    ← ⚠️ Valor sentinela para nulo!
|08/01/2023|
|...       |
+----------+
```

**Explicação técnica:**

- **`data_pedido` como string** — o Spark não reconhece `dd/MM/yyyy` automaticamente. Ele espera formato ISO (`yyyy-MM-dd`). Precisaremos converter manualmente
- **"N/A" como texto** — o sistema legado usa `"N/A"` para representar dados ausentes, em vez de deixar o campo vazio. O Spark trata isso como texto válido, não como null
- **`qtd` como integer e `preco_unit`/`valor_total` como double** — essas inferências estão corretas!
- Esses problemas são muito comuns em dados de parceiros e precisam ser tratados antes de persistir no data lake

---

## Passo 5: Tratar Valores Nulos ("N/A" → null)

**Descrição:** O ERP legado do Parceiro A usa a string `"N/A"` para representar valores ausentes. Precisamos converter isso para `null` real do Spark, caso contrário nossas análises de completude e agregações ficarão incorretas. O Spark oferece o parâmetro `nullValue` na leitura, mas como já lemos os dados, vamos tratar via transformação.

**Código:**

```python
from pyspark.sql.functions import when, col, trim

# Opção 1: Reler com nullValue (mais elegante)
df_parceiro_a = spark.read.csv(
    "data/aula_03/parceiro_a/*.csv",
    header=True,
    sep=";",
    encoding="ISO-8859-1",
    inferSchema=True,
    nullValue="N/A"       # ← Trata "N/A" como null na leitura!
)

# Verificar: agora "N/A" virou null
print("Contagem de nulls por coluna:")
from pyspark.sql.functions import col, count, when, isnan

null_counts = df_parceiro_a.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_parceiro_a.columns
])
null_counts.show(vertical=True)
```

**Resultado esperado:**

```
Contagem de nulls por coluna:
-RECORD 0-------------------
 cod_pedido      | 0
 cod_cliente     | 0
 cod_produto     | 0
 qtd             | 0
 preco_unit      | 0
 valor_total     | 0
 data_pedido     | ~250
 forma_pagamento | ~150
 cidade_entrega  | ~200
 uf_entrega      | ~180
 situacao        | 0
 origem          | 0
```

**Explicação técnica:**

- **`nullValue="N/A"`** — instrui o Spark a tratar a string exata `"N/A"` como valor nulo durante a leitura. É o jeito mais limpo de resolver
- **Alternativa via transformação:** se você já carregou os dados, pode usar `when(col("campo") == "N/A", None).otherwise(col("campo"))` para cada coluna
- Agora os nulls aparecem corretamente — essencial para operações como `count()`, `avg()`, `coalesce()` e checks de qualidade
- Note que as colunas `cod_pedido`, `cod_cliente`, `qtd`, `valor_total` e `situacao` não têm nulls — são campos obrigatórios no ERP

> **💡 Dica de Carlos:** "Cada sistema tem sua própria representação de nulo: 'N/A', 'NULL', '-', '(vazio)', 'NULO', '0000-00-00'. Sempre pergunte ao parceiro e configure o `nullValue` adequado. Uma string 'N/A' contada como dado válido vai distorcer todas as suas métricas."

---

## Passo 6: Converter Data Brasileira (dd/MM/yyyy → Date)

**Descrição:** A coluna `data_pedido` ainda é uma string com formato `dd/MM/yyyy`. Precisamos convertê-la para um tipo `DateType` real do Spark para permitir filtros por período, particionamento por data e funções de janela temporal. Usamos `to_date()` com o padrão de formato correto.

**Código:**

```python
from pyspark.sql.functions import to_date

# Converter data_pedido de string "dd/MM/yyyy" para tipo Date
df_parceiro_a = df_parceiro_a.withColumn(
    "data_pedido",
    to_date(col("data_pedido"), "dd/MM/yyyy")
)

# Verificar a conversão
print("Schema após conversão de data:")
df_parceiro_a.select("data_pedido").printSchema()
print()

# Verificar valores convertidos
print("Amostras de data_pedido (agora como Date):")
df_parceiro_a.select("data_pedido") \
    .filter(col("data_pedido").isNotNull()) \
    .distinct() \
    .orderBy("data_pedido") \
    .show(10)

# Verificar range de datas
print("Range de datas no dataset:")
df_parceiro_a.select(
    spark_sum(col("data_pedido").isNull().cast("int")).alias("datas_nulas"),
    spark_sum(col("data_pedido").isNotNull().cast("int")).alias("datas_validas"),
).show()

from pyspark.sql.functions import min as spark_min, max as spark_max

df_parceiro_a.select(
    spark_min("data_pedido").alias("primeira_data"),
    spark_max("data_pedido").alias("ultima_data")
).show()
```

**Resultado esperado:**

```
Schema após conversão de data:
root
 |-- data_pedido: date (nullable = true)

Amostras de data_pedido (agora como Date):
+----------+
|data_pedido|
+----------+
|2023-01-01|
|2023-01-02|
|2023-01-03|
|2023-01-04|
|...       |
+----------+

Range de datas no dataset:
+-----------+-------------+
|datas_nulas|datas_validas|
+-----------+-------------+
|       ~250|      ~49,750|
+-----------+-------------+

+-------------+-----------+
|primeira_data|ultima_data|
+-------------+-----------+
|   2023-01-01| 2023-12-31|
+-------------+-----------+
```

**Explicação técnica:**

- **`to_date(col, "dd/MM/yyyy")`** — converte string para Date usando o padrão de formato especificado:
  - `dd` = dia com 2 dígitos (01-31)
  - `MM` = mês com 2 dígitos (01-12) — atenção: `MM` maiúsculo! (`mm` é minutos)
  - `yyyy` = ano com 4 dígitos
- **Strings inválidas → null** — se algum valor não bater com o padrão (inclusive os antigos "N/A" que agora são null), o resultado é null. Isso é seguro!
- **Por que converter?** — com tipo Date real, podemos:
  - Filtrar por range: `filter(col("data_pedido").between("2023-06-01", "2023-06-30"))`
  - Extrair componentes: `year()`, `month()`, `dayofweek()`
  - Particionar dados por data para otimizar leituras futuras
  - Usar em window functions com janelas temporais

> **💡 Dica de Carlos:** "O erro mais comum com `to_date()` é confundir `MM` (mês) com `mm` (minutos). Se suas datas estão todas virando null, verifique o padrão de formato! Outro clássico: confundir `dd/MM/yyyy` (brasileiro) com `MM/dd/yyyy` (americano)."

---

## Passo 7: Adicionar Metadados de Ingestão

**Descrição:** Em um data lake profissional, cada registro precisa carregar informação sobre sua origem: de onde veio, quando foi ingerido e de qual arquivo. Esses metadados são essenciais para rastreabilidade (lineage), debugging e reprocessamento. Vamos adicionar 3 colunas de metadados que seguem o padrão da DataFlow.

**Código:**

```python
from pyspark.sql.functions import lit, current_timestamp, input_file_name

# Adicionar metadados de ingestão (padrão DataFlow)
df_parceiro_a_bronze = df_parceiro_a \
    .withColumn("_source", lit("parceiro_a")) \
    .withColumn("_ingestion_ts", current_timestamp()) \
    .withColumn("_file_origin", input_file_name())

# Verificar as novas colunas
print("Schema com metadados de ingestão:")
df_parceiro_a_bronze.printSchema()
print()

# Visualizar metadados
print("Metadados de ingestão (últimas 3 colunas):")
df_parceiro_a_bronze.select(
    "cod_pedido", "_source", "_ingestion_ts", "_file_origin"
).show(5, truncate=50)
```

**Resultado esperado:**

```
Schema com metadados de ingestão:
root
 |-- cod_pedido: string (nullable = true)
 |-- cod_cliente: string (nullable = true)
 |-- cod_produto: string (nullable = true)
 |-- qtd: integer (nullable = true)
 |-- preco_unit: double (nullable = true)
 |-- valor_total: double (nullable = true)
 |-- data_pedido: date (nullable = true)
 |-- forma_pagamento: string (nullable = true)
 |-- cidade_entrega: string (nullable = true)
 |-- uf_entrega: string (nullable = true)
 |-- situacao: string (nullable = true)
 |-- origem: string (nullable = true)
 |-- _source: string (nullable = false)
 |-- _ingestion_ts: timestamp (nullable = false)
 |-- _file_origin: string (nullable = false)

Metadados de ingestão (últimas 3 colunas):
+------------------------------------+----------+--------------------+----------------------------------------------------+
|cod_pedido                          |_source   |_ingestion_ts       |_file_origin                                        |
+------------------------------------+----------+--------------------+----------------------------------------------------+
|a1b2c3d4-e5f6-7890-abcd-ef123456...|parceiro_a|2024-01-15 10:30:...|file:///data/aula_03/parceiro_a/vendas_legacy_01_... |
|b2c3d4e5-f6a7-8901-bcde-f12345678..|parceiro_a|2024-01-15 10:30:...|file:///data/aula_03/parceiro_a/vendas_legacy_01_... |
|...                                                                                                                     |
+------------------------------------+----------+--------------------+----------------------------------------------------+
```

**Explicação técnica:**

- **`_source`** — identifica qual parceiro/sistema originou o dado. Com `lit("parceiro_a")` adicionamos um valor fixo para todos os registros deste batch
- **`_ingestion_ts`** — timestamp exato do momento da ingestão. Essencial para saber quando o dado entrou no lake e para reprocessamento incremental
- **`_file_origin`** — caminho do arquivo de origem (via `input_file_name()`). Permite rastrear de qual export específico veio cada registro
- **Prefixo underscore (`_`)** — convenção para colunas de metadados técnicos. Diferencia das colunas de negócio (sem `_`)
- Esses metadados **nunca são alterados** depois de criados — são imutáveis na camada Bronze

> **💡 Dica de Carlos:** "Metadados de ingestão salvam vidas em produção. Quando a Ana perguntar 'por que essa venda de dezembro sumiu?', você pode rastrear: veio do arquivo X, foi ingerido no timestamp Y, do parceiro Z. Sem isso, debugging vira adivinhação."

---

## Passo 8: Validação Final e Preview da Camada Bronze

**Descrição:** Antes de persistir no data lake, vamos fazer uma validação rápida do DataFrame ingerido: contagens, distribuição por mês, check de qualidade básico. Isso é o que chamamos de "sanity check" — uma verificação rápida de que tudo faz sentido antes de gravar.

**Código:**

```python
from pyspark.sql.functions import month, year, count, round as spark_round

# Resumo geral
total = df_parceiro_a_bronze.count()
print(f"{'='*60}")
print(f"📋 RESUMO DA INGESTÃO — PARCEIRO A")
print(f"{'='*60}")
print(f"   Total de registros: {total:,}")
print(f"   Colunas de negócio: 12")
print(f"   Colunas de metadados: 3")
print(f"   Encoding original: ISO-8859-1")
print(f"   Separador original: ;")
print(f"   Formato de data: dd/MM/yyyy → DateType")
print(f"   Nulls tratados: 'N/A' → null")
print()

# Distribuição por mês (validar que os 3 arquivos foram lidos)
print("📊 Registros por mês de pedido:")
df_parceiro_a_bronze \
    .filter(col("data_pedido").isNotNull()) \
    .withColumn("mes", month("data_pedido")) \
    .groupBy("mes") \
    .count() \
    .orderBy("mes") \
    .show()

# Distribuição por forma de pagamento (validar encoding dos acentos)
print("📊 Distribuição por forma de pagamento:")
df_parceiro_a_bronze.groupBy("forma_pagamento") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# Distribuição por situação
print("📊 Distribuição por situação:")
df_parceiro_a_bronze.groupBy("situacao") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()
```

**Resultado esperado:**

```
============================================================
📋 RESUMO DA INGESTÃO — PARCEIRO A
============================================================
   Total de registros: ~50,000
   Colunas de negócio: 12
   Colunas de metadados: 3
   Encoding original: ISO-8859-1
   Separador original: ;
   Formato de data: dd/MM/yyyy → DateType
   Nulls tratados: 'N/A' → null

📊 Registros por mês de pedido:
+---+-----+
|mes|count|
+---+-----+
|  1|~16000|
|  6|~17000|
| 12|~17000|
+---+-----+

📊 Distribuição por forma de pagamento:
+---------------+-----+
|forma_pagamento|count|
+---------------+-----+
|    credit_card|~17500|
|           pix|~15000|
|    debit_card|~10000|
|        boleto| ~7500|
|          null|  ~150|
+---------------+-----+

📊 Distribuição por situação:
+---------+-----+
| situacao|count|
+---------+-----+
|delivered|~32500|
|  shipped| ~7500|
|  pending| ~5000|
|cancelled| ~5000|
+---------+-----+
```

**Código (preview da persistência Bronze):**

```python
# Preview: como esse DataFrame será salvo na camada Bronze
print("🔮 PREVIEW — Camada Bronze")
print("   Formato de saída: Parquet (compressão snappy)")
print("   Particionamento: por _source")
print("   Modo de escrita: append (incremental)")
print("   Destino: datalake/bronze/vendas/")
print()
print("   Comando de escrita (será executado no próximo exercício):")
print("""
   df_parceiro_a_bronze.write \\
       .mode("append") \\
       .partitionBy("_source") \\
       .parquet("datalake/bronze/vendas/")
""")
print()
print("✅ Parceiro A ingerido com sucesso!")
print("   Próximo passo: ingestão dos Parceiros B (JSON) e C (Parquet)")
```

**Resultado esperado:**

```
🔮 PREVIEW — Camada Bronze
   Formato de saída: Parquet (compressão snappy)
   Particionamento: por _source
   Modo de escrita: append (incremental)
   Destino: datalake/bronze/vendas/

   Comando de escrita (será executado no próximo exercício):

   df_parceiro_a_bronze.write \
       .mode("append") \
       .partitionBy("_source") \
       .parquet("datalake/bronze/vendas/")

✅ Parceiro A ingerido com sucesso!
   Próximo passo: ingestão dos Parceiros B (JSON) e C (Parquet)
```

**Explicação técnica:**

- **Validação por mês** — como temos 3 arquivos (Jan, Jun, Dez), esperamos registros apenas nesses meses. Se aparecessem outros meses, indicaria arquivo errado
- **Validação de encoding** — se `forma_pagamento` mostra "crédito" (e não "crÃ©dito"), confirmamos que o encoding está correto
- **Parquet como formato Bronze** — mesmo que a fonte seja CSV, persistimos como Parquet. Vantagens: compressão ~10x, schema embutido, leitura colunar
- **Particionamento por `_source`** — permite ler dados de um parceiro específico sem escanear todo o diretório
- **Modo `append`** — cada execução adiciona dados (não sobrescreve). Ideal para ingestão incremental diária
- A **camada Bronze armazena dados raw** — não fazemos transformações de negócio aqui, apenas limpeza técnica (encoding, tipos, nulls) e metadados

> **💡 Dica de Carlos:** "A regra de ouro da camada Bronze: o dado deve ser reversível. Se algo der errado na Silver ou Gold, você consegue reprocessar a partir do Bronze. Por isso mantemos todas as colunas originais e adicionamos metadados — nunca removemos dados na Bronze."

---

## Resumo do Exercício

Neste exercício você aprendeu a lidar com um cenário real de ingestão de dados legados — uma das situações mais comuns no dia a dia de um engenheiro de dados no Brasil:

| Problema | Sintoma | Solução |
|----------|---------|---------|
| Encoding errado | `crÃ©dito`, `SÃ£o Paulo` | `encoding="ISO-8859-1"` |
| Separador errado | Toda a linha em 1 coluna | `sep=";"` |
| Data em formato BR | String `15/01/2023` | `to_date(col, "dd/MM/yyyy")` |
| Null como texto | String `"N/A"` | `nullValue="N/A"` |
| Sem rastreabilidade | Não sabe a origem do dado | Colunas `_source`, `_ingestion_ts`, `_file_origin` |

### Conceitos-chave

1. **Sempre comece testando com configurações padrão** — o erro te ensina o que precisa ser ajustado
2. **Encoding ISO-8859-1** é o padrão de sistemas legados brasileiros (ERPs, sistemas bancários, governo)
3. **Separador `;`** é comum em CSVs exportados por Excel e ERPs brasileiros
4. **`nullValue`** na leitura é mais eficiente que tratar depois com `when().otherwise()`
5. **`to_date()` com formato explícito** — nunca confie na inferência automática para datas brasileiras
6. **Metadados de ingestão** são obrigatórios em data lakes profissionais (lineage, debugging, reprocessamento)
7. **Camada Bronze** armazena dados com mínima transformação — apenas o necessário para tornar os dados legíveis

### Tabela de Referência — Parâmetros de Leitura CSV no Spark

| Parâmetro | Default | Descrição |
|-----------|---------|-----------|
| `sep` | `,` | Caractere separador de campos |
| `encoding` | `UTF-8` | Encoding do arquivo |
| `header` | `false` | Primeira linha é cabeçalho? |
| `inferSchema` | `false` | Inferir tipos automaticamente? |
| `nullValue` | `""` | String que representa null |
| `dateFormat` | `yyyy-MM-dd` | Formato padrão de datas |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Formato de timestamps |
| `multiLine` | `false` | Campo pode ter quebra de linha? |
| `quote` | `"` | Caractere de aspas |
| `escape` | `\` | Caractere de escape |
| `mode` | `PERMISSIVE` | Tratamento de linhas mal formatadas |

> **Carlos:** "Excelente! Você acabou de ingerir o primeiro parceiro — o mais problemático dos três, aliás. No próximo exercício, vamos fazer o mesmo com o Parceiro B (JSON de API) e o Parceiro C (Parquet limpo). Depois, juntamos tudo na camada Bronze e começamos a normalizar na Silver. Cada parceiro tem seus desafios — mas o workflow é sempre o mesmo: ler, diagnosticar, corrigir, validar, persistir."

---

## Próximo Exercício

➡️ **Exercício 2 — Ingestão de JSON e Parquet (Parceiros B e C)** (`02_ingestao_json_parquet.md`): leitura de JSON multi-file com schema evolution e Parquet com metadados
