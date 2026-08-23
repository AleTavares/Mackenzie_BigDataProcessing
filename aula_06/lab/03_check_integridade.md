# Exercício 3 — Check de Integridade Referencial (left_anti join)

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Completude e unicidade estão ok. Mas a Ana me mostrou algo estranho: alguns relatórios de vendas por categoria estão com valores menores do que o esperado. Investigando, encontrei o problema — temos pedidos que referenciam `customer_id` e `product_id` que **não existem** nas tabelas de referência. São registros 'órfãos'. Quando fazemos join para gerar relatórios, esses registros caem fora silenciosamente. Vamos usar `left_anti join` para encontrar essas referências quebradas."

## Objetivos

Ao final deste exercício, você será capaz de:

- Carregar tabelas de referência (clientes e produtos)
- Entender o conceito de integridade referencial (FK → PK)
- Usar `left_anti join` para detectar registros órfãos
- Construir uma função reutilizável `check_referential_integrity(df_source, df_ref, source_col, ref_col) -> Dict`
- Quantificar órfãos: contagem e percentual
- Investigar padrões: de onde vêm os órfãos?
- Decidir ação: quarentena ou preenchimento com "unknown"
- Validar: após tratamento, 0 órfãos restantes

## Pré-requisitos

- Ambiente Docker rodando (Spark + Jupyter)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `dados_sujos/vendas_problemas.parquet` disponível na pasta `data/aula_06/`
- Tabelas de referência disponíveis:
  - `data/aula_06/dados_sujos/clientes_referencia.parquet` (~10K clientes)
  - `data/aula_06/dados_sujos/produtos_referencia.parquet` (~5K produtos)
- Exercícios 01 e 02 concluídos — `df_vendas` já carregado e deduplicado

## Duração Estimada

⏱️ ~20 minutos

---

## Passo 1: Garantir que o DataFrame Está Carregado e Deduplicado

**Descrição:** Se você está continuando dos exercícios anteriores, o `df_vendas` já está em memória e deduplicado. Caso contrário, vamos recarregá-lo e aplicar a deduplicação básica.

**Código:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, current_timestamp

# Criar/recuperar SparkSession
spark = SparkSession.builder \
    .appName("DataFlow-Aula06-QualityCheck") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Carregar dataset (se ainda não estiver carregado)
df_vendas = spark.read.parquet("data/aula_06/dados_sujos/vendas_problemas.parquet")

# Deduplicar (garantir 1 registro por order_id)
df_vendas = df_vendas.dropDuplicates(["order_id"])

total_registros = df_vendas.count()
print(f"✅ Dataset carregado e deduplicado: {total_registros:,} registros")
print(f"   Colunas: {df_vendas.columns}")
```

**Resultado esperado:**

```
✅ Dataset carregado e deduplicado: ~49,955 registros
   Colunas: ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'order_date', 'payment_method', 'shipping_city', 'shipping_state', 'status', 'partner_source']
```

---

## Passo 2: Carregar Tabelas de Referência

**Descrição:** Para verificar integridade referencial, precisamos das tabelas que contêm os IDs válidos. A DataFlow mantém tabelas-mestre de clientes e produtos que são a "fonte da verdade". Todo `customer_id` em `df_vendas` deveria existir em `clientes_referencia`, e todo `product_id` deveria existir em `produtos_referencia`.

**Código:**

```python
# Carregar tabela de referência de clientes
df_clientes = spark.read.parquet("data/aula_06/dados_sujos/clientes_referencia.parquet")

# Carregar tabela de referência de produtos
df_produtos = spark.read.parquet("data/aula_06/dados_sujos/produtos_referencia.parquet")

print(f"📊 Tabelas de referência carregadas:")
print(f"   🧑 Clientes: {df_clientes.count():,} registros")
print(f"   📦 Produtos: {df_produtos.count():,} registros")
print()

# Verificar schemas
print("   Schema clientes:")
df_clientes.printSchema()
print("   Schema produtos:")
df_produtos.printSchema()
```

**Resultado esperado:**

```
📊 Tabelas de referência carregadas:
   🧑 Clientes: ~10,000 registros
   📦 Produtos: ~5,000 registros

   Schema clientes:
   root
    |-- customer_id: string (nullable = true)
    |-- customer_name: string (nullable = true)
    |-- ...

   Schema produtos:
   root
    |-- product_id: string (nullable = true)
    |-- product_name: string (nullable = true)
    |-- ...
```

**Código (preview dos dados):**

```python
# Visualizar amostra das tabelas de referência
print("🧑 Amostra de clientes:")
df_clientes.show(3, truncate=False)

print("📦 Amostra de produtos:")
df_produtos.show(3, truncate=False)
```

**Explicação técnica:**

- **Tabela de referência** (ou tabela-mestre) contém a lista completa de entidades válidas
- Em bancos relacionais, seria a tabela referenciada por uma Foreign Key (FK)
- **Integridade referencial** = todo valor de FK no dataset filho existe na tabela-mãe
- Se um `customer_id` em vendas NÃO existe na tabela de clientes → registro órfão
- Causas comuns: migração parcial, sincronização entre sistemas, bugs de ingestão

> **💡 Dica de Carlos:** "Tabelas de referência devem ser a 'fonte da verdade'. Se o cliente existe na tabela-mestre, ele é válido. Se não existe, a venda referencia algo que não deveria existir — e isso precisa ser investigado."

---

## Passo 3: Entender o Conceito de `left_anti join`

**Descrição:** O `left_anti join` é uma das operações mais elegantes do Spark para encontrar registros que NÃO possuem correspondência em outra tabela. Diferente do `left join` (que traz tudo e coloca NULL onde não há match), o `left_anti` retorna apenas os registros da tabela da esquerda que NÃO aparecem na tabela da direita.

**Código:**

```python
# Conceito visual:
# 
# df_vendas (esquerda)     df_clientes (direita)
# ┌──────────────┐         ┌──────────────┐
# │ customer_id  │         │ customer_id  │
# ├──────────────┤         ├──────────────┤
# │ CUST-001  ✅ │  ←match→ │ CUST-001     │
# │ CUST-002  ✅ │  ←match→ │ CUST-002     │
# │ CUST-999  ❌ │  ←sem──→ │              │   ← ÓRFÃO!
# │ CUST-003  ✅ │  ←match→ │ CUST-003     │
# │ CUST-888  ❌ │  ←sem──→ │              │   ← ÓRFÃO!
# └──────────────┘         └──────────────┘
#
# left_anti join retorna: [CUST-999, CUST-888] — apenas os órfãos!

# Demonstração rápida com dados de exemplo
df_exemplo_vendas = spark.createDataFrame([
    ("ORD-1", "CUST-001"), ("ORD-2", "CUST-002"), 
    ("ORD-3", "CUST-999"), ("ORD-4", "CUST-888")
], ["order_id", "customer_id"])

df_exemplo_clientes = spark.createDataFrame([
    ("CUST-001",), ("CUST-002",), ("CUST-003",)
], ["customer_id"])

# left_anti: retorna registros de vendas SEM match em clientes
df_orfaos_exemplo = df_exemplo_vendas.join(
    df_exemplo_clientes,
    on="customer_id",
    how="left_anti"
)

print("🔍 Demonstração do left_anti join:")
print("   Vendas de exemplo:")
df_exemplo_vendas.show(truncate=False)
print("   Clientes válidos:")
df_exemplo_clientes.show(truncate=False)
print("   Resultado left_anti (órfãos):")
df_orfaos_exemplo.show(truncate=False)
```

**Resultado esperado:**

```
🔍 Demonstração do left_anti join:
   Vendas de exemplo:
+--------+-----------+
|order_id|customer_id|
+--------+-----------+
|ORD-1   |CUST-001   |
|ORD-2   |CUST-002   |
|ORD-3   |CUST-999   |
|ORD-4   |CUST-888   |
+--------+-----------+

   Clientes válidos:
+-----------+
|customer_id|
+-----------+
|CUST-001   |
|CUST-002   |
|CUST-003   |
+-----------+

   Resultado left_anti (órfãos):
+--------+-----------+
|order_id|customer_id|
+--------+-----------+
|ORD-3   |CUST-999   |
|ORD-4   |CUST-888   |
+--------+-----------+
```

**Explicação técnica:**

- **`left_anti`** = "me dê tudo da esquerda que NÃO existe na direita"
- Alternativas equivalentes (mais verbosas e menos eficientes):
  - `left join` + `filter(col("ref_col").isNull())` — funciona, mas carrega todas as colunas da direita para depois descartar
  - `WHERE NOT EXISTS (SELECT 1 FROM ref WHERE ...)` em SQL
- O Catalyst Optimizer do Spark otimiza `left_anti` para não materializar a tabela da direita inteira
- É mais eficiente que `left join` + `filter` porque o Spark sabe de antemão que não precisa das colunas da direita

> **💡 Dica de Carlos:** "O `left_anti` é meu join favorito para quality checks. É semântico — lê-se literalmente como 'me dê os registros que NÃO pertencem a essa referência'. Claro, direto, eficiente."

---

## Passo 4: Detectar Órfãos de customer_id

**Descrição:** Agora vamos aplicar o `left_anti join` nos dados reais. Queremos saber: quantas vendas referenciam um `customer_id` que NÃO existe na tabela de clientes? Esses são os registros "órfãos" — vendas atribuídas a clientes fantasmas.

**Código:**

```python
# Encontrar vendas com customer_id que NÃO existe na tabela de clientes
# Primeiro: filtrar apenas registros que TÊM customer_id (não-nulos)
df_vendas_com_customer = df_vendas.filter(col("customer_id").isNotNull())

# left_anti join: vendas SEM correspondência em clientes
df_orfaos_customer = df_vendas_com_customer.join(
    df_clientes,
    on="customer_id",
    how="left_anti"
)

total_com_customer = df_vendas_com_customer.count()
orfaos_customer = df_orfaos_customer.count()
pct_orfaos_customer = (orfaos_customer / total_com_customer) * 100

print(f"🔍 Integridade Referencial — customer_id")
print("=" * 55)
print(f"   Vendas com customer_id preenchido: {total_com_customer:,}")
print(f"   Vendas com customer_id VÁLIDO:     {total_com_customer - orfaos_customer:,}")
print(f"   Vendas com customer_id ÓRFÃO:      {orfaos_customer:,}")
print(f"   Percentual de órfãos:              {pct_orfaos_customer:.2f}%")
print(f"   Status:                            {'✅ PASS' if orfaos_customer == 0 else '❌ FAIL'}")
```

**Resultado esperado:**

```
🔍 Integridade Referencial — customer_id
=======================================================
   Vendas com customer_id preenchido: ~47,380
   Vendas com customer_id VÁLIDO:     ~46,190
   Vendas com customer_id ÓRFÃO:      ~1,190
   Percentual de órfãos:              ~2.51%
   Status:                            ❌ FAIL
```

**Código (visualizar amostra dos órfãos):**

```python
# Quais customer_ids são órfãos?
print(f"\n📋 Amostra de vendas com customer_id órfão:")
df_orfaos_customer.select(
    "order_id", "customer_id", "product_id", 
    "total_amount", "partner_source"
).show(10, truncate=False)
```

**Resultado esperado:**

```
📋 Amostra de vendas com customer_id órfão:
+------------------------------------+----------------+----------+------------+--------------+
|order_id                            |customer_id     |product_id|total_amount|partner_source|
+------------------------------------+----------------+----------+------------+--------------+
|ORD-2023-XXXXX                      |CUST-99001      |PROD-042  |449.97      |parceiro_b    |
|ORD-2023-YYYYY                      |CUST-99055      |PROD-015  |89.90       |parceiro_b    |
|ORD-2023-ZZZZZ                      |CUST-99102      |PROD-301  |1299.00     |parceiro_c    |
|...                                 |...             |...       |...         |...           |
+------------------------------------+----------------+----------+------------+--------------+
```

**Explicação técnica:**

- Filtramos `customer_id IS NOT NULL` primeiro porque nulos não são órfãos — são problemas de completude (já tratados no Ex.01)
- O `left_anti join` retorna TODAS as colunas do `df_vendas_com_customer` para registros sem match
- ~2.5% de órfãos significa que esses registros seriam **silenciosamente excluídos** em qualquer relatório que faça `inner join` com clientes
- É por isso que o faturamento por segmento de cliente estava menor que o esperado — as vendas de clientes órfãos simplesmente desapareciam!

> **💡 Dica de Carlos:** "É aqui que o problema da Ana se explica. Quando o relatório faz `df_vendas.join(df_clientes, 'customer_id')`, os órfãos caem fora silenciosamente. O relatório não dá erro — apenas mostra um número menor. Esse é o tipo de bug mais perigoso: o silencioso."

---

## Passo 5: Detectar Órfãos de product_id

**Descrição:** Vamos repetir o processo para `product_id`. Vendas que referenciam produtos inexistentes podem indicar que o catálogo de produtos está desatualizado ou que existe um bug na ingestão de dados de um parceiro.

**Código:**

```python
# Encontrar vendas com product_id que NÃO existe na tabela de produtos
df_vendas_com_product = df_vendas.filter(col("product_id").isNotNull())

# left_anti join: vendas SEM correspondência em produtos
df_orfaos_product = df_vendas_com_product.join(
    df_produtos,
    on="product_id",
    how="left_anti"
)

total_com_product = df_vendas_com_product.count()
orfaos_product = df_orfaos_product.count()
pct_orfaos_product = (orfaos_product / total_com_product) * 100

print(f"🔍 Integridade Referencial — product_id")
print("=" * 55)
print(f"   Vendas com product_id preenchido:  {total_com_product:,}")
print(f"   Vendas com product_id VÁLIDO:      {total_com_product - orfaos_product:,}")
print(f"   Vendas com product_id ÓRFÃO:       {orfaos_product:,}")
print(f"   Percentual de órfãos:              {pct_orfaos_product:.2f}%")
print(f"   Status:                            {'✅ PASS' if orfaos_product == 0 else '❌ FAIL'}")
```

**Resultado esperado:**

```
🔍 Integridade Referencial — product_id
=======================================================
   Vendas com product_id preenchido:  ~49,955
   Vendas com product_id VÁLIDO:      ~48,705
   Vendas com product_id ÓRFÃO:       ~1,250
   Percentual de órfãos:              ~2.50%
   Status:                            ❌ FAIL
```

**Código (quais product_ids são órfãos):**

```python
# Listar os product_ids órfãos (distintos)
product_ids_orfaos = df_orfaos_product.select("product_id").distinct()
print(f"\n📦 product_ids órfãos distintos: {product_ids_orfaos.count()}")
print("   Amostra:")
product_ids_orfaos.show(10, truncate=False)
```

**Resultado esperado:**

```
📦 product_ids órfãos distintos: ~250
   Amostra:
+----------+
|product_id|
+----------+
|PROD-9001 |
|PROD-9055 |
|PROD-9102 |
|...       |
+----------+
```

**Explicação técnica:**

- O mesmo padrão `left_anti join` funciona para qualquer par de colunas (source → reference)
- Se existem ~250 `product_id` distintos que são órfãos, é provável que sejam produtos descontinuados ou de um catálogo antigo
- Esses registros afetam relatórios de vendas por categoria/produto — uma venda de produto órfão não aparece em nenhuma categoria

> **💡 Dica de Ana (PO):** "Se esses product_ids são de produtos descontinuados, faz sentido — o catálogo foi atualizado mas as vendas históricas ainda referenciam os IDs antigos. Precisamos de uma tabela de 'de-para' ou pelo menos classificar como 'produto_descontinuado'."

---

## Passo 6: Construir a Função `check_referential_integrity()`

**Descrição:** Vamos encapsular a lógica do `left_anti join` em uma função reutilizável. Ela recebe o DataFrame de origem, o DataFrame de referência, a coluna de FK no source e a coluna de PK na referência. Retorna métricas detalhadas.

**Código:**

```python
from pyspark.sql import DataFrame
from typing import Dict

def check_referential_integrity(
    df_source: DataFrame,
    df_ref: DataFrame,
    source_col: str,
    ref_col: str
) -> Dict:
    """
    Verifica a integridade referencial entre dois DataFrames.
    
    Parâmetros:
        df_source: DataFrame com a coluna de foreign key (ex: vendas)
        df_ref: DataFrame de referência com a primary key (ex: clientes)
        source_col: Nome da coluna FK no df_source (ex: "customer_id")
        ref_col: Nome da coluna PK no df_ref (ex: "customer_id")
    
    Retorna:
        Dicionário com métricas de integridade referencial:
        - source_col: nome da coluna verificada no source
        - ref_col: nome da coluna verificada na referência
        - total_records: registros no source com a coluna preenchida (não-nula)
        - valid_references: registros com referência válida
        - orphan_records: registros sem correspondência na referência
        - orphan_pct: percentual de órfãos sobre o total
        - integrity_score: percentual de integridade (1.0 = perfeito)
        - orphan_distinct_keys: quantidade de chaves FK distintas que são órfãs
        - passed: True se não houver órfãos (integridade 100%)
    """
    # Filtrar apenas registros com a coluna preenchida (não-nula)
    df_filled = df_source.filter(col(source_col).isNotNull())
    total = df_filled.count()
    
    # left_anti join: encontrar registros SEM correspondência
    df_orphans = df_filled.join(
        df_ref,
        df_filled[source_col] == df_ref[ref_col],
        "left_anti"
    )
    
    orphan_count = df_orphans.count()
    valid_count = total - orphan_count
    
    # Chaves órfãs distintas
    orphan_distinct = df_orphans.select(source_col).distinct().count()
    
    orphan_pct = (orphan_count / total) * 100 if total > 0 else 0.0
    integrity_score = (valid_count / total) if total > 0 else 1.0
    
    return {
        "source_col": source_col,
        "ref_col": ref_col,
        "total_records": total,
        "valid_references": valid_count,
        "orphan_records": orphan_count,
        "orphan_pct": round(orphan_pct, 2),
        "integrity_score": round(integrity_score, 4),
        "orphan_distinct_keys": orphan_distinct,
        "passed": orphan_count == 0
    }

print("✅ Função check_referential_integrity() definida com sucesso!")
```

**Resultado esperado:**

```
✅ Função check_referential_integrity() definida com sucesso!
```

**Explicação técnica:**

- A função filtra nulos antes de fazer o join — nulos são problema de completude, não de integridade referencial
- Usamos `df_filled[source_col] == df_ref[ref_col]` para suportar nomes de colunas diferentes entre source e referência
- `orphan_distinct_keys` conta quantos IDs distintos são órfãos — útil para entender se são poucos IDs muito repetidos ou muitos IDs únicos
- `integrity_score` é o complemento: 1.0 = 100% íntegro, 0.97 = 97% íntegro (3% órfãos)
- O campo `passed` é rígido: qualquer órfão resulta em falha. Em produção, poderia aceitar um threshold

> **💡 Dica de Carlos:** "Note que usamos `df_filled[source_col] == df_ref[ref_col]` em vez de `on=source_col`. Isso permite que as colunas tenham nomes diferentes — ex: a venda pode ter `cod_cliente` e a referência pode ter `customer_id`. Flexibilidade é essencial em integrações multi-fonte."

---

## Passo 7: Executar o Check e Gerar Relatório Consolidado

**Descrição:** Vamos executar `check_referential_integrity()` para ambas as referências (clientes e produtos) e gerar um relatório consolidado no formato que a Marina precisa para o comitê de governança.

**Código:**

```python
# Executar check para customer_id
resultado_customer = check_referential_integrity(
    df_source=df_vendas,
    df_ref=df_clientes,
    source_col="customer_id",
    ref_col="customer_id"
)

# Executar check para product_id
resultado_product = check_referential_integrity(
    df_source=df_vendas,
    df_ref=df_produtos,
    source_col="product_id",
    ref_col="product_id"
)

# Relatório consolidado
print("📋 RELATÓRIO DE INTEGRIDADE REFERENCIAL — DataFlow Analytics")
print("=" * 70)
print(f"{'Referência':<18} {'Total':>8} {'Válidos':>9} {'Órfãos':>8} {'% Órfãos':>10} {'Status':>8}")
print("-" * 70)

for r in [resultado_customer, resultado_product]:
    status = "✅ PASS" if r["passed"] else "❌ FAIL"
    print(f"{r['source_col']:<18} {r['total_records']:>8,} {r['valid_references']:>9,} "
          f"{r['orphan_records']:>8,} {r['orphan_pct']:>9.2f}% {status:>8}")

print("-" * 70)

total_orfaos = resultado_customer["orphan_records"] + resultado_product["orphan_records"]
print(f"\n📊 Resumo:")
print(f"   Total de referências órfãs:   {total_orfaos:,}")
print(f"   customer_id — integridade:    {resultado_customer['integrity_score']*100:.2f}%")
print(f"   product_id  — integridade:    {resultado_product['integrity_score']*100:.2f}%")

if total_orfaos > 0:
    print(f"\n⚠️  ALERTA: {total_orfaos:,} registros referenciam entidades inexistentes!")
    print(f"   Esses registros são EXCLUÍDOS silenciosamente em joins de relatório.")
```

**Resultado esperado:**

```
📋 RELATÓRIO DE INTEGRIDADE REFERENCIAL — DataFlow Analytics
======================================================================
Referência           Total  Válidos   Órfãos   % Órfãos   Status
----------------------------------------------------------------------
customer_id         ~47,380  ~46,190   ~1,190     ~2.51% ❌ FAIL
product_id          ~49,955  ~48,705   ~1,250     ~2.50% ❌ FAIL
----------------------------------------------------------------------

📊 Resumo:
   Total de referências órfãs:   ~2,440
   customer_id — integridade:    ~97.49%
   product_id  — integridade:    ~97.50%

⚠️  ALERTA: ~2,440 registros referenciam entidades inexistentes!
   Esses registros são EXCLUÍDOS silenciosamente em joins de relatório.
```

---

## Passo 8: Investigar — De Onde Vêm os Órfãos?

**Descrição:** Antes de decidir o tratamento, precisamos entender a causa raiz. Os órfãos vêm de um parceiro específico? São de um período específico? Esse diagnóstico guia a ação corretiva.

**Código:**

```python
from pyspark.sql.functions import count as spark_count, countDistinct

# Análise: órfãos de customer_id por parceiro (partner_source)
print("🔍 Investigação: órfãos de customer_id por parceiro")
print("=" * 60)

# Recriar o DataFrame de órfãos usando a função
df_vendas_com_customer = df_vendas.filter(col("customer_id").isNotNull())
df_orfaos_customer = df_vendas_com_customer.join(
    df_clientes,
    on="customer_id",
    how="left_anti"
)

# Distribuição por partner_source
df_orfaos_customer.groupBy("partner_source") \
    .agg(
        spark_count("*").alias("qtd_orfaos"),
        countDistinct("customer_id").alias("customer_ids_distintos")
    ) \
    .orderBy(col("qtd_orfaos").desc()) \
    .show(truncate=False)
```

**Resultado esperado:**

```
🔍 Investigação: órfãos de customer_id por parceiro
============================================================
+--------------+----------+----------------------+
|partner_source|qtd_orfaos|customer_ids_distintos|
+--------------+----------+----------------------+
|parceiro_b    |~600      |~120                  |
|parceiro_c    |~350      |~80                   |
|parceiro_a    |~240      |~50                   |
+--------------+----------+----------------------+
```

**Código (análise temporal):**

```python
from pyspark.sql.functions import month, year

# Distribuição temporal dos órfãos
print("\n📅 Distribuição temporal dos órfãos (por mês):")
df_orfaos_customer.withColumn("mes", month("order_date")) \
    .groupBy("mes") \
    .agg(spark_count("*").alias("qtd_orfaos")) \
    .orderBy("mes") \
    .show(truncate=False)
```

**Resultado esperado:**

```
📅 Distribuição temporal dos órfãos (por mês):
+---+----------+
|mes|qtd_orfaos|
+---+----------+
|1  |~90       |
|2  |~85       |
|3  |~100      |
|... |...      |
|12 |~110      |
+---+----------+
```

**Código (padrão nos IDs órfãos):**

```python
# Investigar padrão nos customer_ids órfãos
# Será que seguem um padrão? Ex: todos começam com "CUST-9"?
from pyspark.sql.functions import substring

print("\n🔤 Padrão nos customer_ids órfãos (prefixo):")
df_orfaos_customer.select("customer_id") \
    .distinct() \
    .withColumn("prefixo", substring("customer_id", 1, 7)) \
    .groupBy("prefixo") \
    .agg(spark_count("*").alias("qtd")) \
    .orderBy(col("qtd").desc()) \
    .show(10, truncate=False)
```

**Resultado esperado:**

```
🔤 Padrão nos customer_ids órfãos (prefixo):
+-------+---+
|prefixo|qtd|
+-------+---+
|CUST-90|~80|
|CUST-91|~50|
|CUST-99|~40|
|...    |...|
+-------+---+
```

**Explicação técnica:**

- A análise por `partner_source` revela se um parceiro específico está enviando IDs inválidos
- A análise temporal mostra se o problema é pontual (um mês ruim) ou crônico (todos os meses)
- A análise de prefixo dos IDs pode revelar padrões — ex: IDs acima de 9000 são de um sistema legado que não foi migrado
- Essas informações guiam a conversa com o time de cada parceiro para corrigir na fonte

> **💡 Dica de Carlos:** "Quando os órfãos seguem um padrão claro (prefixo, parceiro, período), a causa raiz é identificável. Se fossem aleatórios, seria mais difícil — provavelmente um race condition na ingestão. No nosso caso, parece que IDs com prefixo CUST-9xxx vêm de um sistema que não está sincronizado."

---

## Passo 9: Decidir Ação — Quarentena ou Preenchimento com "unknown"

**Descrição:** Agora precisamos decidir o que fazer com os registros órfãos. A DataFlow adota duas estratégias dependendo do caso:
1. **Quarentena**: isolar os registros para investigação (abordagem conservadora)
2. **Preenchimento**: substituir a referência por um valor sentinela como "UNKNOWN" para que os registros não sejam perdidos em joins

Vamos implementar a Estratégia 2 (preenchimento) para demonstrar como manter os registros no pipeline.

**Código:**

```python
# Estratégia: criar entradas "UNKNOWN" nas tabelas de referência
# Isso garante que registros órfãos continuem participando de joins

# 1. Criar registro sentinela na tabela de clientes
from pyspark.sql import Row
from pyspark.sql.functions import when

# Abordagem: substituir customer_id órfão por "UNKNOWN_CUSTOMER"
# Para isso, identificamos quais são órfãos e fazemos o replace

# Criar lista de customer_ids válidos para lookup rápido
customer_ids_validos = set(
    df_clientes.select("customer_id")
    .rdd.flatMap(lambda x: x).collect()
)

print(f"📊 customer_ids válidos na referência: {len(customer_ids_validos):,}")
print(f"   (Usaremos broadcast para eficiência no join)")
```

**Resultado esperado:**

```
📊 customer_ids válidos na referência: ~10,000
   (Usaremos broadcast para eficiência no join)
```

**Código (estratégia usando join para marcar órfãos):**

```python
from pyspark.sql.functions import coalesce, when, broadcast

# Abordagem eficiente com Spark: usar left join + coalesce
# Em vez de coletar IDs no driver, fazemos tudo distribuído

# Marcar registros órfãos de customer_id
df_com_flag_customer = df_vendas.join(
    broadcast(df_clientes.select("customer_id").withColumn("_customer_exists", lit(True))),
    on="customer_id",
    how="left"
).withColumn(
    "customer_id_tratado",
    when(
        (col("customer_id").isNull()) | (col("_customer_exists").isNull()),
        lit("UNKNOWN_CUSTOMER")
    ).otherwise(col("customer_id"))
).drop("_customer_exists")

# Contar tratamentos realizados
tratados_customer = df_com_flag_customer.filter(
    col("customer_id_tratado") == "UNKNOWN_CUSTOMER"
).count()

print(f"\n🔧 Tratamento de customer_id:")
print(f"   Registros com customer_id nulo:    marcados como UNKNOWN_CUSTOMER")
print(f"   Registros com customer_id órfão:   marcados como UNKNOWN_CUSTOMER")
print(f"   Total tratados:                    {tratados_customer:,}")
```

**Resultado esperado:**

```
🔧 Tratamento de customer_id:
   Registros com customer_id nulo:    marcados como UNKNOWN_CUSTOMER
   Registros com customer_id órfão:   marcados como UNKNOWN_CUSTOMER
   Total tratados:                    ~3,765
```

**Código (tratamento alternativo — quarentena):**

```python
# Alternativa: Quarentena (separar registros em vez de modificar)
# Use esta abordagem quando quiser investigar antes de decidir

df_validos_ref = df_vendas.join(
    df_clientes.select("customer_id"),
    on="customer_id",
    how="left_semi"  # left_semi = inverso do left_anti (mantém quem TEM match)
)

df_quarentena_ref = df_vendas.filter(col("customer_id").isNotNull()).join(
    df_clientes,
    on="customer_id",
    how="left_anti"
).withColumn("quarantine_reason", lit("customer_id não encontrado na referência")) \
 .withColumn("quarantine_check", lit("referential_integrity")) \
 .withColumn("quarantine_ts", current_timestamp())

print(f"\n🔒 Alternativa — Quarentena:")
print(f"   ✅ Registros válidos (left_semi):    {df_validos_ref.count():,}")
print(f"   🔒 Registros em quarentena:          {df_quarentena_ref.count():,}")
```

**Resultado esperado:**

```
🔒 Alternativa — Quarentena:
   ✅ Registros válidos (left_semi):    ~46,190
   🔒 Registros em quarentena:          ~1,190
```

**Explicação técnica:**

- **`left_semi` join** = inverso do `left_anti` — retorna registros que TÊM correspondência na referência
- **Broadcast**: `broadcast(df_clientes)` instrui o Spark a enviar a tabela pequena de referência para todos os executors, evitando shuffle. Ideal quando a referência cabe em memória (~10K registros é trivial)
- **Sentinela "UNKNOWN"**: permite que registros participem de agregações sem serem perdidos. Relatórios mostram "UNKNOWN_CUSTOMER" em vez de excluir silenciosamente
- A escolha entre quarentena e sentinela depende do contexto:
  - **Quarentena** → quando quer investigar e possivelmente recuperar os dados
  - **Sentinela** → quando quer manter os registros no pipeline imediatamente

> **💡 Dica de Marina:** "Em produção, costumo usar as duas: quarentena para auditoria E sentinela no pipeline. Os registros ficam na quarentena para investigação, mas uma cópia com 'UNKNOWN' segue no fluxo. Assim, nenhuma receita é perdida nos relatórios."

---

## Passo 10: Validar — Após Tratamento, 0 Órfãos Restantes

**Descrição:** Após o tratamento, precisamos validar que a integridade referencial foi restaurada. Vamos adicionar o registro sentinela "UNKNOWN_CUSTOMER" na tabela de referência e executar o check novamente. O resultado deve ser 0 órfãos.

**Código:**

```python
from pyspark.sql.types import StructType, StructField, StringType

# Adicionar registro sentinela às tabelas de referência
# Isso garante que "UNKNOWN_CUSTOMER" e "UNKNOWN_PRODUCT" são válidos em joins

# Sentinela para clientes
schema_clientes = df_clientes.schema
sentinela_cliente = spark.createDataFrame(
    [("UNKNOWN_CUSTOMER",) + (None,) * (len(schema_clientes.fields) - 1)],
    schema_clientes.fieldNames()
)
df_clientes_completo = df_clientes.union(sentinela_cliente)

# Sentinela para produtos
schema_produtos = df_produtos.schema
sentinela_produto = spark.createDataFrame(
    [("UNKNOWN_PRODUCT",) + (None,) * (len(schema_produtos.fields) - 1)],
    schema_produtos.fieldNames()
)
df_produtos_completo = df_produtos.union(sentinela_produto)

print(f"📊 Tabelas de referência atualizadas:")
print(f"   🧑 Clientes: {df_clientes_completo.count():,} (inclui UNKNOWN_CUSTOMER)")
print(f"   📦 Produtos: {df_produtos_completo.count():,} (inclui UNKNOWN_PRODUCT)")
```

**Resultado esperado:**

```
📊 Tabelas de referência atualizadas:
   🧑 Clientes: ~10,001 (inclui UNKNOWN_CUSTOMER)
   📦 Produtos: ~5,001 (inclui UNKNOWN_PRODUCT)
```

**Código (aplicar tratamento completo e re-validar):**

```python
# Aplicar tratamento completo: substituir IDs órfãos por UNKNOWN
# Para customer_id
df_tratado = df_vendas.join(
    df_clientes.select("customer_id").withColumn("_c_exists", lit(True)),
    on="customer_id",
    how="left"
).withColumn(
    "customer_id",
    when(
        (col("customer_id").isNull()) | (col("_c_exists").isNull()),
        lit("UNKNOWN_CUSTOMER")
    ).otherwise(col("customer_id"))
).drop("_c_exists")

# Para product_id
df_tratado = df_tratado.join(
    df_produtos.select("product_id").withColumn("_p_exists", lit(True)),
    on="product_id",
    how="left"
).withColumn(
    "product_id",
    when(
        (col("product_id").isNull()) | (col("_p_exists").isNull()),
        lit("UNKNOWN_PRODUCT")
    ).otherwise(col("product_id"))
).drop("_p_exists")

print(f"\n✅ Tratamento aplicado. Re-validando integridade...")
print(f"   Registros no dataset tratado: {df_tratado.count():,}")
```

**Resultado esperado:**

```
✅ Tratamento aplicado. Re-validando integridade...
   Registros no dataset tratado: ~49,955
```

**Código (re-executar checks de integridade):**

```python
# Re-executar check com tabelas de referência atualizadas
resultado_customer_pos = check_referential_integrity(
    df_source=df_tratado,
    df_ref=df_clientes_completo,
    source_col="customer_id",
    ref_col="customer_id"
)

resultado_product_pos = check_referential_integrity(
    df_source=df_tratado,
    df_ref=df_produtos_completo,
    source_col="product_id",
    ref_col="product_id"
)

# Relatório pós-tratamento
print("\n📋 VALIDAÇÃO PÓS-TRATAMENTO")
print("=" * 70)
print(f"{'Referência':<18} {'Órfãos Antes':>13} {'Órfãos Depois':>14} {'Status':>8}")
print("-" * 70)
print(f"{'customer_id':<18} {resultado_customer['orphan_records']:>13,} "
      f"{resultado_customer_pos['orphan_records']:>14,} "
      f"{'✅ PASS' if resultado_customer_pos['passed'] else '❌ FAIL':>8}")
print(f"{'product_id':<18} {resultado_product['orphan_records']:>13,} "
      f"{resultado_product_pos['orphan_records']:>14,} "
      f"{'✅ PASS' if resultado_product_pos['passed'] else '❌ FAIL':>8}")
print("-" * 70)

# Validação final
assert resultado_customer_pos["orphan_records"] == 0, \
    "ERRO: ainda existem órfãos de customer_id!"
assert resultado_product_pos["orphan_records"] == 0, \
    "ERRO: ainda existem órfãos de product_id!"

print(f"\n🎉 VALIDAÇÃO OK: 0 órfãos restantes!")
print(f"   Integridade referencial: 100% para ambas as referências.")
print(f"   Dataset pronto para joins seguros na camada Gold!")
```

**Resultado esperado:**

```
📋 VALIDAÇÃO PÓS-TRATAMENTO
======================================================================
Referência         Órfãos Antes  Órfãos Depois   Status
----------------------------------------------------------------------
customer_id              ~1,190              0 ✅ PASS
product_id               ~1,250              0 ✅ PASS
----------------------------------------------------------------------

🎉 VALIDAÇÃO OK: 0 órfãos restantes!
   Integridade referencial: 100% para ambas as referências.
   Dataset pronto para joins seguros na camada Gold!
```

**Explicação técnica:**

- Adicionamos registros sentinela às tabelas de referência — agora "UNKNOWN_CUSTOMER" é um ID válido
- No dataset tratado, todos os IDs que eram órfãos agora apontam para "UNKNOWN_*"
- O `assert` garante que não sobrou nenhum órfão — se falhar, algo deu errado no tratamento
- Em produção, a tabela de referência atualizada seria persistida e versionada
- Relatórios podem filtrar "UNKNOWN_*" quando quiserem ou mantê-los para totalização

> **💡 Dica de Carlos:** "O padrão sentinela é comum em data warehouses — a maioria dos modelos dimensionais tem uma linha '-1' ou 'Unknown' em cada dimensão. Isso garante integridade referencial a nível de schema."

---

## Passo 11: Impacto nos Relatórios — Antes vs Depois

**Descrição:** Para fechar, vamos demonstrar o impacto concreto que os órfãos causavam nos relatórios de negócio. Quando a Ana faz join de vendas com clientes para gerar relatório de faturamento por segmento, os órfãos são silenciosamente excluídos em um `inner join`.

**Código:**

```python
from pyspark.sql.functions import sum as spark_sum

# Simular relatório: faturamento total via inner join com clientes
# ANTES do tratamento (usando df original com órfãos)
fat_com_inner_antes = df_vendas.filter(col("customer_id").isNotNull()) \
    .join(df_clientes.select("customer_id"), on="customer_id", how="inner") \
    .agg(spark_sum("total_amount").alias("faturamento")).collect()[0]["faturamento"]

# DEPOIS do tratamento (usando df tratado + referência com sentinela)
fat_com_inner_depois = df_tratado.filter(col("customer_id").isNotNull()) \
    .join(df_clientes_completo.select("customer_id"), on="customer_id", how="inner") \
    .agg(spark_sum("total_amount").alias("faturamento")).collect()[0]["faturamento"]

# Total real (sem join — baseline)
fat_total_real = df_vendas.filter(col("customer_id").isNotNull()) \
    .agg(spark_sum("total_amount").alias("faturamento")).collect()[0]["faturamento"]

receita_perdida = fat_total_real - fat_com_inner_antes
pct_perdida = (receita_perdida / fat_total_real) * 100

print("💰 IMPACTO DE ÓRFÃOS NOS RELATÓRIOS")
print("=" * 60)
print(f"   Faturamento real (todos os registros):  R$ {fat_total_real:,.2f}")
print(f"   Faturamento via join ANTES tratamento:  R$ {fat_com_inner_antes:,.2f}")
print(f"   Faturamento via join DEPOIS tratamento: R$ {fat_com_inner_depois:,.2f}")
print(f"\n   🚨 Receita 'invisível' por órfãos:      R$ {receita_perdida:,.2f}")
print(f"   📉 Percentual perdido em relatórios:    {pct_perdida:.2f}%")
print(f"\n   ✅ Após tratamento, 100% da receita aparece nos relatórios!")
```

**Resultado esperado:**

```
💰 IMPACTO DE ÓRFÃOS NOS RELATÓRIOS
============================================================
   Faturamento real (todos os registros):  R$ X,XXX,XXX.XX
   Faturamento via join ANTES tratamento:  R$ X,XXX,XXX.XX
   Faturamento via join DEPOIS tratamento: R$ X,XXX,XXX.XX

   🚨 Receita 'invisível' por órfãos:      R$ ~XX,XXX.XX
   📉 Percentual perdido em relatórios:    ~2.51%

   ✅ Após tratamento, 100% da receita aparece nos relatórios!
```

**Explicação técnica:**

- O `inner join` descarta registros sem correspondência — os órfãos simplesmente desaparecem
- Sem o check de integridade, a Ana nunca saberia que ~2.5% da receita estava "sumindo"
- Após o tratamento, `UNKNOWN_CUSTOMER` existe na referência, então o join mantém todos os registros
- A diferença entre "antes" e "depois" é exatamente a receita dos clientes órfãos
- Esse é o argumento de negócio mais forte para investir em quality checks: **dinheiro que estava invisível**

> **💡 Dica de Ana (PO):** "2.5% de receita invisível! Isso explica a diferença que eu vinha reportando. Com o sentinela UNKNOWN_CUSTOMER, pelo menos a receita aparece no total — e podemos investigar quem são esses clientes com calma, sem perder visibilidade financeira."

---

## Resumo do Exercício

Neste exercício você implementou o terceiro check de qualidade do Data Quality Program da DataFlow:

| Etapa | O que fizemos | Função/Técnica |
|-------|---------------|----------------|
| Carregar referências | Tabelas-mestre de clientes e produtos | `spark.read.parquet()` |
| Conceito left_anti | Entender o join que encontra "quem NÃO existe" | `left_anti` vs `left join` + filter |
| Detectar órfãos | Encontrar vendas com IDs sem correspondência | `join(..., how="left_anti")` |
| Função reutilizável | `check_referential_integrity()` genérica | Aceita qualquer par source/ref |
| Investigar padrões | Origem, período e prefixo dos órfãos | `groupBy("partner_source")`, `substring()` |
| Tratar — sentinela | Substituir IDs órfãos por "UNKNOWN_*" | `when().otherwise()` + `broadcast()` |
| Tratar — quarentena | Isolar registros para investigação | `left_anti` + metadados |
| Validar resultado | Confirmar 0 órfãos após tratamento | `assert` + re-execução do check |
| Medir impacto | Receita invisível em relatórios com join | Comparação antes/depois |

### A Função que Você Criou

```python
check_referential_integrity(df_source, df_ref, source_col, ref_col) -> Dict
```

Essa função será reutilizada nos próximos exercícios como parte do `DataQualityFramework`.

### Conceitos-Chave

1. **Integridade referencial** = todo valor de FK no dataset filho deve existir na tabela-mãe
2. **Registro órfão** = registro que referencia uma entidade inexistente
3. **`left_anti join`** = retorna registros da esquerda SEM correspondência na direita
4. **`left_semi join`** = inverso — retorna registros da esquerda COM correspondência
5. **Sentinela "UNKNOWN"** = registro especial na referência que absorve órfãos em joins
6. **Impacto silencioso** = joins descartam órfãos sem erro — o resultado simplesmente fica menor
7. **Broadcast join** = enviar tabela pequena para todos os executors (evita shuffle)

### Tipos de Join para Quality Checks

| Join | Retorna | Uso em QA |
|------|---------|-----------|
| `left_anti` | Esquerda SEM match na direita | Encontrar órfãos |
| `left_semi` | Esquerda COM match na direita | Filtrar válidos |
| `inner` | Ambos COM match | Relatórios (perde órfãos!) |
| `left` | Tudo da esquerda + match ou NULL | Detectar + manter |

> **Carlos:** "Três checks implementados! Completude, unicidade e integridade referencial cobrem a grande maioria dos problemas de qualidade em pipelines de dados. No próximo exercício, vamos juntar tudo: implementar um **sistema de quarentena** que separa automaticamente dados inválidos e envia alertas quando os thresholds são ultrapassados."

---

## Próximo Exercício

➡️ **Exercício 4 — Sistema de Quarentena** (`04_quarentena.md`): separação automática de dados válidos e inválidos com metadados de auditoria e pipeline de reprocessamento.
