# Exercício 2 — Check de Unicidade (Detecção de Duplicatas)

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "A Ana acabou de me ligar preocupada — o faturamento mensal está 3% acima do que o financeiro reporta. Já verificamos completude e está ok. Minha aposta? **Duplicatas.** Quando integramos dados de múltiplos parceiros, é comum o mesmo pedido entrar mais de uma vez. Um `order_id` repetido significa receita contada em dobro. Vamos usar `groupBy` + `count` para detectar e depois decidir o que fazer com elas."

## Objetivos

Ao final deste exercício, você será capaz de:

- Detectar registros duplicados usando `groupBy` + `count` + `filter`
- Quantificar o impacto: quantas duplicatas existem e qual o percentual do total
- Construir uma função reutilizável `check_uniqueness(df, key_columns) -> Dict`
- Investigar duplicatas: são cópias exatas ou parciais?
- Deduplicar usando `dropDuplicates()` (abordagem simples)
- Deduplicar usando window function `row_number()` (abordagem com mais controle)
- Comparar as duas abordagens e entender quando usar cada uma
- Medir o impacto de duplicatas em agregações financeiras

## Pré-requisitos

- Ambiente Docker rodando (Spark + Jupyter)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `dados_sujos/vendas_problemas.parquet` disponível na pasta `data/aula_06/`
- Exercício 01 (Completude) concluído — `df_vendas` já carregado

## Duração Estimada

⏱️ ~20 minutos

---

## Passo 1: Garantir que o DataFrame Está Carregado

**Descrição:** Se você está continuando do exercício anterior, o `df_vendas` já está em memória. Caso contrário, vamos recarregá-lo. Também garantimos que a SparkSession está ativa.

**Código:**

```python
from pyspark.sql import SparkSession

# Criar/recuperar SparkSession
spark = SparkSession.builder \
    .appName("DataFlow-Aula06-QualityCheck") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Carregar dataset (se ainda não estiver carregado)
df_vendas = spark.read.parquet("data/aula_06/dados_sujos/vendas_problemas.parquet")

total_registros = df_vendas.count()
print(f"✅ Dataset carregado: {total_registros:,} registros")
print(f"   Colunas: {df_vendas.columns}")
```

**Resultado esperado:**

```
✅ Dataset carregado: 51,500 registros
   Colunas: ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'total_amount', 'order_date', 'payment_method', 'shipping_city', 'shipping_state', 'status', 'partner_source']
```

---

## Passo 2: Detectar Duplicatas com groupBy + count

**Descrição:** A forma mais direta de detectar duplicatas é agrupar pela(s) coluna(s) chave e contar ocorrências. Se um `order_id` aparece mais de uma vez, é duplicata. Vamos começar identificando quais `order_id` têm mais de um registro.

**Código:**

```python
from pyspark.sql.functions import col, count, sum as spark_sum

# Agrupar por order_id e contar ocorrências
df_contagem = df_vendas.groupBy("order_id") \
    .count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc())

# Quantos order_ids estão duplicados?
total_ids_duplicados = df_contagem.count()
print(f"🔍 order_ids com mais de 1 registro: {total_ids_duplicados:,}")

# Visualizar os mais repetidos
print(f"\n📊 Top 10 order_ids mais duplicados:")
df_contagem.show(10, truncate=False)
```

**Resultado esperado:**

```
🔍 order_ids com mais de 1 registro: ~1,545

📊 Top 10 order_ids mais duplicados:
+------------------------------------+-----+
|order_id                            |count|
+------------------------------------+-----+
|ORD-2023-XXXXX                      |2    |
|ORD-2023-XXXXX                      |2    |
|ORD-2023-XXXXX                      |2    |
|...                                 |2    |
+------------------------------------+-----+
only showing top 10 rows
```

**Explicação técnica:**

- `groupBy("order_id")` — agrupa todos os registros com o mesmo `order_id`
- `.count()` — conta quantos registros existem em cada grupo
- `.filter(col("count") > 1)` — mantém apenas grupos com mais de 1 ocorrência (duplicatas)
- `.orderBy(col("count").desc())` — ordena do mais duplicado para o menos
- Cada `order_id` nesta lista aparece pelo menos 2 vezes no dataset original

> **💡 Dica de Carlos:** "Na maioria dos casos de duplicata por ingestão, o count será 2 (registro entrou duas vezes). Se você vir count = 3 ou mais, investigue — pode ser um bug no pipeline de extração."

---

## Passo 3: Quantificar o Impacto das Duplicatas

**Descrição:** Saber que existem duplicatas é o primeiro passo. Agora precisamos quantificar: quantos registros no total são duplicatas? Qual o percentual sobre o dataset? Isso nos dá a magnitude do problema.

**Código:**

```python
# Total de registros que são duplicatas (todos os registros dos groups com count > 1)
# Se order_id X aparece 2 vezes, são 2 registros duplicados (1 é "original", 1 é "extra")
df_registros_duplicados = df_vendas.join(
    df_contagem.select("order_id"),
    on="order_id",
    how="inner"
)

total_registros_em_grupos_dup = df_registros_duplicados.count()

# Registros "extras" = total em grupos duplicados - quantidade de grupos (os originais)
registros_extras = total_registros_em_grupos_dup - total_ids_duplicados

print(f"📊 Análise de Duplicatas:")
print(f"   Total de registros no dataset:       {total_registros:,}")
print(f"   order_ids únicos com duplicata:      {total_ids_duplicados:,}")
print(f"   Registros em grupos duplicados:      {total_registros_em_grupos_dup:,}")
print(f"   Registros extras (duplicatas):       {registros_extras:,}")
print(f"   Percentual de duplicatas:            {(registros_extras/total_registros)*100:.2f}%")
print(f"\n   💰 Se cada registro vale em média R$ X,")
print(f"      as duplicatas inflam o faturamento em ~{registros_extras:,} transações!")
```

**Resultado esperado:**

```
📊 Análise de Duplicatas:
   Total de registros no dataset:       51,500
   order_ids únicos com duplicata:      ~1,545
   Registros em grupos duplicados:      ~3,090
   Registros extras (duplicatas):       ~1,545
   Percentual de duplicatas:            ~3.00%

   💰 Se cada registro vale em média R$ X,
      as duplicatas inflam o faturamento em ~1,545 transações!
```

**Explicação técnica:**

- Fazemos um `inner join` entre o dataset original e a lista de `order_id` duplicados
- Isso nos dá TODOS os registros que pertencem a grupos duplicados (originais + extras)
- Se um `order_id` aparece 2 vezes, ambos registros são retornados no join
- Registros extras = total em grupos duplicados - número de `order_id` distintos nesses grupos
- O percentual de ~3% confirma a suspeita da Ana sobre o faturamento inflado

> **💡 Dica de Carlos:** "3% de duplicatas pode parecer pouco, mas em faturamento mensal de R$ 50 milhões, são R$ 1.5 milhão de receita fantasma. Isso afeta projeções, comissões e impostos."

---

## Passo 4: Construir a Função `check_uniqueness()`

**Descrição:** Assim como fizemos com `check_completeness()`, vamos encapsular a lógica de detecção de duplicatas em uma função reutilizável. Ela recebe um DataFrame, as colunas-chave para verificar unicidade e retorna métricas detalhadas.

**Código:**

```python
from pyspark.sql import DataFrame
from typing import Dict, List

def check_uniqueness(df: DataFrame, key_columns: List[str]) -> Dict:
    """
    Verifica a unicidade de registros com base em colunas-chave.
    
    Parâmetros:
        df: DataFrame PySpark a ser verificado
        key_columns: Lista de colunas que compõem a chave de unicidade
                     Ex: ["order_id"] ou ["order_id", "product_id"]
    
    Retorna:
        Dicionário com métricas de unicidade:
        - total_records: total de registros no DataFrame
        - distinct_keys: quantidade de combinações únicas das key_columns
        - duplicate_keys: quantidade de chaves que aparecem mais de uma vez
        - duplicate_records: total de registros extras (acima de 1 por chave)
        - duplicate_pct: percentual de registros extras sobre o total
        - passed: True se não houver duplicatas (0%)
        - max_occurrences: máximo de vezes que uma chave se repete
    """
    total = df.count()
    
    # Contar ocorrências de cada combinação de colunas-chave
    df_grouped = df.groupBy(key_columns).count()
    
    # Quantidade de chaves distintas
    distinct_keys = df_grouped.count()
    
    # Chaves duplicadas (count > 1)
    df_duplicates = df_grouped.filter(col("count") > 1)
    duplicate_keys = df_duplicates.count()
    
    # Total de registros extras
    if duplicate_keys > 0:
        # Soma de (count - 1) para cada chave duplicada = registros extras
        duplicate_records = df_duplicates.select(
            spark_sum(col("count") - 1).alias("extras")
        ).collect()[0]["extras"]
        max_occurrences = df_duplicates.agg({"count": "max"}).collect()[0][0]
    else:
        duplicate_records = 0
        max_occurrences = 1
    
    duplicate_pct = (duplicate_records / total) * 100 if total > 0 else 0.0
    
    return {
        "key_columns": key_columns,
        "total_records": total,
        "distinct_keys": distinct_keys,
        "duplicate_keys": duplicate_keys,
        "duplicate_records": int(duplicate_records),
        "duplicate_pct": round(duplicate_pct, 2),
        "passed": duplicate_keys == 0,
        "max_occurrences": int(max_occurrences)
    }

print("✅ Função check_uniqueness() definida com sucesso!")
```

**Resultado esperado:**

```
✅ Função check_uniqueness() definida com sucesso!
```

**Explicação técnica:**

- A função aceita uma **lista** de colunas-chave — isso permite verificar chaves compostas (ex: `["order_id", "product_id"]`)
- `duplicate_records` calcula a soma de `(count - 1)` para cada grupo, pois 1 registro é o "original" e os demais são "extras"
- O campo `passed` é `True` apenas se não houver NENHUMA duplicata — threshold rígido
- `max_occurrences` ajuda a identificar outliers (ex: um order_id que aparece 10 vezes indica um bug grave)
- Usamos `.collect()[0]` para trazer um resultado pequeno ao driver — seguro pois é uma agregação

> **💡 Dica de Carlos:** "Note que passamos `key_columns` como lista. Isso é importante porque às vezes a unicidade não é por uma coluna só. Ex: uma venda pode ter o mesmo `order_id` para itens diferentes (`product_id` diferente). A chave composta seria `['order_id', 'product_id']`."

---

## Passo 5: Executar o Check e Gerar Relatório

**Descrição:** Vamos executar `check_uniqueness()` usando `order_id` como chave primária e apresentar os resultados no formato de relatório que a Ana precisa para o comitê de qualidade.

**Código:**

```python
# Executar check de unicidade por order_id
resultado = check_uniqueness(df_vendas, ["order_id"])

# Relatório formatado
print("📋 RELATÓRIO DE UNICIDADE — DataFlow Analytics")
print("=" * 60)
print(f"  Chave verificada:        {resultado['key_columns']}")
print(f"  Total de registros:      {resultado['total_records']:,}")
print(f"  Chaves distintas:        {resultado['distinct_keys']:,}")
print(f"  Chaves duplicadas:       {resultado['duplicate_keys']:,}")
print(f"  Registros extras:        {resultado['duplicate_records']:,}")
print(f"  Percentual duplicatas:   {resultado['duplicate_pct']:.2f}%")
print(f"  Máx. ocorrências:        {resultado['max_occurrences']}")
print(f"  Status:                  {'✅ PASS' if resultado['passed'] else '❌ FAIL'}")
print("=" * 60)

if not resultado['passed']:
    print(f"\n⚠️  ALERTA: {resultado['duplicate_records']:,} registros duplicados detectados!")
    print(f"   Isso representa {resultado['duplicate_pct']:.2f}% do dataset.")
    print(f"   Impacto estimado no faturamento: +{resultado['duplicate_pct']:.2f}% de inflação.")
```

**Resultado esperado:**

```
📋 RELATÓRIO DE UNICIDADE — DataFlow Analytics
============================================================
  Chave verificada:        ['order_id']
  Total de registros:      51,500
  Chaves distintas:        ~49,955
  Chaves duplicadas:       ~1,545
  Registros extras:        ~1,545
  Percentual duplicatas:   ~3.00%
  Máx. ocorrências:        2
  Status:                  ❌ FAIL
============================================================

⚠️  ALERTA: ~1,545 registros duplicados detectados!
   Isso representa ~3.00% do dataset.
   Impacto estimado no faturamento: +3.00% de inflação.
```

---

## Passo 6: Investigar as Duplicatas — Cópias Exatas ou Parciais?

**Descrição:** Nem toda duplicata é igual. Às vezes o registro inteiro é idêntico (cópia exata — reprocessamento), outras vezes apenas a chave é igual mas outros campos diferem (duplicata parcial — pode ser um update). Precisamos investigar para decidir a estratégia de deduplicação.

**Código:**

```python
from pyspark.sql.functions import collect_list, size, array_distinct

# Pegar uma amostra de order_ids duplicados para investigar
amostra_ids = df_contagem.limit(5).select("order_id").collect()
ids_para_investigar = [row["order_id"] for row in amostra_ids]

print("🔍 Investigando duplicatas — são cópias exatas?")
print("=" * 60)

# Para cada order_id duplicado, mostrar TODOS os registros
for oid in ids_para_investigar[:3]:  # Investigar 3 exemplos
    print(f"\n📌 order_id: {oid}")
    df_vendas.filter(col("order_id") == oid) \
        .select("order_id", "customer_id", "product_id", 
                "quantity", "total_amount", "order_date") \
        .show(truncate=False)
```

**Resultado esperado:**

```
🔍 Investigando duplicatas — são cópias exatas?
============================================================

📌 order_id: ORD-2023-XXXXX
+------------------------------------+-----------+----------+--------+------------+-------------------+
|order_id                            |customer_id|product_id|quantity|total_amount|order_date         |
+------------------------------------+-----------+----------+--------+------------+-------------------+
|ORD-2023-XXXXX                      |CUST-001   |PROD-042  |3       |449.97      |2023-06-15 10:30:00|
|ORD-2023-XXXXX                      |CUST-001   |PROD-042  |3       |449.97      |2023-06-15 10:30:00|
+------------------------------------+-----------+----------+--------+------------+-------------------+
(cópia exata — todos os campos são idênticos)

📌 order_id: ORD-2023-YYYYY
+------------------------------------+-----------+----------+--------+------------+-------------------+
|order_id                            |customer_id|product_id|quantity|total_amount|order_date         |
+------------------------------------+-----------+----------+--------+------------+-------------------+
|ORD-2023-YYYYY                      |CUST-089   |PROD-015  |1       |89.90       |2023-07-20 14:15:00|
|ORD-2023-YYYYY                      |CUST-089   |PROD-015  |1       |89.90       |2023-07-20 14:22:00|
+------------------------------------+-----------+----------+--------+------------+-------------------+
(duplicata parcial — mesmo pedido, mas timestamp ligeiramente diferente)
```

**Código (análise automatizada de duplicatas exatas vs parciais):**

```python
# Verificar quantas duplicatas são cópias EXATAS (todas as colunas iguais)
total_com_duplicata = df_vendas.count()
total_distinct_todas_colunas = df_vendas.distinct().count()
copias_exatas = total_com_duplicata - total_distinct_todas_colunas

# Duplicatas parciais = duplicatas por order_id - cópias exatas
duplicatas_parciais = resultado['duplicate_records'] - copias_exatas

print(f"\n📊 Classificação das Duplicatas:")
print(f"   Total de registros extras (por order_id): {resultado['duplicate_records']:,}")
print(f"   Cópias exatas (todas colunas idênticas):  {copias_exatas:,}")
print(f"   Duplicatas parciais (alguma diferença):   {duplicatas_parciais:,}")
print(f"\n   📋 Proporção:")
if resultado['duplicate_records'] > 0:
    pct_exatas = (copias_exatas / resultado['duplicate_records']) * 100
    pct_parciais = (duplicatas_parciais / resultado['duplicate_records']) * 100
    print(f"      Exatas:   {pct_exatas:.1f}%")
    print(f"      Parciais: {pct_parciais:.1f}%")
```

**Resultado esperado:**

```
📊 Classificação das Duplicatas:
   Total de registros extras (por order_id): ~1,545
   Cópias exatas (todas colunas idênticas):  ~1,000
   Duplicatas parciais (alguma diferença):   ~545

   📋 Proporção:
      Exatas:   ~65%
      Parciais: ~35%
```

**Explicação técnica:**

- `df.distinct()` remove registros onde TODAS as colunas são iguais — detecta cópias exatas
- A diferença entre `count()` e `distinct().count()` nos dá o número de cópias exatas
- Subtraindo das duplicatas por `order_id`, obtemos as duplicatas parciais
- **Cópia exata**: provavelmente causada por reprocessamento (mesmo arquivo carregado 2x)
- **Duplicata parcial**: mais complexa — pode ser retry com timestamp diferente, ou update de status
- A estratégia de deduplicação depende do tipo: cópias exatas → `dropDuplicates()` simples; parciais → window function para manter a mais recente

> **💡 Dica de Carlos:** "Se >90% são cópias exatas, o problema provavelmente é no pipeline de ingestão (arquivo carregado duas vezes). Se a maioria é parcial, pode ser um retry no sistema de origem. Essa distinção guia a correção na causa raiz."

---

## Passo 7: Deduplicação — Abordagem 1: `dropDuplicates()`

**Descrição:** A forma mais simples de remover duplicatas no Spark é usando `dropDuplicates()`. Quando chamado com uma lista de colunas, mantém apenas a primeira ocorrência de cada combinação. É ideal para cópias exatas, mas não oferece controle sobre QUAL registro manter.

**Código:**

```python
# Abordagem 1: dropDuplicates — simples e direto
# Mantém apenas 1 registro por order_id (o primeiro encontrado)
df_dedup_simples = df_vendas.dropDuplicates(["order_id"])

count_antes = df_vendas.count()
count_depois = df_dedup_simples.count()
removidos = count_antes - count_depois

print("🧹 Deduplicação — Abordagem 1: dropDuplicates()")
print("=" * 55)
print(f"   Registros antes:   {count_antes:,}")
print(f"   Registros depois:  {count_depois:,}")
print(f"   Removidos:         {removidos:,}")
print(f"   Redução:           {(removidos/count_antes)*100:.2f}%")
```

**Resultado esperado:**

```
🧹 Deduplicação — Abordagem 1: dropDuplicates()
=======================================================
   Registros antes:   51,500
   Registros depois:  ~49,955
   Removidos:         ~1,545
   Redução:           ~3.00%
```

**Código (validar resultado):**

```python
# Validar: após dedup, não deve haver mais duplicatas por order_id
resultado_pos_dedup = check_uniqueness(df_dedup_simples, ["order_id"])
print(f"\n✅ Validação pós-dedup:")
print(f"   Chaves duplicadas restantes: {resultado_pos_dedup['duplicate_keys']}")
print(f"   Status: {'✅ PASS' if resultado_pos_dedup['passed'] else '❌ FAIL'}")
```

**Resultado esperado:**

```
✅ Validação pós-dedup:
   Chaves duplicadas restantes: 0
   Status: ✅ PASS
```

**Explicação técnica:**

- `dropDuplicates(["order_id"])` mantém o PRIMEIRO registro encontrado para cada `order_id`
- "Primeiro" aqui é arbitrário — depende da ordem interna das partições Spark (não determinístico!)
- Vantagem: simples, rápido, funciona perfeitamente para cópias exatas
- Desvantagem: para duplicatas parciais, não temos controle sobre QUAL versão é mantida
- Se quisermos manter sempre a versão mais recente (por `order_date`), precisamos de window function

> **💡 Dica de Carlos:** "Use `dropDuplicates()` quando não importa QUAL registro manter — geralmente cópias exatas. Quando há diferença entre os registros (timestamps diferentes, status diferente), precisamos de mais controle."

---

## Passo 8: Deduplicação — Abordagem 2: Window Function `row_number()`

**Descrição:** Para duplicatas parciais, precisamos decidir QUAL registro manter. A abordagem com `row_number()` numera os registros dentro de cada grupo (partição por `order_id`) ordenados por um critério (ex: `order_date` mais recente). Mantemos apenas o registro com `row_number = 1`.

**Código:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Definir a janela: particionar por order_id, ordenar por order_date DESC
# Isso coloca o registro mais recente como row_number = 1
window_spec = Window.partitionBy("order_id").orderBy(desc("order_date"))

# Adicionar row_number a cada registro
df_com_rank = df_vendas.withColumn("row_num", row_number().over(window_spec))

# Visualizar como funciona para um order_id duplicado
print("🔍 Como o row_number() funciona:")
df_com_rank.filter(col("order_id").isin(ids_para_investigar[:2])) \
    .select("order_id", "order_date", "total_amount", "row_num") \
    .orderBy("order_id", "row_num") \
    .show(truncate=False)
```

**Resultado esperado:**

```
🔍 Como o row_number() funciona:
+------------------------------------+-------------------+------------+-------+
|order_id                            |order_date         |total_amount|row_num|
+------------------------------------+-------------------+------------+-------+
|ORD-2023-XXXXX                      |2023-06-15 10:30:00|449.97      |1      |
|ORD-2023-XXXXX                      |2023-06-15 10:30:00|449.97      |2      |
|ORD-2023-YYYYY                      |2023-07-20 14:22:00|89.90       |1      |
|ORD-2023-YYYYY                      |2023-07-20 14:15:00|89.90       |2      |
+------------------------------------+-------------------+------------+-------+
```

**Código (aplicar deduplicação mantendo o mais recente):**

```python
# Manter apenas row_number = 1 (registro mais recente por order_date)
df_dedup_window = df_com_rank.filter(col("row_num") == 1).drop("row_num")

count_window = df_dedup_window.count()
removidos_window = count_antes - count_window

print(f"\n🧹 Deduplicação — Abordagem 2: Window Function row_number()")
print("=" * 60)
print(f"   Registros antes:   {count_antes:,}")
print(f"   Registros depois:  {count_window:,}")
print(f"   Removidos:         {removidos_window:,}")
print(f"   Redução:           {(removidos_window/count_antes)*100:.2f}%")

# Validar
resultado_pos_window = check_uniqueness(df_dedup_window, ["order_id"])
print(f"\n   ✅ Chaves duplicadas restantes: {resultado_pos_window['duplicate_keys']}")
print(f"   Status: {'✅ PASS' if resultado_pos_window['passed'] else '❌ FAIL'}")
```

**Resultado esperado:**

```
🧹 Deduplicação — Abordagem 2: Window Function row_number()
============================================================
   Registros antes:   51,500
   Registros depois:  ~49,955
   Removidos:         ~1,545
   Redução:           ~3.00%

   ✅ Chaves duplicadas restantes: 0
   Status: ✅ PASS
```

**Explicação técnica:**

- `Window.partitionBy("order_id")` — cria uma "janela" para cada `order_id` (similar a um GROUP BY, mas sem agregar)
- `.orderBy(desc("order_date"))` — dentro de cada janela, ordena do mais recente ao mais antigo
- `row_number().over(window_spec)` — atribui 1, 2, 3... para cada registro dentro da janela
- Filtrando `row_num == 1`, mantemos apenas o registro mais recente de cada `order_id`
- `.drop("row_num")` — remove a coluna auxiliar que não faz parte do schema original
- Esta abordagem é **determinística**: sempre mantém o registro mais recente, independente da ordem das partições

> **💡 Dica de Carlos:** "A window function é mais cara computacionalmente (precisa ordenar dentro de cada partição), mas dá total controle. Em produção, usamos row_number quando a regra de negócio diz 'mantenha o mais recente' ou 'mantenha o de maior valor'."

---

## Passo 9: Comparação das Abordagens

**Descrição:** Ambas as abordagens removem duplicatas, mas têm trade-offs diferentes. Vamos comparar lado a lado para entender quando usar cada uma.

**Código:**

```python
# Comparação lado a lado
print("📊 Comparação de Abordagens de Deduplicação")
print("=" * 70)
print(f"{'Aspecto':<25} {'dropDuplicates()':<22} {'row_number() Window':<22}")
print("-" * 70)
print(f"{'Simplicidade':<25} {'✅ Muito simples':<22} {'⚠️ Mais código':<22}")
print(f"{'Determinístico':<25} {'❌ Não':<22} {'✅ Sim':<22}")
print(f"{'Controle qual manter':<25} {'❌ Arbitrário':<22} {'✅ Critério definido':<22}")
print(f"{'Performance':<25} {'✅ Mais rápido':<22} {'⚠️ Requer sort':<22}")
print(f"{'Uso de memória':<25} {'✅ Menor':<22} {'⚠️ Maior (window)':<22}")
print(f"{'Registros removidos':<25} {f'{removidos:,}':<22} {f'{removidos_window:,}':<22}")
print("-" * 70)
print(f"\n💡 Recomendação:")
print(f"   • Cópias exatas → dropDuplicates() (simples e eficiente)")
print(f"   • Duplicatas parciais → row_number() (mantém versão mais recente)")
print(f"   • Produção/auditoria → row_number() (resultado reproduzível)")
```

**Resultado esperado:**

```
📊 Comparação de Abordagens de Deduplicação
======================================================================
Aspecto                   dropDuplicates()      row_number() Window   
----------------------------------------------------------------------
Simplicidade              ✅ Muito simples      ⚠️ Mais código       
Determinístico            ❌ Não                ✅ Sim                
Controle qual manter      ❌ Arbitrário         ✅ Critério definido  
Performance               ✅ Mais rápido        ⚠️ Requer sort       
Uso de memória            ✅ Menor              ⚠️ Maior (window)    
Registros removidos       ~1,545                ~1,545                
----------------------------------------------------------------------

💡 Recomendação:
   • Cópias exatas → dropDuplicates() (simples e eficiente)
   • Duplicatas parciais → row_number() (mantém versão mais recente)
   • Produção/auditoria → row_number() (resultado reproduzível)
```

**Explicação técnica:**

- **Determinístico** significa que o resultado é o mesmo em execuções diferentes. Com `dropDuplicates()`, a ordem das partições pode mudar entre execuções, mantendo registros diferentes a cada vez
- **Performance**: `dropDuplicates()` usa hash internamente (O(n)); `row_number()` precisa ordenar (O(n log n)) dentro de cada partição
- Em datasets grandes (bilhões de registros), a diferença de performance é significativa
- Para o cenário da DataFlow (~50K registros), ambas são instantâneas — a escolha é por clareza e controle

> **💡 Dica de Marina:** "Em produção, sempre prefira a abordagem determinística. Se o pipeline roda todo dia e o resultado muda, você perde rastreabilidade. Use `row_number()` com critério explícito (data, versão, ou status)."

---

## Passo 10: Impacto de Duplicatas em Agregações — Antes vs Depois

**Descrição:** A Ana precisa apresentar ao Roberto (CEO) o impacto concreto das duplicatas no faturamento. Vamos calcular métricas financeiras ANTES e DEPOIS da deduplicação para mostrar o quanto o relatório estava inflado.

**Código:**

```python
from pyspark.sql.functions import sum as spark_sum, avg, countDistinct

# Métricas ANTES da deduplicação (com duplicatas)
metricas_antes = df_vendas.agg(
    spark_sum("total_amount").alias("faturamento_total"),
    avg("total_amount").alias("ticket_medio"),
    count("*").alias("total_transacoes"),
    countDistinct("order_id").alias("pedidos_unicos")
).collect()[0]

# Métricas DEPOIS da deduplicação (usando window function - mais preciso)
metricas_depois = df_dedup_window.agg(
    spark_sum("total_amount").alias("faturamento_total"),
    avg("total_amount").alias("ticket_medio"),
    count("*").alias("total_transacoes"),
    countDistinct("order_id").alias("pedidos_unicos")
).collect()[0]

# Comparação
fat_antes = metricas_antes["faturamento_total"]
fat_depois = metricas_depois["faturamento_total"]
inflacao = fat_antes - fat_depois
pct_inflacao = (inflacao / fat_depois) * 100

print("💰 IMPACTO DE DUPLICATAS NO FATURAMENTO")
print("=" * 60)
print(f"{'Métrica':<25} {'Com Duplicatas':>15} {'Sem Duplicatas':>15}")
print("-" * 60)
print(f"{'Faturamento Total':<25} R$ {fat_antes:>12,.2f} R$ {fat_depois:>12,.2f}")
print(f"{'Total Transações':<25} {metricas_antes['total_transacoes']:>15,} {metricas_depois['total_transacoes']:>15,}")
print(f"{'Pedidos Únicos':<25} {metricas_antes['pedidos_unicos']:>15,} {metricas_depois['pedidos_unicos']:>15,}")
print(f"{'Ticket Médio':<25} R$ {metricas_antes['ticket_medio']:>12,.2f} R$ {metricas_depois['ticket_medio']:>12,.2f}")
print("-" * 60)
print(f"\n🚨 Inflação causada por duplicatas:")
print(f"   Valor absoluto:  R$ {inflacao:,.2f}")
print(f"   Percentual:      +{pct_inflacao:.2f}%")
print(f"   Transações fantasma: {metricas_antes['total_transacoes'] - metricas_depois['total_transacoes']:,}")
```

**Resultado esperado:**

```
💰 IMPACTO DE DUPLICATAS NO FATURAMENTO
============================================================
Métrica                   Com Duplicatas  Sem Duplicatas
------------------------------------------------------------
Faturamento Total         R$  X,XXX,XXX  R$  X,XXX,XXX
Total Transações                 51,500          ~49,955
Pedidos Únicos                  ~49,955          ~49,955
Ticket Médio              R$      XXX.XX R$      XXX.XX
------------------------------------------------------------

🚨 Inflação causada por duplicatas:
   Valor absoluto:  R$ ~XX,XXX
   Percentual:      +~3.00%
   Transações fantasma: ~1,545
```

**Explicação técnica:**

- O faturamento com duplicatas inclui a receita dos registros extras — dinheiro que não existe
- O ticket médio pode mudar levemente após a dedup (depende dos valores dos registros removidos)
- `countDistinct("order_id")` confirma: mesmo antes da dedup, os pedidos únicos batem com o resultado pós-dedup
- Esse relatório é o tipo de evidência que a Ana precisa para justificar investimento em qualidade de dados
- Em escala real (ex: 10M registros/mês), 3% de inflação pode significar milhões de reais em relatórios errados

> **💡 Dica de Ana (PO):** "Com esses números, consigo mostrar ao Roberto que investir em qualidade de dados não é custo — é prevenção de prejuízo. Imagina se tomássemos decisões de expansão baseadas em faturamento 3% inflado?"

---

## Passo 11: Validação Final — count após dedup == distinct count

**Descrição:** Como validação final, vamos confirmar que após a deduplicação, o número de registros é exatamente igual ao número de `order_id` distintos. Essa é a prova definitiva de que a deduplicação funcionou corretamente.

**Código:**

```python
# Validação final: count deve ser igual ao distinct count de order_id
count_final = df_dedup_window.count()
distinct_order_ids = df_dedup_window.select("order_id").distinct().count()

print("✅ VALIDAÇÃO FINAL DE DEDUPLICAÇÃO")
print("=" * 50)
print(f"   Registros após dedup:          {count_final:,}")
print(f"   order_ids distintos após dedup: {distinct_order_ids:,}")
print(f"   Diferença:                      {count_final - distinct_order_ids}")

assert count_final == distinct_order_ids, \
    f"ERRO: count ({count_final}) != distinct ({distinct_order_ids})"

print(f"\n   ✓ Validação OK: cada order_id aparece exatamente 1 vez.")
print(f"   ✓ Dataset limpo pronto para uso na camada Silver!")
```

**Resultado esperado:**

```
✅ VALIDAÇÃO FINAL DE DEDUPLICAÇÃO
==================================================
   Registros após dedup:          ~49,955
   order_ids distintos após dedup: ~49,955
   Diferença:                      0

   ✓ Validação OK: cada order_id aparece exatamente 1 vez.
   ✓ Dataset limpo pronto para uso na camada Silver!
```

**Explicação técnica:**

- O `assert` garante que não restou nenhuma duplicata — se falhar, algo deu errado na deduplicação
- Essa validação é um padrão de engenharia de dados: sempre valide o resultado de transformações
- Em produção, esse check seria uma task no Airflow que bloqueia o pipeline se falhar
- A igualdade `count == distinct` é a definição formal de unicidade para a coluna testada

> **💡 Dica de Carlos:** "Sempre valide DEPOIS de transformar. Não confie que a função fez o que deveria. Esse assert no notebook vira um `if` em produção que levanta alerta caso algo inesperado aconteça."

---

## Resumo do Exercício

Neste exercício você implementou o segundo check de qualidade do Data Quality Program da DataFlow:

| Etapa | O que fizemos | Função/Técnica |
|-------|---------------|----------------|
| Detectar duplicatas | Agrupar por chave e filtrar count > 1 | `groupBy()` + `count()` + `filter()` |
| Quantificar impacto | Calcular % de duplicatas e registros extras | Join com lista de duplicados |
| Função reutilizável | `check_uniqueness()` genérica | Aceita qualquer DataFrame + key_columns |
| Investigar tipo | Distinguir cópias exatas de parciais | `distinct()` vs contagem por chave |
| Dedup simples | Remover duplicatas sem critério | `dropDuplicates(["order_id"])` |
| Dedup controlada | Manter registro mais recente | `Window` + `row_number()` + `desc("order_date")` |
| Medir impacto financeiro | Comparar agregações antes/depois | `agg()` com `sum`, `avg`, `countDistinct` |
| Validar resultado | Confirmar count == distinct count | `assert` + `distinct().count()` |

### A Função que Você Criou

```python
check_uniqueness(df, key_columns) -> Dict
```

Essa função será reutilizada nos próximos exercícios como parte do `DataQualityFramework`.

### Conceitos-Chave

1. **Unicidade** = cada combinação de colunas-chave aparece no máximo 1 vez
2. **Duplicata exata** = todas as colunas idênticas (reprocessamento)
3. **Duplicata parcial** = chave igual mas outros campos diferem (retry, update)
4. **dropDuplicates()** = simples, não-determinístico, bom para cópias exatas
5. **row_number() + Window** = mais controle, determinístico, bom para parciais
6. **Validação pós-dedup** = sempre conferir que count == distinct count

### Impacto de Negócio

- 3% de duplicatas = 3% de inflação no faturamento
- Decisões baseadas em dados inflados → investimentos errados, comissões indevidas
- O custo de NÃO tratar duplicatas é muito maior que o custo de implementar o check

> **Carlos:** "Dois checks feitos! Completude e unicidade cobrem a maioria dos problemas mais comuns em pipelines de dados. No próximo exercício, vamos atacar a **integridade referencial** — verificar se os `product_id` e `customer_id` dos pedidos realmente existem nas tabelas de referência. Usaremos `left_anti join`, que é uma das operações mais elegantes do Spark para esse tipo de validação."

---

## Próximo Exercício

➡️ **Exercício 3 — Check de Integridade Referencial** (`03_check_integridade.md`): detecção de referências órfãs com `left_anti join` e estratégias de resolução.
