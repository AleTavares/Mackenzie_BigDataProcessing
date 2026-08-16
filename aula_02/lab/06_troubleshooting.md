# Troubleshooting — Aula 2: Problemas Comuns em Joins, Window Functions e UDFs

## Contexto

> **Marina Silva (Arquiteta de Dados):** "Joins e UDFs são onde a maioria dos problemas de performance e erros misteriosos aparecem no Spark. Depois de investigar centenas de jobs lentos na DataFlow Analytics, posso dizer que 80% dos casos se resumem a: dados duplicados em joins, serialização de UDFs, ou falta de broadcast. Vou mostrar como diagnosticar e resolver cada um."

---

## 1. OutOfMemoryError em Joins

**Sintoma:**
```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.joins.SortMergeJoinExec
```
Ou:
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
  at org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
```
Container morre com status "Exited (137)" durante operação de join.

**Causa:** O Spark está tentando realizar um join entre DataFrames grandes sem otimização adequada. Casos mais comuns:
- Cross join (produto cartesiano) acidental ou intencional em DataFrames grandes
- Join sem broadcast quando um dos lados é pequeno o suficiente para caber na memória
- Data skew — uma chave de join concentra milhões de registros em uma única partição

**Solução:**
```python
# ❌ PROBLEMA — cross join em DataFrames grandes (explode a memória):
df_resultado = df_pedidos.crossJoin(df_produtos)  # 1M x 10K = 10 BILHÕES de linhas!

# ✅ SOLUÇÃO 1 — Usar broadcast para o DataFrame menor:
from pyspark.sql.functions import broadcast

df_resultado = df_pedidos.join(
    broadcast(df_produtos),  # Força broadcast do DF menor
    on="product_id",
    how="inner"
)

# ✅ SOLUÇÃO 2 — Aumentar memória e limitar cross join:
spark.conf.set("spark.sql.crossJoin.enabled", "true")  # Exige confirmação explícita
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.executor.memory", "4g")

# ✅ SOLUÇÃO 3 — Para cross joins necessários, filtrar ANTES:
df_produtos_categoria = df_produtos.filter(col("categoria") == "Eletrônicos")
df_resultado = df_pedidos.crossJoin(broadcast(df_produtos_categoria))

# ✅ SOLUÇÃO 4 — Resolver data skew com salting:
from pyspark.sql.functions import lit, rand, floor, concat

# Adicionar salt key para distribuir melhor
num_salts = 10
df_pedidos_salt = df_pedidos.withColumn("salt", floor(rand() * num_salts))
df_produtos_exploded = df_produtos.crossJoin(
    spark.range(num_salts).withColumnRenamed("id", "salt")
)
df_resultado = df_pedidos_salt.join(
    df_produtos_exploded,
    on=["product_id", "salt"],
    how="inner"
).drop("salt")
```

**Prevenção:**
- Sempre use `broadcast()` quando um dos DataFrames tem menos de 100MB
- Evite cross joins — se necessário, filtre antes para reduzir o tamanho
- Monitore o Spark UI (http://localhost:4040) para ver se há partições desproporcionais
- Configure `spark.sql.autoBroadcastJoinThreshold` para ajustar o limiar automático

---

## 2. UDF Serialization Errors (PicklingError)

**Sintoma:**
```
PicklingError: Could not serialize object:
  Exception: It appears that you are attempting to reference SparkContext
  from a broadcast variable, action, or transformation.
```
Ou:
```
_pickle.PicklingError: Can't pickle <class 'module'>: attribute lookup module on builtins failed
```
Ou:
```
Py4JError: An error occurred while calling o72.udf.
  java.io.NotSerializableException: org.apache.spark.sql.SparkSession
```

**Causa:** UDFs no PySpark são serializadas e enviadas para os executores. Se a função referencia objetos não serializáveis (SparkSession, conexões de banco, módulos inteiros, ou objetos complexos), a serialização falha.

**Solução:**
```python
# ❌ PROBLEMA — referenciando SparkSession dentro da UDF:
def calcular_desconto(valor):
    config = spark.conf.get("app.desconto")  # ← NÃO SERIALIZA!
    return valor * (1 - float(config))

# ❌ PROBLEMA — referenciando módulo ou objeto externo complexo:
import pandas as pd
conexao = criar_conexao_banco()  # Objeto não-serializável

def enriquecer(valor):
    resultado = conexao.query(valor)  # ← ERRO!
    return resultado

# ✅ SOLUÇÃO 1 — Capturar valores simples em variáveis locais:
desconto_valor = float(spark.conf.get("app.desconto", "0.1"))

@udf(returnType=DoubleType())
def calcular_desconto(valor):
    return valor * (1 - desconto_valor)  # ← variável primitiva, OK!

# ✅ SOLUÇÃO 2 — Usar broadcast variables para dados de referência:
tabela_descontos = {"premium": 0.2, "standard": 0.1, "basic": 0.05}
bc_descontos = spark.sparkContext.broadcast(tabela_descontos)

@udf(returnType=DoubleType())
def aplicar_desconto(valor, tipo_cliente):
    desc = bc_descontos.value.get(tipo_cliente, 0.0)
    return valor * (1 - desc)

# ✅ SOLUÇÃO 3 — Manter UDFs como funções puras (sem dependências externas):
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def classificar_valor(valor):
    if valor > 1000:
        return "alto"
    elif valor > 100:
        return "medio"
    else:
        return "baixo"

df_resultado = df_pedidos.withColumn("faixa", classificar_valor(col("total_amount")))
```

**Prevenção:**
- UDFs devem ser **funções puras**: recebem valores, retornam valores, sem efeitos colaterais
- Nunca referencie `spark`, `sc`, conexões, ou objetos não-serializáveis dentro de UDFs
- Use `broadcast()` para enviar dados de lookup aos executores
- Teste sua UDF localmente com valores simples antes de aplicar no DataFrame

---

## 3. Join Retorna Número Inesperado de Linhas

**Sintoma:**
```python
print(f"Pedidos: {df_pedidos.count()}")       # 10.000
print(f"Clientes: {df_clientes.count()}")     # 5.000
df_join = df_pedidos.join(df_clientes, on="customer_id", how="inner")
print(f"Resultado: {df_join.count()}")        # 25.000 ← MAIS que o esperado!
```
Ou o oposto — resultado tem MENOS linhas que o esperado.

**Causa:**
- **Mais linhas que o esperado:** Chaves duplicadas em um ou ambos os lados do join criam produto cartesiano parcial. Se `customer_id` aparece 5 vezes em `df_clientes`, cada pedido será multiplicado por 5.
- **Menos linhas que o esperado:** Inner join descarta linhas sem correspondência. Se `customer_id` de um pedido não existe em `df_clientes`, a linha é perdida.

**Solução:**
```python
# DIAGNÓSTICO — Verificar duplicatas na chave de join:
print("Duplicatas em clientes:")
df_clientes.groupBy("customer_id").count() \
    .filter(col("count") > 1) \
    .orderBy(col("count").desc()) \
    .show(10)

# DIAGNÓSTICO — Verificar quantos registros não terão match:
df_sem_match = df_pedidos.join(df_clientes, on="customer_id", how="left_anti")
print(f"Pedidos sem cliente correspondente: {df_sem_match.count()}")

# ✅ SOLUÇÃO para duplicatas — deduplicar ANTES do join:
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_dedup = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
df_clientes_dedup = df_clientes \
    .withColumn("rn", row_number().over(window_dedup)) \
    .filter(col("rn") == 1) \
    .drop("rn")

df_join = df_pedidos.join(df_clientes_dedup, on="customer_id", how="inner")

# ✅ SOLUÇÃO para linhas perdidas — usar left join:
df_join = df_pedidos.join(df_clientes, on="customer_id", how="left")
# Linhas de pedidos sem cliente terão NULL nas colunas de clientes
```

**Prevenção:**
- Sempre verifique unicidade das chaves de join: `df.groupBy("key").count().filter(col("count") > 1)`
- Escolha o tipo de join adequado: `inner` descarta, `left` preserva o lado esquerdo
- Use `left_anti` para diagnosticar registros que serão perdidos antes de executar o join final
- Documente a cardinalidade esperada: 1:1, 1:N, ou N:M

---

## 4. Window Function Retorna Resultados Inesperados

**Sintoma:**
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank

window = Window.partitionBy("department").orderBy("salary")
df_ranked = df_employees.withColumn("rank", rank().over(window))

# Resultado: todos os employees recebem rank = 1
# Ou: ranking parece aleatório/não respeita a ordenação esperada
```

**Causa:**
- `partitionBy` ou `orderBy` usando coluna errada ou com valores NULL
- Confusão entre `row_number()`, `rank()` e `dense_rank()` para empates
- Falta de `orderBy` na window (resultado não-determinístico)
- NULLs são tratados como um grupo separado no `partitionBy`

**Solução:**
```python
# DIAGNÓSTICO — Verificar NULLs na coluna de partição/ordenação:
from pyspark.sql.functions import isnull, count, when

df_employees.select(
    count(when(isnull("department"), True)).alias("nulls_department"),
    count(when(isnull("salary"), True)).alias("nulls_salary")
).show()

# ✅ SOLUÇÃO 1 — Tratar NULLs antes de aplicar window:
df_clean = df_employees.filter(col("department").isNotNull())

window = Window.partitionBy("department").orderBy(col("salary").desc())
df_ranked = df_clean.withColumn("rank", row_number().over(window))

# ✅ SOLUÇÃO 2 — Controlar ordenação de NULLs:
from pyspark.sql.functions import asc_nulls_last, desc_nulls_last

window = Window.partitionBy("department").orderBy(desc_nulls_last("salary"))
df_ranked = df_employees.withColumn("rank", row_number().over(window))

# ✅ SOLUÇÃO 3 — Escolher a função correta para o caso de uso:
window = Window.partitionBy("department").orderBy(col("salary").desc())

df_comparacao = df_employees \
    .withColumn("row_number", row_number().over(window)) \
    .withColumn("rank", rank().over(window)) \
    .withColumn("dense_rank", dense_rank().over(window))

# Para salários [5000, 5000, 3000]:
# row_number: 1, 2, 3  (sempre único, desempate arbitrário)
# rank:       1, 1, 3  (empate recebe mesmo rank, pula posição)
# dense_rank: 1, 1, 2  (empate recebe mesmo rank, NÃO pula)

# ✅ SOLUÇÃO 4 — Definir frame explícito para funções de agregação:
window_sum = Window.partitionBy("department") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_acumulado = df_vendas.withColumn(
    "total_acumulado",
    sum("valor").over(window_sum)
)
```

**Prevenção:**
- Sempre inclua `orderBy()` na window spec — sem ela, o resultado é não-determinístico
- Verifique NULLs nas colunas usadas em `partitionBy` e `orderBy`
- Use `row_number()` para ranking único, `rank()` para empates com gap, `dense_rank()` sem gap
- Para somas/médias acumuladas, defina o frame explicitamente com `rowsBetween()`

---

## 5. Saída do explain() Confusa

**Sintoma:**
```python
df_resultado.explain()
# Output enorme e incompreensível:
# == Physical Plan ==
# AdaptiveSparkPlan isFinalPlan=false
# +- BroadcastHashJoin [product_id#12], [product_id#45], Inner, BuildRight, false
#    :- Filter isnotnull(product_id#12)
#    :  +- FileScan csv [order_id#10,product_id#12,quantity#13]
#    +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=123]
#       +- Filter isnotnull(product_id#45)
#          +- FileScan csv [product_id#45,product_name#46,category#47]
```

**Causa:** O plano de execução do Spark usa terminologia específica que não é intuitiva para iniciantes. Sem entender os operadores, é difícil otimizar queries.

**Solução:**
```python
# ✅ Usar explain com modo "formatted" para saída mais legível:
df_resultado.explain(mode="formatted")

# ✅ Usar explain com modo "simple" para visão geral:
df_resultado.explain(mode="simple")

# ✅ Usar explain(True) para ver planos lógico + físico:
df_resultado.explain(True)
```

**Guia de leitura do plano físico:**

| Operador | Significado | O que observar |
|----------|-------------|----------------|
| `FileScan` | Leitura de arquivo | Quais colunas são lidas (column pruning) |
| `Filter` | Filtro aplicado | Se `isnotnull` aparece, o Spark otimizou NULL handling |
| `BroadcastHashJoin` | Join com broadcast | ✅ Bom para tabelas pequenas — rápido |
| `SortMergeJoin` | Join com sort+merge | ⚠️ Requer shuffle — mais lento |
| `Exchange` / `ShuffleExchange` | Redistribuição de dados | 🔴 Indica shuffle pela rede — ponto de atenção |
| `HashAggregate` | Agregação com hash | Operação de groupBy |
| `Sort` | Ordenação | Necessário para SortMergeJoin e window functions |
| `Project` | Projeção de colunas | Select de colunas específicas |
| `BroadcastExchange` | Envio de dados para broadcast | Precede BroadcastHashJoin |
| `Window` | Window function | Inclui partitioning e ordering |
| `BatchEvalPython` | Execução de UDF Python | 🔴 Indicador de UDF — possível gargalo |

```python
# ✅ DICA — ler de baixo para cima (das folhas para a raiz):
# O plano é uma árvore:
# - Folhas (FileScan) = leitura de dados
# - Nós intermediários = transformações
# - Raiz (topo) = resultado final

# ✅ Exemplo de análise:
df_analise = df_pedidos.join(broadcast(df_produtos), on="product_id") \
    .groupBy("categoria") \
    .agg(sum("total_amount").alias("total"))

df_analise.explain(mode="formatted")
# Leitura: FileScan → Filter (nulls) → BroadcastExchange → BroadcastHashJoin → HashAggregate
# ✅ Sem ShuffleExchange no join = broadcast funcionou!
```

**Prevenção:**
- Use `explain(mode="formatted")` — é mais legível que o padrão
- Procure por `Exchange`/`ShuffleExchange` — cada um é um ponto de shuffle (potencialmente lento)
- `BroadcastHashJoin` é preferível a `SortMergeJoin` para joins com tabela pequena
- `BatchEvalPython` indica UDF Python — considere substituir por funções nativas do Spark

---

## 6. BroadcastHashJoin Não Está Sendo Usado

**Sintoma:**
```python
# Você espera broadcast, mas o explain mostra SortMergeJoin:
df_pedidos.join(df_produtos, on="product_id").explain()
# == Physical Plan ==
# SortMergeJoin [product_id], [product_id], Inner
#    :- Sort [product_id ASC]
#    :  +- Exchange hashpartitioning(product_id, 200)  ← SHUFFLE!
#    :     +- FileScan csv ...
#    +- Sort [product_id ASC]
#       +- Exchange hashpartitioning(product_id, 200)  ← SHUFFLE!
#          +- FileScan csv ...
```

**Causa:** O Spark automaticamente usa broadcast quando um DataFrame é menor que `spark.sql.autoBroadcastJoinThreshold` (padrão: 10MB). Se o DataFrame excede esse limite, o Spark usa SortMergeJoin com shuffle.

**Solução:**
```python
# ✅ SOLUÇÃO 1 — Forçar broadcast explicitamente:
from pyspark.sql.functions import broadcast

df_resultado = df_pedidos.join(
    broadcast(df_produtos),  # Força broadcast independente do tamanho
    on="product_id",
    how="inner"
)

# Verificar:
df_resultado.explain()
# Deve mostrar: BroadcastHashJoin + BroadcastExchange

# ✅ SOLUÇÃO 2 — Aumentar o threshold de auto-broadcast:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100m")  # 100MB

# ✅ SOLUÇÃO 3 — Verificar tamanho real do DataFrame:
# O Spark calcula o tamanho estimado a partir das estatísticas do catálogo
# Para CSVs sem estatísticas, a estimativa pode ser imprecisa
df_produtos.cache()
df_produtos.count()  # Força materialização
# Agora o Spark tem estatísticas reais para decidir broadcast

# ⚠️ CUIDADO — NÃO force broadcast em DataFrames grandes:
# Se o DataFrame > memória do executor, o broadcast causa OOM!
# Verifique antes:
tamanho_bytes = spark.catalog.isCached("produtos")
print(f"Tamanho estimado: {df_produtos.count()} linhas")
```

**Prevenção:**
- Conheça o tamanho dos seus DataFrames — `broadcast()` só funciona se o DF cabe na memória
- Para tabelas de dimensão (produtos, clientes, categorias) < 100MB: sempre use broadcast
- Para tabelas de fato (pedidos, transações, logs): nunca faça broadcast
- Verifique com `explain()` se o broadcast está sendo aplicado

---

## 7. Performance Lenta Após Adicionar UDFs

**Sintoma:**
```python
# Antes da UDF: job completa em 2 segundos
df_resultado = df_pedidos.groupBy("categoria").agg(sum("total").alias("soma"))

# Depois da UDF: job demora 45+ segundos
@udf(returnType=StringType())
def classificar(valor):
    if valor > 1000: return "premium"
    elif valor > 100: return "standard"
    else: return "basic"

df_resultado = df_pedidos.withColumn("classe", classificar(col("total")))
# explain() mostra: BatchEvalPython ← gargalo!
```

**Causa:** UDFs Python quebram as otimizações do Catalyst Optimizer. Cada linha é serializada Java→Python, processada, e serializada de volta Python→Java. Esse overhead de serialização é enorme (10x-100x mais lento que funções nativas).

**Solução:**
```python
# ✅ SOLUÇÃO 1 — Substituir UDF por funções nativas do Spark:
from pyspark.sql.functions import when

# Equivalente à UDF acima, mas 10-100x mais rápido:
df_resultado = df_pedidos.withColumn(
    "classe",
    when(col("total") > 1000, "premium")
    .when(col("total") > 100, "standard")
    .otherwise("basic")
)

# ✅ SOLUÇÃO 2 — Usar Pandas UDF (vectorized) se UDF é inevitável:
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def classificar_pandas(valores: pd.Series) -> pd.Series:
    return valores.apply(
        lambda v: "premium" if v > 1000 else ("standard" if v > 100 else "basic")
    )

# Pandas UDF processa em batches (vetorizado) — muito mais rápido que UDF regular
df_resultado = df_pedidos.withColumn("classe", classificar_pandas(col("total")))

# ✅ SOLUÇÃO 3 — Para lógica complexa, usar expressões SQL:
df_pedidos.createOrReplaceTempView("pedidos")
df_resultado = spark.sql("""
    SELECT *,
        CASE
            WHEN total > 1000 THEN 'premium'
            WHEN total > 100 THEN 'standard'
            ELSE 'basic'
        END AS classe
    FROM pedidos
""")

# ✅ Comparar planos de execução:
df_com_udf.explain()      # Mostra BatchEvalPython 🔴
df_com_when.explain()     # Sem BatchEvalPython ✅
df_com_pandas_udf.explain()  # Mostra ArrowEvalPython (mais eficiente que BatchEval)
```

**Prevenção:**
- Regra de ouro: **sempre tente usar funções nativas do Spark primeiro**
- `when/otherwise`, `regexp_extract`, `split`, `concat` cobrem 90% dos casos
- Se precisa de UDF, prefira Pandas UDF (vectorized) — até 100x mais rápido
- Use `explain()` para verificar se `BatchEvalPython` aparece — se sim, há UDF no caminho

---

## 8. Cache Não Funciona Como Esperado

**Sintoma:**
```python
df_resultado = df_pedidos.join(df_clientes, on="customer_id")
df_resultado.cache()

# Primeira execução: 30 segundos (esperado)
df_resultado.show()

# Segunda execução: AINDA demora 30 segundos! (deveria ser instantâneo)
df_resultado.groupBy("cidade").count().show()
```
Ou no Storage tab do Spark UI: o cache mostra "0% cached".

**Causa:**
- `cache()` é **lazy** — só materializa quando uma action é executada
- Transformações adicionais APÓS o cache criam um novo DataFrame (não cacheado)
- O DataFrame original pode ter sido invalidado por operações subsequentes
- Memória insuficiente para armazenar o cache completo

**Solução:**
```python
# ❌ PROBLEMA — cache sem forçar materialização:
df_resultado = df_pedidos.join(df_clientes, on="customer_id")
df_resultado.cache()
# Neste ponto, NADA foi cacheado ainda! É só uma marcação.

# ✅ SOLUÇÃO 1 — Forçar materialização após cache:
df_resultado = df_pedidos.join(df_clientes, on="customer_id")
df_resultado.cache()
df_resultado.count()  # ← FORÇA o cache a ser preenchido
# Agora operações subsequentes usam o cache:
df_resultado.groupBy("cidade").count().show()  # ← rápido!

# ✅ SOLUÇÃO 2 — Usar persist() com nível de storage adequado:
from pyspark import StorageLevel

df_resultado.persist(StorageLevel.MEMORY_AND_DISK)
df_resultado.count()  # Materializa — se não cabe em RAM, vai para disco

# ✅ SOLUÇÃO 3 — Garantir que está usando o mesmo DataFrame:
# ❌ ERRADO — cria novo DataFrame, cache não se aplica:
df_resultado.cache()
df_novo = df_resultado.filter(col("valor") > 100)  # df_novo NÃO está cacheado!

# ✅ CORRETO — aplicar transformações e DEPOIS cachear o resultado final:
df_final = df_pedidos.join(df_clientes, on="customer_id") \
    .filter(col("valor") > 100)
df_final.cache()
df_final.count()  # Materializa o cache

# ✅ SOLUÇÃO 4 — Liberar cache quando não precisa mais:
df_resultado.unpersist()  # Libera memória

# ✅ VERIFICAÇÃO — checar se está cacheado:
print(df_resultado.is_cached)  # True/False
# Ou verificar no Spark UI → Storage tab
```

**Prevenção:**
- Sempre execute uma action (`count()`, `show()`) logo após `cache()` para materializar
- Cache apenas DataFrames que serão reutilizados múltiplas vezes
- Libere o cache com `unpersist()` quando não precisar mais
- Monitore o Storage tab no Spark UI (localhost:4040) para confirmar que o cache está ativo
- Não cacheie DataFrames que cabem na memória e são rápidos de recomputar

---

## Quick Reference: Tabela de Diagnóstico Rápido

| Sintoma | Causa Provável | Solução Rápida |
|---------|---------------|----------------|
| `OutOfMemoryError` durante join | Cross join ou join sem broadcast em DFs grandes | Usar `broadcast()` no DF menor |
| `PicklingError` / `NotSerializableException` | UDF referencia objeto não-serializável (spark, conexão) | Remover referências externas, usar broadcast variables |
| Join retorna mais linhas que o esperado | Chaves duplicadas (join N:M) | Deduplicar com `row_number()` antes do join |
| Join retorna menos linhas que o esperado | Inner join descarta não-matches | Usar `left` join ou verificar com `left_anti` |
| Window function retorna todos rank = 1 | Todos valores iguais na coluna de `orderBy` | Verificar coluna de ordenação, adicionar desempate |
| Window function resultado aleatório | Falta `orderBy` na window spec | Adicionar `orderBy()` explícito |
| `explain()` mostra `Exchange`/`ShuffleExchange` | Shuffle desnecessário | Usar `broadcast()` ou reparticionar pela chave de join |
| `SortMergeJoin` ao invés de `BroadcastHashJoin` | DF excede threshold de auto-broadcast (10MB) | Forçar `broadcast()` ou aumentar threshold |
| UDF 10-100x mais lenta que esperado | `BatchEvalPython` — serialização Java↔Python | Substituir por funções nativas (`when`, `regexp_extract`) |
| Cache não acelera segunda execução | `cache()` é lazy — não materializou | Executar `count()` após `cache()` |
| `BatchEvalPython` no explain | UDF Python no pipeline | Usar Pandas UDF ou funções nativas |
| `GC overhead limit exceeded` | Muitos objetos pequenos na heap | Aumentar memória ou reduzir partições |

---

## Quick Reference: Comandos de Debugging para Aula 02

### Diagnosticar Joins

```python
# Verificar duplicatas na chave de join
df.groupBy("join_key").count().filter(col("count") > 1).show()

# Ver quantos registros não terão match
df_left.join(df_right, on="key", how="left_anti").count()

# Verificar tipo de join usado
df_left.join(df_right, on="key").explain()
```

### Diagnosticar Window Functions

```python
# Verificar NULLs nas colunas de window
df.select([count(when(isnull(c), c)).alias(c) for c in ["partition_col", "order_col"]]).show()

# Ver valores distintos na coluna de partição
df.select("partition_col").distinct().show()

# Testar window em subset pequeno
df.filter(col("partition_col") == "valor_teste").show()
```

### Diagnosticar UDFs

```python
# Verificar se UDF aparece no plano
df_com_udf.explain()  # Procurar por "BatchEvalPython" ou "ArrowEvalPython"

# Testar UDF isoladamente (fora do Spark)
resultado = minha_udf_func("valor_teste")
print(resultado)  # Deve funcionar sem erros

# Comparar tempo com vs sem UDF
import time
start = time.time()
df_sem_udf.count()
print(f"Sem UDF: {time.time() - start:.2f}s")

start = time.time()
df_com_udf.count()
print(f"Com UDF: {time.time() - start:.2f}s")
```

### Diagnosticar Cache

```python
# Verificar se DataFrame está cacheado
print(f"Cacheado: {df.is_cached}")

# Ver uso de storage
spark.catalog.clearCache()  # Limpar todo o cache

# Listar todos DataFrames cacheados (via Spark UI)
# http://localhost:4040 → Storage tab
```

---

> **Marina:** "A dica mais importante que posso dar: sempre execute `explain()` antes de rodar um job em produção. Em 30 segundos de análise do plano, você identifica 90% dos problemas de performance — shuffles desnecessários, UDFs no caminho crítico, e joins sem broadcast. É o raio-X do seu pipeline."
