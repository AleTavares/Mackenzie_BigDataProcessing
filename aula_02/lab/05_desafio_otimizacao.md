# Exercício 5 — Desafio: Otimização de Query com Broadcast Join + Cache

## Contexto

> **Marina Silva (CTO):** "Carlos, o time de infra me mostrou os logs do Spark — nosso pipeline de relatório consolidado está demorando quase 3 minutos para processar. O Roberto quer esse relatório atualizado a cada hora, e com esse tempo de execução vamos estourar a janela. Preciso que alguém otimize isso antes de sexta."

> **Carlos Mendes (Eng. Sênior):** "Marina, eu olhei o pipeline e achei pelo menos 3 problemas graves: joins sem broadcast em tabelas pequenas, reprocessamento de dados que poderiam estar cacheados, e particionamento inadequado. Com as técnicas certas — broadcast join, cache estratégico e repartição inteligente — dá pra reduzir de minutos para segundos. Vou passar esse desafio pro time: quero ver quem consegue o maior speedup."

## Objetivos

Ao final deste exercício, você será capaz de:

- Diagnosticar gargalos de performance em pipelines Spark usando `explain()`
- Aplicar broadcast join para eliminar shuffles desnecessários
- Usar `cache()` e `persist()` estrategicamente para evitar recomputação
- Ajustar o número de partições para otimizar paralelismo
- Medir e comparar tempos de execução antes e depois das otimizações
- Combinar múltiplas técnicas de otimização em um pipeline real

## Pré-requisitos

- Exercícios 1 a 4 concluídos
- SparkSession ativa com datasets carregados:
  - `data/aula_02/vendas_2023_completo.parquet` (1M registros)
  - `data/aula_02/clientes.parquet` (500K registros)
  - `data/aula_02/categorias.json` (10 categorias)
- Jupyter Notebook acessível em http://localhost:8888

## Duração Estimada

⏱️ ~15-20 minutos

## Nível de Dificuldade

🔴 **Desafio** — Orientação mínima. Você recebe o pipeline lento e os critérios de validação, mas deve construir a solução otimizada de forma independente. Apenas dicas são fornecidas, sem solução completa.

---

## O Problema: Pipeline Lento

O pipeline abaixo simula o relatório consolidado da DataFlow Analytics. Ele está **propositalmente ineficiente** — seu objetivo é otimizá-lo e demonstrar um speedup mensurável.

### Pipeline Original (NÃO OTIMIZADO)

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, broadcast,
    dense_rank, lag, round as spark_round
)
from pyspark.sql.window import Window

# SparkSession
spark = SparkSession.builder \
    .appName("DataFlow-Desafio-Otimizacao") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Leitura dos datasets
df_vendas = spark.read.parquet("data/aula_02/vendas_2023_completo.parquet")
df_clientes = spark.read.parquet("data/aula_02/clientes.parquet")
df_categorias = spark.read.json("data/aula_02/categorias.json")

# ============================================
# PIPELINE LENTO — OTIMIZE ESTE CÓDIGO!
# ============================================

def pipeline_lento():
    """Pipeline com problemas de performance propositais."""
    
    # Problema 1: Join entre vendas (1M) e clientes (500K) SEM broadcast
    # → Gera SortMergeJoin com shuffle dos dois lados
    df_join1 = df_vendas.join(df_clientes, "customer_id", "inner")
    
    # Problema 2: Join com categorias (10 registros) SEM broadcast
    # → Gera SortMergeJoin com shuffle desnecessário
    df_join2 = df_join1.join(df_categorias, "category_id", "inner")
    
    # Problema 3: Mesma base é usada para 3 agregações diferentes
    # sem cache — recomputa o join a cada agregação
    
    # Agregação 1: Faturamento por estado
    agg_estado = df_join2 \
        .groupBy("shipping_state") \
        .agg(
            spark_sum("total_amount").alias("faturamento"),
            count("order_id").alias("num_pedidos"),
            avg("total_amount").alias("ticket_medio")
        )
    resultado_1 = agg_estado.orderBy(col("faturamento").desc())
    
    # Agregação 2: Top categorias por faturamento
    agg_categoria = df_join2 \
        .groupBy("category_name") \
        .agg(
            spark_sum("total_amount").alias("faturamento"),
            count("order_id").alias("num_pedidos")
        )
    resultado_2 = agg_categoria.orderBy(col("faturamento").desc())
    
    # Agregação 3: Ranking de clientes (window function)
    window_spec = Window.orderBy(col("total_gasto").desc())
    
    agg_clientes = df_join2 \
        .groupBy("customer_id", "customer_name") \
        .agg(spark_sum("total_amount").alias("total_gasto"))
    
    resultado_3 = agg_clientes \
        .withColumn("ranking", dense_rank().over(window_spec)) \
        .filter(col("ranking") <= 100)
    
    # Forçar materialização dos 3 resultados
    count_1 = resultado_1.count()
    count_2 = resultado_2.count()
    count_3 = resultado_3.count()
    
    return count_1, count_2, count_3


# Medir tempo do pipeline lento
inicio = time.time()
r1, r2, r3 = pipeline_lento()
tempo_lento = time.time() - inicio

print(f"⏱️ Pipeline LENTO: {tempo_lento:.2f} segundos")
print(f"   Resultados: {r1} estados, {r2} categorias, {r3} top clientes")
```

---

## Seu Desafio

Crie uma função `pipeline_otimizado()` que produza **os mesmos resultados** que o pipeline lento, mas com tempo de execução significativamente menor.

### Problemas a Resolver

| # | Problema | Técnica de Otimização |
|---|----------|----------------------|
| 1 | Join vendas×clientes gera SortMergeJoin com 2 shuffles | Avaliar se broadcast é viável (considere o tamanho do df_clientes) |
| 2 | Join com categorias (10 registros) gera shuffle desnecessário | Broadcast join — tabela pequena |
| 3 | Base joinada é recomputada 3 vezes para as 3 agregações | `cache()` ou `persist()` após o join |
| 4 | `spark.sql.shuffle.partitions = 200` pode ser excessivo para os dados | Ajustar partições para o volume real |

### O que Implementar

1. **`pipeline_otimizado()`** — versão otimizada que produz os mesmos 3 resultados
2. **Comparação de tempo** — medir e exibir o speedup obtido
3. **Análise com `explain()`** — mostrar a diferença nos planos de execução (antes vs depois)
4. **Justificativa** — para cada otimização aplicada, explique por quê ela melhora a performance

---

## Dicas

### Dica 1: Broadcast Join
- A tabela `df_categorias` tem apenas 10 registros — é candidata perfeita para broadcast
- A tabela `df_clientes` tem 500K registros — broadcast pode funcionar se couber na memória (verifique com `spark.conf.get("spark.sql.autoBroadcastJoinThreshold")`)
- Use `from pyspark.sql.functions import broadcast`
- Verifique no `explain()` se `BroadcastHashJoin` substituiu `SortMergeJoin`

### Dica 2: Cache Estratégico
- Se o mesmo DataFrame é usado múltiplas vezes, `cache()` evita recomputação
- Coloque o `.cache()` **depois** dos joins e **antes** das agregações
- Force a materialização do cache com um `.count()` antes das agregações
- Lembre-se de `unpersist()` ao final para liberar memória

### Dica 3: Particionamento
- O padrão `spark.sql.shuffle.partitions = 200` é para clusters grandes
- Para volumes menores (1M registros em ambiente local/lab), 20-50 partições pode ser mais eficiente
- Menos partições = menos overhead de scheduling e menos arquivos intermediários
- Use `spark.conf.set("spark.sql.shuffle.partitions", "N")` para ajustar

### Dica 4: Medição Justa
- Execute cada pipeline 3 vezes e use a **média** (descarte a primeira execução se quiser — warm-up)
- Certifique-se de que o cache do pipeline lento não está ativo ao medir (use `spark.catalog.clearCache()`)
- Compare contagens dos resultados para garantir equivalência

---

## Formato Esperado da Saída

```
============================================
🏁 RESULTADO DO DESAFIO DE OTIMIZAÇÃO
============================================

⏱️ Pipeline LENTO:     XX.XX segundos (média 3 execuções)
⏱️ Pipeline OTIMIZADO: XX.XX segundos (média 3 execuções)

📊 Speedup: X.Xx mais rápido

✅ Validação de resultados:
   Estados:   LENTO={N} | OTIMIZADO={N} | Match: ✅
   Categorias: LENTO={N} | OTIMIZADO={N} | Match: ✅
   Top Clientes: LENTO={N} | OTIMIZADO={N} | Match: ✅

🔧 Otimizações aplicadas:
   1. [descreva sua otimização 1]
   2. [descreva sua otimização 2]
   3. [descreva sua otimização 3]
   4. [descreva sua otimização 4]

📋 Plano de execução (join principal):
   ANTES: SortMergeJoin + N Exchange(s)
   DEPOIS: BroadcastHashJoin + N Exchange(s)
============================================
```

---

## Critérios de Validação

Para considerar o desafio completo, seu pipeline otimizado deve atender a **todos** os critérios:

| Critério | Requisito |
|----------|-----------|
| **Corretude** | Os 3 resultados (contagens) devem ser idênticos ao pipeline lento |
| **Speedup** | Pipeline otimizado deve ser pelo menos **2x mais rápido** |
| **Broadcast** | `explain()` deve mostrar `BroadcastHashJoin` (pelo menos no join de categorias) |
| **Cache** | DataFrame joinado deve ser cacheado e reutilizado (não recomputado 3x) |
| **Partições** | `spark.sql.shuffle.partitions` deve ser ajustado para valor < 200 |
| **Justificativa** | Cada otimização deve ter explicação do porquê melhora a performance |

---

## Bônus (para quem quer ir além)

- **Bônus 1:** Experimente `persist(StorageLevel.MEMORY_AND_DISK)` vs `cache()` — qual a diferença?
- **Bônus 2:** Use `repartition()` ou `coalesce()` após o join para controlar partições do DataFrame (não apenas do shuffle)
- **Bônus 3:** Adicione `predicate pushdown` — filtre por `shipping_state = 'SP'` antes dos joins e compare o plano
- **Bônus 4:** Tente `df_vendas.repartition("customer_id")` antes do join — isso ajuda ou piora? Por quê?

---

## Reflexão Final

> **Carlos Mendes:** "Em produção na DataFlow, otimização não é luxo — é necessidade. Um pipeline que roda em 3 minutos pode parecer OK quando executa uma vez por dia. Mas quando o Roberto pediu atualização a cada hora, 3 minutos virou inaceitável. As mesmas técnicas que vocês usaram aqui — broadcast para tabelas pequenas, cache para reuso, particionamento adequado — são as que aplicamos diariamente no cluster real. O `explain()` é seu melhor amigo: nunca coloque um pipeline em produção sem verificar o plano de execução."

---

## Referências

- [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Broadcast Join Hints](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-hints.html)
- [Caching and Persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Shuffle Partition Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options)
