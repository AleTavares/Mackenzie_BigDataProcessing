# Exercício 3 — Implementar Escrita Idempotente (Overwrite por Partição)

## Duração Estimada

⏱️ ~15 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, na semana passada o pipeline falhou às 4h da manhã por timeout no storage. O SRE fez retry manual às 7h. Quando fui conferir, tinha dados duplicados na Gold — o relatório do Roberto mostrava faturamento 2x maior que a realidade. Precisamos garantir que rodar o pipeline de novo **nunca** duplique dados."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Esse é o conceito de **idempotência**: executar a mesma operação N vezes produz o mesmo resultado que executar 1 vez. A solução no Spark é usar `mode('overwrite')` com `partitionBy` e a configuração `partitionOverwriteMode=dynamic`. Assim só a partição do dia é sobrescrita — outros dias ficam intactos."

> **Marina Silva (CTO):** "Perfeito. E durante backfill? Se precisarmos reprocessar os últimos 7 dias, cada dia tem que sobrescrever só sua partição sem afetar os outros."

> **Carlos Mendes:** "Exatamente. Dynamic partition overwrite faz exatamente isso."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender o que é idempotência e por que ela é essencial em pipelines de dados
- Identificar o problema de duplicação causado por `mode("append")`
- Configurar `partitionOverwriteMode=dynamic` no Spark
- Verificar que dados de outros dias permanecem intactos após reprocessamento
- Provar idempotência: executar o pipeline 2x com mesma data e verificar contagem idêntica
- Aplicar boas práticas para transformações determinísticas

## Pré-requisitos

- Exercícios 01 e 02 concluídos (`pipeline_vendas.py` com CLI e logging)
- Ambiente Docker rodando (Spark + Jupyter)
- Dataset `datasets/aula_07/producao/` disponível
- Terminal com acesso ao container Spark

---

## Passo 1: Entender o Problema — Por que Append Duplica Dados

**Descrição:** Antes da solução, vamos entender o problema. Quando usamos `mode("append")`, cada execução **adiciona** registros ao destino. Se o pipeline falha no meio e é reexecutado, a parte que já foi escrita aparece duplicada.

**Cenário do problema:**

```
┌──────────────────────────────────────────────────────────────────────┐
│  PROBLEMA: mode("append") + retry = dados duplicados                  │
│                                                                       │
│  1ª execução (06:00):                                                │
│     Bronze ✅ (1000 registros escritos)                               │
│     Silver ✅ (950 registros escritos)                                │
│     Gold   ❌ (falha por timeout)                                    │
│                                                                       │
│  2ª execução — retry (07:00):                                        │
│     Bronze ✅ (1000 registros ADICIONADOS → agora tem 2000!)         │
│     Silver ✅ (950 registros ADICIONADOS → agora tem 1900!)          │
│     Gold   ✅ (processa 1900 → métricas erradas!)                    │
│                                                                       │
│  Resultado: dados duplicados, métricas infladas                       │
└──────────────────────────────────────────────────────────────────────┘
```

**Abra o PySpark Shell para demonstrar:**

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark
```

**Simulação do problema com append:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .appName("demo-idempotencia") \
    .getOrCreate()

# Criar dados de exemplo (simulando vendas de 2024-01-15)
dados = [(1, "Produto A", 100.0), (2, "Produto B", 200.0), (3, "Produto C", 150.0)]
df = spark.createDataFrame(dados, ["order_id", "produto", "valor"])
df = df.withColumn("data_ref", lit("2024-01-15"))

# ❌ PROBLEMA: Escrita com append
caminho = "/tmp/demo_append"
df.write.mode("append").partitionBy("data_ref").parquet(caminho)

# Verificar: 3 registros
print(f"Após 1ª execução: {spark.read.parquet(caminho).count()} registros")

# Simular retry — executar de novo
df.write.mode("append").partitionBy("data_ref").parquet(caminho)

# Verificar: 6 registros! (duplicou!)
print(f"Após 2ª execução (retry): {spark.read.parquet(caminho).count()} registros")
```

**Saída esperada:**

```
Após 1ª execução: 3 registros
Após 2ª execução (retry): 6 registros
```

> **Carlos:** "Viu? O `append` adicionou os 3 registros de novo. Em produção com 50 mil registros/dia, isso é catastrófico — relatórios de faturamento dobram da noite pro dia."

---

## Passo 2: A Solução — Overwrite por Partição com Dynamic Mode

**Descrição:** A solução é usar `mode("overwrite")` combinado com `partitionBy` e `partitionOverwriteMode=dynamic`. Essa configuração diz ao Spark: "sobrescreva **somente** as partições que estão no DataFrame, deixe as outras intactas."

**Conceito visual:**

```
┌──────────────────────────────────────────────────────────────────────┐
│  SOLUÇÃO: mode("overwrite") + partitionBy + dynamic                   │
│                                                                       │
│  Configuração:                                                        │
│    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")│
│                                                                       │
│  Datalake ANTES do pipeline (dias 13, 14 já processados):            │
│    silver/vendas/                                                      │
│    ├── data_ref=2024-01-13/  (800 registros)  ← INTACTO              │
│    ├── data_ref=2024-01-14/  (900 registros)  ← INTACTO              │
│    └── data_ref=2024-01-15/  (950 registros)  ← SOBRESCRITO          │
│                                                                       │
│  1ª execução para data_ref=2024-01-15:                               │
│    → Sobrescreve APENAS data_ref=2024-01-15 (950 registros)          │
│                                                                       │
│  2ª execução (retry) para data_ref=2024-01-15:                       │
│    → Sobrescreve APENAS data_ref=2024-01-15 (950 registros)          │
│    → Resultado: ainda 950 registros (idempotente!)                   │
│    → Dias 13 e 14: inalterados                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**Demonstração — A solução correta:**

```python
# ✅ SOLUÇÃO: Configurar dynamic partition overwrite
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Limpar demo anterior
import shutil
shutil.rmtree("/tmp/demo_dynamic", ignore_errors=True)

# Criar dados de dois dias diferentes
dados_dia_14 = [(10, "X", 500.0), (11, "Y", 600.0)]
dados_dia_15 = [(1, "A", 100.0), (2, "B", 200.0), (3, "C", 150.0)]

df_14 = spark.createDataFrame(dados_dia_14, ["order_id", "produto", "valor"]) \
    .withColumn("data_ref", lit("2024-01-14"))
df_15 = spark.createDataFrame(dados_dia_15, ["order_id", "produto", "valor"]) \
    .withColumn("data_ref", lit("2024-01-15"))

caminho = "/tmp/demo_dynamic"

# Escrever dia 14 primeiro
df_14.write.mode("overwrite").partitionBy("data_ref").parquet(caminho)
print(f"Após escrever dia 14: {spark.read.parquet(caminho).count()} registros")

# Escrever dia 15 (overwrite dynamic — NÃO apaga dia 14!)
df_15.write.mode("overwrite").partitionBy("data_ref").parquet(caminho)
print(f"Após escrever dia 15: {spark.read.parquet(caminho).count()} registros")

# Verificar: dia 14 continua intacto
df_check = spark.read.parquet(caminho)
df_check.groupBy("data_ref").count().orderBy("data_ref").show()
```

**Saída esperada:**

```
Após escrever dia 14: 2 registros
Após escrever dia 15: 5 registros

+----------+-----+
|  data_ref|count|
+----------+-----+
|2024-01-14|    2|
|2024-01-15|    3|
+----------+-----+
```

> **Carlos:** "Perceba: ao escrever o dia 15, o dia 14 ficou intacto. Isso é o `partitionOverwriteMode=dynamic` em ação — ele identifica quais partições estão no DataFrame e sobrescreve **somente** essas."

---

## Passo 3: Entender Static vs Dynamic — O Perigo do Modo Padrão

**Descrição:** O Spark tem dois modos de partition overwrite. O **static** (padrão) apaga TODAS as partições existentes e reescreve. O **dynamic** apaga apenas as partições presentes no DataFrame. Se você esquecer de configurar `dynamic`, perde dados de outros dias!

**Comparação:**

| Modo | Comportamento | Risco |
|------|---------------|-------|
| `static` (padrão) | Apaga **todas** as partições → escreve | Perde dados de outros dias! |
| `dynamic` | Apaga **só** partições do DataFrame → escreve | Seguro para produção |

**Demonstração do perigo do modo static:**

```python
# ⚠️ PERIGO: Modo STATIC (padrão do Spark)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

import shutil
shutil.rmtree("/tmp/demo_static", ignore_errors=True)

caminho = "/tmp/demo_static"

# Escrever dias 13, 14 e 15
from pyspark.sql.functions import lit

for dia in ["2024-01-13", "2024-01-14", "2024-01-15"]:
    dados = [(i, f"Produto-{i}", 100.0 * i) for i in range(1, 4)]
    df = spark.createDataFrame(dados, ["order_id", "produto", "valor"]) \
        .withColumn("data_ref", lit(dia))
    df.write.mode("append").partitionBy("data_ref").parquet(caminho)

print("ANTES — 3 dias de dados:")
spark.read.parquet(caminho).groupBy("data_ref").count().orderBy("data_ref").show()

# ❌ Overwrite STATIC: reprocessar só dia 15
df_reprocessar = spark.createDataFrame(
    [(1, "A", 100.0), (2, "B", 200.0), (3, "C", 150.0)],
    ["order_id", "produto", "valor"]
).withColumn("data_ref", lit("2024-01-15"))

df_reprocessar.write.mode("overwrite").partitionBy("data_ref").parquet(caminho)

print("DEPOIS — modo STATIC apagou tudo:")
spark.read.parquet(caminho).groupBy("data_ref").count().orderBy("data_ref").show()
```

**Saída esperada:**

```
ANTES — 3 dias de dados:
+----------+-----+
|  data_ref|count|
+----------+-----+
|2024-01-13|    3|
|2024-01-14|    3|
|2024-01-15|    3|
+----------+-----+

DEPOIS — modo STATIC apagou tudo:
+----------+-----+
|  data_ref|count|
+----------+-----+
|2024-01-15|    3|
+----------+-----+
```

> **Carlos:** "Catástrofe! Os dias 13 e 14 sumiram. O modo static faz overwrite no **nível do diretório raiz** — apaga tudo antes de escrever. Por isso a configuração `dynamic` é obrigatória em produção."

---

## Passo 4: Verificar no pipeline_vendas.py — Onde a Idempotência é Configurada

**Descrição:** Nosso `pipeline_vendas.py` (Exercício 01) já implementa idempotência. Vamos identificar exatamente onde cada peça está configurada.

**Abra o arquivo e observe as 3 peças da idempotência:**

```bash
cat aula_07/code/spark_jobs/pipeline_vendas.py | grep -n -A2 "partitionOverwrite\|mode.*overwrite\|partitionBy"
```

**As 3 peças fundamentais:**

```python
# PEÇA 1: Configuração na SparkSession (linha ~107)
def criar_spark_session(data_ref: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(f"DataFlow-PipelineVendas-{data_ref}") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # ← AQUI
        .getOrCreate()

# PEÇA 2 + 3: Em cada etapa de escrita (ex: Bronze, linha ~134)
    df_bronze.write \
        .mode("overwrite") \          # ← PEÇA 2: overwrite (não append)
        .partitionBy("_data_ref") \   # ← PEÇA 3: partição por data
        .parquet(caminho_bronze)
```

**Checklist de idempotência:**

| # | Peça | Localização | Propósito |
|---|------|-------------|-----------|
| 1 | `partitionOverwriteMode=dynamic` | `criar_spark_session()` | Sobrescreve só partições do DataFrame |
| 2 | `.mode("overwrite")` | Cada `.write` | Substitui ao invés de adicionar |
| 3 | `.partitionBy("data_ref")` | Cada `.write` | Define a granularidade do overwrite |

> **Marina:** "Sem qualquer UMA dessas três peças, a idempotência quebra. Se faltar o `dynamic`, perde dados de outros dias. Se usar `append`, duplica. Se não tiver `partitionBy`, sobrescreve tudo numa pasta só."

---

## Passo 5: Provar Idempotência — Executar o Pipeline 2x

**Descrição:** A prova definitiva é executar o pipeline para a mesma data duas vezes e verificar que a contagem de registros é idêntica. Vamos usar o `pipeline_vendas.py` real.

**Executar a primeira vez:**

```bash
cd aula_07/code/spark_jobs

python pipeline_vendas.py --data-ref 2024-01-15
```

**Contar registros na Silver após 1ª execução:**

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark -e "
spark.read.parquet('data/aula_07/datalake/silver/vendas/data_ref=2024-01-15').count()
"
```

**Anotar o resultado** (ex: 950 registros).

**Executar a segunda vez (retry simulado):**

```bash
python pipeline_vendas.py --data-ref 2024-01-15
```

**Contar registros novamente:**

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark -e "
spark.read.parquet('data/aula_07/datalake/silver/vendas/data_ref=2024-01-15').count()
"
```

**Resultado esperado:** mesma contagem! Nenhum registro duplicado.

```
┌──────────────────────────────────────────────────────────┐
│  PROVA DE IDEMPOTÊNCIA                                    │
│                                                           │
│  1ª execução: Silver = 950 registros                      │
│  2ª execução: Silver = 950 registros  ← MESMO número!    │
│  3ª execução: Silver = 950 registros  ← AINDA o mesmo!   │
│                                                           │
│  Conclusão: pipeline é idempotente ✅                    │
└──────────────────────────────────────────────────────────┘
```

---

## Passo 6: Verificar que Outros Dias Ficam Intactos

**Descrição:** Tão importante quanto não duplicar é não perder dados de outros dias. Vamos processar dois dias diferentes e confirmar que um não afeta o outro.

**Processar dia 14:**

```bash
python pipeline_vendas.py --data-ref 2024-01-14
```

**Processar dia 15:**

```bash
python pipeline_vendas.py --data-ref 2024-01-15
```

**Reprocessar dia 15 (backfill):**

```bash
python pipeline_vendas.py --data-ref 2024-01-15
```

**Verificar todas as partições:**

```python
# No PySpark Shell:
df = spark.read.parquet("data/aula_07/datalake/silver/vendas")
df.groupBy("data_ref").count().orderBy("data_ref").show()
```

**Saída esperada:**

```
+----------+-----+
|  data_ref|count|
+----------+-----+
|2024-01-14|  920|  ← intacto após reprocessar dia 15
|2024-01-15|  950|  ← reprocessado, mas mesma contagem
+----------+-----+
```

> **Carlos:** "O dia 14 não foi tocado quando reprocessamos o dia 15. Cada partição é independente. Isso permite fazer backfill de um período específico sem risco para os demais."

---

## Passo 7: Boas Práticas para Garantir Idempotência

**Descrição:** A configuração de escrita é apenas uma parte. Para garantir idempotência real, o pipeline inteiro precisa ser **determinístico** — dado o mesmo input, produz o mesmo output, sempre.

**Regras de ouro para pipelines idempotentes:**

| # | Regra | Por quê | Exemplo |
|---|-------|---------|---------|
| 1 | Sem `current_timestamp()` em colunas de negócio | Muda a cada execução | Use `lit(data_ref)` |
| 2 | Sem `rand()` ou `uuid()` como chaves | IDs diferentes a cada run | Use chaves naturais (order_id) |
| 3 | Sem dependência de ordem de leitura | Spark lê em ordem arbitrária | Use `orderBy` explícito se necessário |
| 4 | Sem side-effects em UDFs | Pode rodar mais de 1x | UDFs devem ser puras |
| 5 | `_ingestion_ts` só em metadados técnicos | Aceita variação na coluna de auditoria | Não usar para join ou dedup |
| 6 | Todas as transformações são determinísticas | Mesmo input → mesmo output | groupBy + agg é OK |

**❌ Anti-padrões que quebram idempotência:**

```python
# ❌ ERRADO: uuid muda a cada execução
from pyspark.sql.functions import expr
df = df.withColumn("id", expr("uuid()"))

# ❌ ERRADO: timestamp como chave de dedup
df = df.withColumn("processed_at", current_timestamp())
df_dedup = df.dropDuplicates(["order_id", "processed_at"])  # nunca dedup!

# ❌ ERRADO: append sem partição
df.write.mode("append").parquet("/output")  # duplica no retry

# ❌ ERRADO: random sampling muda a cada run
df_sample = df.sample(fraction=0.1)  # amostra diferente cada vez
```

**✅ Padrões corretos:**

```python
# ✅ CORRETO: chave natural como identificador
df_dedup = df.dropDuplicates(["order_id"])

# ✅ CORRETO: timestamp só em metadados (prefixo _)
df = df.withColumn("_ingestion_ts", current_timestamp())  # auditoria

# ✅ CORRETO: overwrite + partitionBy + dynamic
df.write.mode("overwrite").partitionBy("data_ref").parquet("/output")

# ✅ CORRETO: transformações puras (mesmo input → mesmo output)
df = df.withColumn("ticket_medio", col("total_amount") / col("quantity"))
```

> **Marina:** "A regra é simples: se eu rodar o pipeline hoje ou amanhã para a mesma `data_ref`, os dados de negócio na Silver e Gold devem ser **bit-a-bit** idênticos. Só metadados técnicos como `_ingestion_ts` podem variar."

---

## Resumo

Neste exercício você aprendeu:

| Conceito | O que é | Como aplicar |
|----------|---------|--------------|
| **Idempotência** | N execuções = mesmo resultado que 1 | overwrite + partitionBy + dynamic |
| **Append (problema)** | Adiciona registros a cada execução | Nunca usar em pipelines com retry |
| **Static overwrite** | Apaga TODAS as partições | Perigoso — perde dados de outros dias |
| **Dynamic overwrite** | Apaga SÓ partições do DataFrame | Seguro — padrão para produção |
| **Determinismo** | Mesmo input → mesmo output | Sem random, uuid, timestamp em chaves |

**Configuração completa para idempotência no Spark:**

```python
# Na criação da SparkSession:
spark = SparkSession.builder \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# Em cada escrita:
df.write \
    .mode("overwrite") \
    .partitionBy("data_ref") \
    .parquet(caminho)
```

**Cenários onde idempotência salva:**

| Cenário | Sem idempotência | Com idempotência |
|---------|-----------------|------------------|
| Pipeline falha no meio + retry | Dados duplicados na etapa que completou | Sobrescreve, resultado correto |
| Backfill de 7 dias | Pode corromper dias já processados | Cada dia é independente |
| Airflow retry automático (3x) | 3x dados se falha após write | Sempre resultado correto |
| Reprocessamento manual | Precisa limpar dados antes | Basta rodar de novo |

---

## Próximo Passo

No **Exercício 04**, vamos criar a **DAG Airflow** que orquestra o `pipeline_vendas.py` via `SparkSubmitOperator`, com retry automático e callbacks de alerta — aproveitando a idempotência que acabamos de implementar para permitir retries seguros.
