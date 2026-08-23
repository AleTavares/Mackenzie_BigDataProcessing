# Exercício 3 — Camada Bronze: Persistência Raw

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Perfeito! Já ingerimos os dados dos 3 parceiros e cada DataFrame está enriquecido com metadados de ingestão. Agora vem a parte crucial: persistir tudo na camada Bronze do nosso data lake. A Bronze é a 'landing zone' — dados brutos, exatamente como vieram, sem nenhuma transformação de negócio. Só adicionamos metadados técnicos. O princípio fundamental: append-only. Nunca sobrescrevemos, nunca deletamos. Se os dados de amanhã vierem errados, a gente corrige na Silver — a Bronze é nosso 'backup histórico sagrado'. Vou te mostrar como persistir em Parquet particionado e depois validar que nada foi perdido no processo."

## Objetivos

Ao final deste exercício, você será capaz de:

- Criar a estrutura de diretórios de um data lake (bronze/silver/gold)
- Persistir DataFrames em formato Parquet com `mode("append")`
- Particionar dados por `_source` para isolamento entre parceiros
- Validar round-trip de escrita/leitura (nenhum registro perdido)
- Listar e inspecionar a estrutura de partições no disco
- Ler dados de partições específicas usando filtros (partition discovery)
- Compreender os princípios fundamentais da camada Bronze

## Pré-requisitos

- SparkSession ativa (criada no Exercício 1)
- DataFrames dos 3 parceiros já criados e com metadados:
  - `df_parceiro_a_bronze` (~50.000 registros)
  - `df_parceiro_b_bronze` (~30.000 registros)
  - `df_parceiro_c_bronze` (~80.000 registros)

## Duração Estimada

⏱️ ~15 minutos

---

## Passo 1: Criar a Estrutura do Data Lake (Bronze / Silver / Gold)

**Descrição:** Antes de gravar qualquer dado, precisamos criar a estrutura de diretórios do data lake. Seguimos a arquitetura Medallion com 3 camadas: Bronze (raw), Silver (normalizada) e Gold (agregada para negócio). Vamos usar Python puro para criar os diretórios — isso é algo que normalmente faz parte do setup inicial da infraestrutura.

**Código:**

```python
import os

# Definir caminho base do data lake
DATALAKE_PATH = "datalake"

# Criar estrutura de diretórios
diretorios = [
    f"{DATALAKE_PATH}/bronze",
    f"{DATALAKE_PATH}/silver",
    f"{DATALAKE_PATH}/gold",
]

for d in diretorios:
    os.makedirs(d, exist_ok=True)
    print(f"📁 Criado: {d}/")

print(f"\n✅ Estrutura do data lake criada em '{DATALAKE_PATH}/'")
print(f"   📂 bronze/ — dados brutos, como vieram dos parceiros")
print(f"   📂 silver/ — dados normalizados, limpos, unificados")
print(f"   📂 gold/   — dados agregados para consumo de negócio")
```

**Resultado esperado:**

```
📁 Criado: datalake/bronze/
📁 Criado: datalake/silver/
📁 Criado: datalake/gold/

✅ Estrutura do data lake criada em 'datalake/'
   📂 bronze/ — dados brutos, como vieram dos parceiros
   📂 silver/ — dados normalizados, limpos, unificados
   📂 gold/   — dados agregados para consumo de negócio
```

**Explicação técnica:**

- **Arquitetura Medallion** — padrão amplamente adotado (Databricks, Azure, AWS). Cada camada tem uma responsabilidade clara e regras de governança específicas
- **Bronze** — landing zone. Dados "crus" com metadados de ingestão. Nenhuma regra de negócio aplicada. Append-only
- **Silver** — normalização. Schemas unificados, deduplicação, conversão de tipos, limpeza de nulls. Pode sobrescrever partições
- **Gold** — consumo. Agregações, KPIs, métricas prontas para dashboards e relatórios. Otimizada para leitura
- **`exist_ok=True`** — não dá erro se o diretório já existir. Importante para idempotência (executar múltiplas vezes sem efeito colateral)

> **💡 Dica de Carlos:** "Em produção, o data lake fica em cloud storage (S3, ADLS, GCS) — não em disco local. Mas a estrutura lógica é a mesma. Muitas empresas adicionam camadas extras como 'landing' (antes da Bronze) ou 'platinum' (após Gold), mas Bronze/Silver/Gold cobre 90% dos cenários."

---

## Passo 2: Gravar Parceiro A na Bronze (Parquet + Append + Partição)

**Descrição:** Vamos gravar o DataFrame do Parceiro A na camada Bronze. Usamos formato Parquet (compressão Snappy por padrão), modo `append` (nunca sobrescrevemos na Bronze) e particionamento por `_source`. O particionamento cria subdiretórios no disco — um para cada valor distinto da coluna de partição. Isso permite leituras seletivas no futuro.

**Código:**

```python
# Caminho da Bronze
BRONZE_PATH = f"{DATALAKE_PATH}/bronze/vendas"

# Gravar Parceiro A na Bronze
df_parceiro_a_bronze.write \
    .mode("append") \
    .partitionBy("_source") \
    .parquet(BRONZE_PATH)

# Verificar
count_a = df_parceiro_a_bronze.count()
print(f"✅ Parceiro A gravado na Bronze!")
print(f"   📊 Registros: {count_a:,}")
print(f"   📁 Caminho: {BRONZE_PATH}/")
print(f"   📦 Formato: Parquet (Snappy)")
print(f"   📂 Partição: _source=parceiro_a")
print(f"   ✏️  Modo: append")
```

**Resultado esperado:**

```
✅ Parceiro A gravado na Bronze!
   📊 Registros: ~50,000
   📁 Caminho: datalake/bronze/vendas/
   📦 Formato: Parquet (Snappy)
   📂 Partição: _source=parceiro_a
   ✏️  Modo: append
```

**Explicação técnica:**

- **`mode("append")`** — adiciona dados sem remover o que já existe. É o modo padrão para Bronze. Se rodarmos a célula novamente, teremos dados duplicados — na prática, usamos controles de idempotência para evitar isso
- **`partitionBy("_source")`** — cria diretório `_source=parceiro_a/` dentro de `bronze/vendas/`. Quando lermos da Bronze, podemos filtrar por `_source` sem escanear todos os dados
- **Parquet** — formato colunar, comprimido (~4x menor que CSV), com schema embutido. É o padrão de facto para data lakes
- **Snappy** — codec de compressão padrão do Spark para Parquet. Equilíbrio entre velocidade de descompressão e taxa de compressão

> **💡 Dica de Carlos:** "Append-only é o princípio #1 da Bronze. Se você precisa corrigir dados, nunca altere a Bronze — crie uma nova versão na Silver. A Bronze é imutável por design: é sua 'source of truth' histórica. Se um parceiro reenviar dados corrigidos, eles entram como um novo append — ambas versões coexistem."

---

## Passo 3: Gravar Parceiro B na Bronze (Mesmo Padrão)

**Descrição:** Seguimos exatamente o mesmo padrão para o Parceiro B. A consistência é fundamental: todos os parceiros seguem a mesma convenção de escrita. Isso simplifica a leitura posterior — um único `spark.read.parquet("bronze/vendas/")` lê tudo, e o Spark descobre as partições automaticamente.

**Código:**

```python
# Gravar Parceiro B na Bronze (mesmo caminho, append)
df_parceiro_b_bronze.write \
    .mode("append") \
    .partitionBy("_source") \
    .parquet(BRONZE_PATH)

# Verificar
count_b = df_parceiro_b_bronze.count()
print(f"✅ Parceiro B gravado na Bronze!")
print(f"   📊 Registros: {count_b:,}")
print(f"   📁 Caminho: {BRONZE_PATH}/")
print(f"   📂 Partição: _source=parceiro_b")
print(f"   ✏️  Modo: append")
```

**Resultado esperado:**

```
✅ Parceiro B gravado na Bronze!
   📊 Registros: 30,000
   📁 Caminho: datalake/bronze/vendas/
   📂 Partição: _source=parceiro_b
   ✏️  Modo: append
```

**Explicação técnica:**

- **Mesmo caminho (`BRONZE_PATH`)** — todos os parceiros gravam no mesmo dataset da Bronze. O particionamento por `_source` separa fisicamente os dados
- **Schema consistency** — como os 3 DataFrames têm schemas diferentes (nomes de colunas, tipos), o Parquet armazena cada arquivo com seu schema próprio. Na leitura, o Spark faz `mergeSchema` se necessário
- **Arquivo separado** — cada `write` cria novos arquivos `.parquet` dentro da partição correspondente. Os arquivos do Parceiro A não são afetados quando gravamos o Parceiro B

> **💡 Dica de Carlos:** "Note que estamos gravando no mesmo diretório base. Cada parceiro gera sua própria partição (`_source=parceiro_b/`). Quando lemos `bronze/vendas/`, o Spark descobre automaticamente TODAS as partições e monta o DataFrame completo. É o chamado 'partition discovery'."

---

## Passo 4: Gravar Parceiro C na Bronze (Completando a Ingestão)

**Descrição:** Último parceiro! Após esta escrita, teremos todos os ~160.000 registros persistidos na Bronze. O Parceiro C é o que tem mais volume (80K registros), mas como já vem em Parquet, a conversão é essencialmente uma cópia otimizada com reparticionamento.

**Código:**

```python
# Gravar Parceiro C na Bronze (mesmo caminho, append)
df_parceiro_c_bronze.write \
    .mode("append") \
    .partitionBy("_source") \
    .parquet(BRONZE_PATH)

# Verificar
count_c = df_parceiro_c_bronze.count()
print(f"✅ Parceiro C gravado na Bronze!")
print(f"   📊 Registros: {count_c:,}")
print(f"   📁 Caminho: {BRONZE_PATH}/")
print(f"   📂 Partição: _source=parceiro_c")
print(f"   ✏️  Modo: append")

# Resumo da ingestão na Bronze
print(f"\n{'='*60}")
print(f"📋 INGESTÃO COMPLETA NA CAMADA BRONZE")
print(f"{'='*60}")
print(f"   Parceiro A: {count_a:>8,} registros")
print(f"   Parceiro B: {count_b:>8,} registros")
print(f"   Parceiro C: {count_c:>8,} registros")
print(f"   {'─'*40}")
print(f"   TOTAL:      {count_a + count_b + count_c:>8,} registros")
print(f"{'='*60}")
```

**Resultado esperado:**

```
✅ Parceiro C gravado na Bronze!
   📊 Registros: 80,000
   📁 Caminho: datalake/bronze/vendas/
   📂 Partição: _source=parceiro_c
   ✏️  Modo: append

============================================================
📋 INGESTÃO COMPLETA NA CAMADA BRONZE
============================================================
   Parceiro A:   50,000 registros
   Parceiro B:   30,000 registros
   Parceiro C:   80,000 registros
   ────────────────────────────────────────
   TOTAL:       160,000 registros
============================================================
```

**Explicação técnica:**

- **Consistência no padrão** — os 3 parceiros seguem exatamente o mesmo template de escrita: `mode("append")`, `partitionBy("_source")`, mesmo caminho. Isso é intencional e facilita manutenção
- **Parquet → Parquet** — para o Parceiro C (que já era Parquet), estamos essencialmente reparticionando por `_source`. O Spark re-serializa os dados com compressão Snappy
- **Separação física** — no disco, temos: `bronze/vendas/_source=parceiro_a/`, `_source=parceiro_b/`, `_source=parceiro_c/`. Três diretórios completamente independentes
- **Isolamento de falhas** — se a escrita do Parceiro C falhar, os dados dos Parceiros A e B já estão salvos. Cada write é atômico por partição

> **💡 Dica de Carlos:** "Em produção, cada parceiro teria seu próprio job/DAG de ingestão. Eles não rodam na mesma célula. Aqui estamos fazendo sequencial para fins didáticos, mas no Airflow cada um seria uma task independente com retry próprio."

---

## Passo 5: Verificar a Estrutura no Disco (Partições e Arquivos)

**Descrição:** Vamos inspecionar o que foi criado no disco. O Spark organiza os dados em partições (subdiretórios) e dentro de cada partição há um ou mais arquivos `.parquet`. Essa estrutura é fundamental para entender como o Spark faz "partition pruning" — ler apenas as partições necessárias.

**Código:**

```python
import os

def listar_estrutura(caminho, indent=0):
    """Lista a estrutura de diretórios e arquivos recursivamente"""
    items = sorted(os.listdir(caminho))
    for item in items:
        full_path = os.path.join(caminho, item)
        if os.path.isdir(full_path):
            print(f"{'  ' * indent}📂 {item}/")
            listar_estrutura(full_path, indent + 1)
        else:
            # Mostrar tamanho do arquivo
            size_kb = os.path.getsize(full_path) / 1024
            if item.endswith(".parquet"):
                print(f"{'  ' * indent}📄 {item} ({size_kb:.1f} KB)")
            elif not item.startswith("."):
                print(f"{'  ' * indent}📄 {item}")

print("📁 Estrutura do Data Lake:")
print("=" * 60)
listar_estrutura(DATALAKE_PATH)
```

**Resultado esperado:**

```
📁 Estrutura do Data Lake:
============================================================
📂 bronze/
  📂 vendas/
    📂 _source=parceiro_a/
      📄 part-00000-xxxxx.snappy.parquet (850.2 KB)
      📄 part-00001-xxxxx.snappy.parquet (843.7 KB)
      ...
    📂 _source=parceiro_b/
      📄 part-00000-xxxxx.snappy.parquet (512.3 KB)
      📄 part-00001-xxxxx.snappy.parquet (508.9 KB)
      ...
    📂 _source=parceiro_c/
      📄 part-00000-xxxxx.snappy.parquet (1,240.5 KB)
      📄 part-00001-xxxxx.snappy.parquet (1,235.1 KB)
      ...
📂 gold/
📂 silver/
```

**Explicação técnica:**

- **`_source=parceiro_a/`** — diretório de partição. O nome segue o formato `coluna=valor`. O Spark cria automaticamente essa estrutura quando usamos `partitionBy()`
- **`part-00000-xxxxx.snappy.parquet`** — arquivo de dados. O número (`00000`) indica a partição do Spark (parallelism). O `snappy` indica a compressão. O `xxxxx` é um UUID para evitar conflitos
- **Múltiplos arquivos por partição** — o Spark grava um arquivo por partição de processamento (executor). Quanto mais paralelismo, mais arquivos. Em produção, arquivos muito pequenos (<128MB) devem ser compactados (coalesce)
- **Diretórios `silver/` e `gold/` vazios** — ainda não gravamos nada nessas camadas. Serão preenchidas nos próximos exercícios

> **💡 Dica de Carlos:** "Em produção com S3/ADLS, você veria a mesma estrutura — mas em object storage em vez de filesystem. O `_SUCCESS` que o Spark cria em cada diretório indica que a escrita foi concluída com sucesso. Se não existir, a escrita pode ter falhado no meio."

---

## Passo 6: Ler de Volta da Bronze e Validar Contagens

**Descrição:** Agora vem a validação mais importante: ler os dados de volta da Bronze e confirmar que nenhum registro foi perdido no processo de escrita. Esse "round-trip test" (gravar → ler → comparar) é uma prática obrigatória em engenharia de dados. Se os números não baterem, temos um problema sério.

**Código:**

```python
# Ler TODA a camada Bronze de volta
df_bronze_lido = spark.read.parquet(BRONZE_PATH)

# Contagem total
total_bronze = df_bronze_lido.count()
total_original = count_a + count_b + count_c

print("=" * 60)
print("🔍 VALIDAÇÃO DE ROUND-TRIP (escrita → leitura)")
print("=" * 60)
print(f"   Registros gravados:  {total_original:>9,}")
print(f"   Registros lidos:     {total_bronze:>9,}")
print(f"   Diferença:           {total_bronze - total_original:>9,}")
print()

# Validar contagem por partição
print("📊 Contagem por partição (_source):")
df_bronze_lido.groupBy("_source").count() \
    .orderBy("_source") \
    .show()

# Validação com assert
assert total_bronze == total_original, \
    f"❌ ERRO: esperado {total_original}, encontrado {total_bronze}!"

print("✅ Validação OK! Nenhum registro perdido no round-trip.")
```

**Resultado esperado:**

```
============================================================
🔍 VALIDAÇÃO DE ROUND-TRIP (escrita → leitura)
============================================================
   Registros gravados:    160,000
   Registros lidos:       160,000
   Diferença:                   0

📊 Contagem por partição (_source):
+----------+------+
|   _source| count|
+----------+------+
|parceiro_a| 50000|
|parceiro_b| 30000|
|parceiro_c| 80000|
+----------+------+

✅ Validação OK! Nenhum registro perdido no round-trip.
```

**Explicação técnica:**

- **Round-trip test** — gravar e ler de volta para confirmar integridade. Parece trivial, mas erros de serialização, encoding ou particionamento podem causar perda silenciosa de dados
- **`spark.read.parquet(BRONZE_PATH)`** — lê TODOS os arquivos Parquet recursivamente, incluindo todas as partições. O Spark automaticamente adiciona a coluna `_source` a partir do nome do diretório de partição
- **Partition discovery** — o Spark descobre automaticamente que existem 3 partições (`parceiro_a`, `parceiro_b`, `parceiro_c`) e reconstrói a coluna `_source` nos dados. É transparente para o leitor
- **`assert`** — em produção, usaríamos frameworks de data quality (Great Expectations, Deequ) para essas validações. Aqui o assert é suficiente para fins didáticos

> **💡 Dica de Carlos:** "Sempre valide contagens após writes na Bronze. Em produção, eu implemento um check automático no Airflow: se a contagem lida difere da gravada em mais de 0.1%, o pipeline falha e dispara alerta. Dados silenciosamente perdidos são piores que erros explícitos."

---

## Passo 7: Leitura Seletiva — Partition Pruning (Ler Apenas Um Parceiro)

**Descrição:** Uma das maiores vantagens do particionamento é poder ler apenas os dados que interessam, sem escanear o dataset inteiro. Quando filtramos por `_source`, o Spark faz "partition pruning" — ele nem abre os arquivos das outras partições. Isso é crucial quando seu data lake tem terabytes e você só precisa de uma fatia.

**Código:**

```python
# Ler APENAS os dados do Parceiro A da Bronze
df_apenas_parceiro_a = spark.read.parquet(BRONZE_PATH) \
    .filter(col("_source") == "parceiro_a")

# Contagem — deve ser igual ao original
count_lido_a = df_apenas_parceiro_a.count()
print(f"📊 Registros lidos (apenas Parceiro A): {count_lido_a:,}")
print(f"   Esperado: {count_a:,}")
print(f"   Match: {'✅' if count_lido_a == count_a else '❌'}")
print()

# Verificar que só veio Parceiro A
print("Valores de _source no DataFrame filtrado:")
df_apenas_parceiro_a.select("_source").distinct().show()

# Mostrar o plano de execução — observe o partition pruning
print("📋 Plano de execução (observe 'PartitionFilters'):")
df_apenas_parceiro_a.explain()
```

**Resultado esperado:**

```
📊 Registros lidos (apenas Parceiro A): ~50,000
   Esperado: ~50,000
   Match: ✅

Valores de _source no DataFrame filtrado:
+----------+
|   _source|
+----------+
|parceiro_a|
+----------+

📋 Plano de execução (observe 'PartitionFilters'):
== Physical Plan ==
*(1) ColumnarToRow
+- FileScan parquet [order_id#..., ...]
   PartitionFilters: [isnotnull(_source), (_source = parceiro_a)]
   Location: InMemoryFileIndex[file:///...datalake/bronze/vendas]
   ReadSchema: struct<...>
```

**Explicação técnica:**

- **`PartitionFilters`** — no plano de execução, indica que o Spark aplicou o filtro na DESCOBERTA de partições (antes de ler dados). Ele simplesmente não abre os diretórios `_source=parceiro_b/` e `_source=parceiro_c/`
- **Zero I/O desperdiçado** — em um dataset de 100TB particionado por fonte, ler apenas 1 parceiro pode significar ler 10TB em vez de 100TB. Economia de 90% em I/O e tempo
- **Filtro no read vs no DataFrame** — quando fazemos `.filter(col("_source") == "parceiro_a")`, o Spark otimiza automaticamente com partition pruning. Não precisa de sintaxe especial
- **Comparação com CSV** — com CSV em um único diretório, filtrar por fonte exigiria ler TODOS os dados e depois descartar. Com Parquet particionado, os dados indesejados nem são tocados

> **💡 Dica de Carlos:** "Partition pruning é a otimização #1 em data lakes. Escolha suas colunas de partição com cuidado: elas devem ser usadas frequentemente em filtros (WHERE) e ter cardinalidade baixa-média (10-1000 valores distintos). Nunca particione por colunas de alta cardinalidade como `order_id` — você terá milhões de diretórios com 1 arquivo cada!"

---

## Passo 8: Verificar Schema da Bronze e Metadados Preservados

**Descrição:** Vamos verificar que o schema completo foi preservado na persistência, incluindo os metadados de ingestão. Também vamos inspecionar os `_ingestion_ts` para confirmar que os timestamps de ingestão estão registrados corretamente — isso é essencial para auditoria e rastreabilidade.

**Código:**

```python
# Schema da Bronze (lido do Parquet)
print("📋 Schema da Camada Bronze:")
df_bronze_lido.printSchema()

# Verificar metadados de ingestão
print("\n📊 Metadados de ingestão por parceiro:")
from pyspark.sql.functions import min as spark_min, max as spark_max

df_bronze_lido.groupBy("_source").agg(
    spark_min("_ingestion_ts").alias("primeiro_ingestion"),
    spark_max("_ingestion_ts").alias("ultimo_ingestion"),
    count("*").alias("total_registros")
).orderBy("_source").show(truncate=False)

# Verificar que _file_origin foi preservado
print("📊 Arquivos de origem por parceiro (amostra):")
df_bronze_lido.groupBy("_source") \
    .agg(count("*").alias("registros")) \
    .show()
```

**Resultado esperado:**

```
📋 Schema da Camada Bronze:
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: ... (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- order_date: ... (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- shipping_city: string (nullable = true)
 |-- shipping_state: string (nullable = true)
 |-- status: string (nullable = true)
 |-- _ingestion_ts: timestamp (nullable = true)
 |-- _file_origin: string (nullable = true)
 |-- _source: string (nullable = true)

📊 Metadados de ingestão por parceiro:
+----------+-------------------+-------------------+---------------+
|_source   |primeiro_ingestion |ultimo_ingestion   |total_registros|
+----------+-------------------+-------------------+---------------+
|parceiro_a|2024-01-15 10:30:..|2024-01-15 10:30:..|50000          |
|parceiro_b|2024-01-15 10:35:..|2024-01-15 10:35:..|30000          |
|parceiro_c|2024-01-15 10:40:..|2024-01-15 10:40:..|80000          |
+----------+-------------------+-------------------+---------------+

📊 Arquivos de origem por parceiro (amostra):
+----------+---------+
|   _source|registros|
+----------+---------+
|parceiro_a|    50000|
|parceiro_b|    30000|
|parceiro_c|    80000|
+----------+---------+
```

**Explicação técnica:**

- **Schema preservado** — o Parquet mantém tipos e nomes exatamente como gravados. A coluna `_source` aparece por último porque é a coluna de partição (reconstruída pelo partition discovery)
- **`_ingestion_ts`** — timestamp de quando os dados foram ingeridos no nosso sistema. Cada parceiro tem timestamps ligeiramente diferentes (foram processados sequencialmente)
- **`_file_origin`** — caminho do arquivo original de onde cada registro veio. Permite rastrear qualquer registro até sua fonte original
- **Auditoria** — com esses 3 metadados, podemos responder: "De onde veio esse dado?" (`_source` + `_file_origin`) e "Quando foi ingerido?" (`_ingestion_ts`). Requisito fundamental de governança de dados

> **💡 Dica de Carlos:** "Esses metadados são sua 'linha de vida' em produção. Quando a Ana ligar dizendo 'esse número no relatório está errado', você consegue rastrear: veio do Parceiro B, arquivo api_dump_page_002.json, ingerido dia 15/01 às 10:35. Sem esses metadados, é arqueologia."

---

## Passo 9: Resumo — Princípios da Camada Bronze

**Descrição:** Antes de prosseguir para a Silver, vamos consolidar os princípios fundamentais da camada Bronze. Esses conceitos são o alicerce de qualquer data lake bem arquitetado.

**Código:**

```python
print("""
╔══════════════════════════════════════════════════════════════════╗
║             PRINCÍPIOS DA CAMADA BRONZE                         ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  1️⃣  RAW (Bruto)                                                 ║
║     • Dados exatamente como vieram da fonte                      ║
║     • Nenhuma transformação de negócio aplicada                  ║
║     • Apenas ajustes técnicos: encoding, parsing de formato      ║
║                                                                  ║
║  2️⃣  APPEND-ONLY (Somente Adição)                                ║
║     • Nunca sobrescrever dados existentes                        ║
║     • Nunca deletar registros                                    ║
║     • Correções acontecem na Silver, não na Bronze               ║
║                                                                  ║
║  3️⃣  METADATA ENRICHED (Enriquecido com Metadados)              ║
║     • _source: de quem veio o dado                               ║
║     • _ingestion_ts: quando foi ingerido                         ║
║     • _file_origin: arquivo original de onde veio                ║
║                                                                  ║
║  4️⃣  PARTITIONED (Particionado)                                  ║
║     • Partição por fonte (_source) para isolamento               ║
║     • Permite leitura seletiva sem escanear tudo                 ║
║     • Facilita reprocessamento de uma fonte específica           ║
║                                                                  ║
║  5️⃣  PARQUET FORMAT (Formato Otimizado)                          ║
║     • Compressão Snappy (~4x menor que CSV)                      ║
║     • Schema embutido (auto-descritivo)                          ║
║     • Leitura colunar (só lê colunas necessárias)               ║
║     • Predicate pushdown (filtros na leitura)                    ║
║                                                                  ║
╠══════════════════════════════════════════════════════════════════╣
║  BRONZE = Landing Zone = Source of Truth Histórica               ║
║  "Se algo deu errado, a Bronze sempre tem a versão original"     ║
╚══════════════════════════════════════════════════════════════════╝
""")
```

**Código (tabela comparativa):**

```python
print("📊 Comparação: O que PODE e o que NÃO PODE na Bronze")
print("=" * 60)
print(f"""
   ✅ PODE na Bronze:
      • Converter encoding (ISO-8859-1 → UTF-8)
      • Fazer parsing de formato (CSV → Parquet)
      • Adicionar metadados de ingestão
      • Converter tipos primitivos (string → date)
      • Append de novos dados
      • Particionamento por fonte/data

   ❌ NÃO PODE na Bronze:
      • Filtrar registros (remover "inválidos")
      • Renomear colunas para padrão unificado
      • Aplicar regras de negócio
      • Deduplicar registros
      • Fazer joins entre fontes
      • Deletar ou sobrescrever dados antigos
      • Agregar ou sumarizar dados
""")

print("\n⏭️  Próximo exercício: construir a camada Silver —")
print("   onde a normalização, limpeza e unificação acontecem!")
```

**Resultado esperado:**

O quadro de princípios e a tabela de permissões são exibidos na tela, consolidando todo o aprendizado deste exercício.

---

## Resumo do Exercício

Neste exercício você aprendeu a implementar a camada Bronze de um data lake, aplicando os princípios fundamentais de persistência raw:

| Operação | Comando | Justificativa |
|----------|---------|---------------|
| Criar diretórios | `os.makedirs(path, exist_ok=True)` | Estrutura bronze/silver/gold |
| Gravar Parquet | `df.write.parquet(path)` | Formato otimizado, schema embutido |
| Modo append | `.mode("append")` | Nunca sobrescrever na Bronze |
| Particionar | `.partitionBy("_source")` | Isolamento e leitura seletiva |
| Ler de volta | `spark.read.parquet(path)` | Validação de round-trip |
| Filtrar partição | `.filter(col("_source") == "x")` | Partition pruning automático |

### Conceitos-chave

1. **Append-only** — princípio fundamental da Bronze. Dados nunca são removidos ou sobrescritos
2. **Particionamento por `_source`** — isola parceiros fisicamente no disco, permitindo leitura seletiva
3. **Round-trip validation** — sempre validar que `count(gravado) == count(lido)` após persistência
4. **Partition pruning** — filtros em colunas de partição evitam I/O desnecessário
5. **Metadados de ingestão** — `_source`, `_ingestion_ts`, `_file_origin` garantem rastreabilidade completa
6. **Parquet + Snappy** — compressão eficiente com schema preservado e leitura colunar
7. **Bronze ≠ Silver** — Bronze é raw (como veio), Silver é normalizada (como queremos)

### Tabela de Referência — Modos de Escrita no Spark

| Modo | Comportamento | Uso na Bronze? |
|------|---------------|----------------|
| `append` | Adiciona sem alterar existente | ✅ Padrão |
| `overwrite` | Substitui toda a tabela/partição | ❌ Nunca na Bronze |
| `ignore` | Não grava se já existir | ⚠️ Raramente |
| `error`/`errorifexists` | Erro se destino existir | ❌ Impede re-execução |

### Tabela de Referência — Boas Práticas de Particionamento

| Prática | Recomendação |
|---------|--------------|
| Cardinalidade | Baixa-média (10-1000 valores) |
| Coluna | Frequentemente usada em WHERE |
| Evitar | Colunas de alta cardinalidade (IDs) |
| Combinar | Até 2-3 níveis (ex: `_source/year/month`) |
| Tamanho mínimo | Cada partição deve ter >128MB |
| Tamanho máximo | Cada arquivo <1GB |

> **Carlos:** "A Bronze está completa! 160 mil registros, 3 parceiros, tudo em Parquet particionado com metadados de auditoria. Agora vem a parte mais desafiadora: unificar 3 schemas completamente diferentes em um schema padronizado na Silver. Cada parceiro tem nomes de colunas diferentes, tipos incompatíveis e campos exclusivos. É onde o engenheiro de dados realmente mostra seu valor."

---

## Próximo Exercício

➡️ **Exercício 4 — Camada Silver: Normalização e Unificação** (`04_camada_silver.md`): normalizar schemas, unificar os 3 parceiros em um DataFrame único e persistir na Silver com particionamento por data
