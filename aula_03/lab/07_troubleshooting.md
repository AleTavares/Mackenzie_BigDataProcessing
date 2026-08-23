# Troubleshooting — Aula 3: Ingestão e Persistência de Dados

## Contexto

> **Marina Silva (Arquiteta de Dados):** "Na DataFlow Analytics, quando integramos dados de múltiplos parceiros, cada um com seu formato e suas peculiaridades, aprendi que 80% dos problemas se concentram em três áreas: encoding, schema e particionamento. Vou compartilhar com vocês os erros que mais encontro no dia a dia — e como resolver cada um rapidamente."

---

## 1. UnicodeDecodeError / Encoding Incorreto

**Sintoma:**
```python
df = spark.read.csv("data/parceiro_a/vendas_legado.csv", header=True)
df.show(5)
# +----------+-------------------+----------+
# |produto   |descricao          |cidade    |
# +----------+-------------------+----------+
# |PROD-001  |CafÃ© ExpressÃ£o   |SÃ£o Paulo|
# |PROD-002  |PÃ£o FrancÃªs      |CuritibÃ¡ |
# +----------+-------------------+----------+
```

Ou, em casos mais graves:
```
UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe9 in position 42: invalid continuation byte
```

**Causa:** O arquivo CSV foi salvo com encoding `ISO-8859-1` (Latin-1) ou `Windows-1252`, mas o Spark está tentando ler como `UTF-8` (padrão). Caracteres acentuados (é, ã, ç, ô) ocupam bytes diferentes em cada encoding.

**Solução:**
```python
# ✅ Especificar o encoding correto na leitura
df = spark.read.csv(
    "data/parceiro_a/vendas_legado.csv",
    header=True,
    encoding="ISO-8859-1",  # ou "latin1" ou "Windows-1252"
    sep=";"                  # Parceiros brasileiros frequentemente usam ";"
)
df.show(5)
# +----------+-------------------+----------+
# |produto   |descricao          |cidade    |
# +----------+-------------------+----------+
# |PROD-001  |Café Expressão     |São Paulo |
# |PROD-002  |Pão Francês        |Curitiba  |
# +----------+-------------------+----------+
```

```python
# Se não sabe qual encoding o arquivo usa, detecte com chardet:
import chardet

with open("data/parceiro_a/vendas_legado.csv", "rb") as f:
    resultado = chardet.detect(f.read(10000))
    print(resultado)
# {'encoding': 'ISO-8859-1', 'confidence': 0.73, 'language': 'Portuguese'}
```

**Prevenção:**
- Documente o encoding de cada fonte de dados no README do parceiro
- Padronize a conversão para UTF-8 na camada Bronze imediatamente após a leitura
- Sempre pergunte ao parceiro: "Qual encoding e separador vocês usam?" antes de integrar

---

## 2. AnalysisException: Schema Mismatch no Append

**Sintoma:**
```python
df_novo.write.mode("append").parquet("data/silver/vendas/")

# AnalysisException: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]
# Cannot write incompatible data for table.
# Cannot safely cast 'total_amount' from StringType to DoubleType.
```

Ou:
```
AnalysisException: Column 'desconto' does not exist in the target schema.
Existing columns: [order_id, customer_id, product_id, quantity, unit_price, total_amount]
```

**Causa:** Ao usar `mode("append")`, o Spark verifica se o schema do DataFrame sendo gravado é compatível com o schema dos arquivos existentes no diretório. Se houver colunas extras, faltantes ou com tipos diferentes, a operação falha.

**Solução:**
```python
# 1. Verificar schema existente vs schema novo
df_existente = spark.read.parquet("data/silver/vendas/")
df_novo.printSchema()
df_existente.printSchema()

# 2. Alinhar schemas antes do append

# Caso A: Colunas com tipos diferentes → fazer cast
from pyspark.sql.functions import col
df_novo = df_novo.withColumn("total_amount", col("total_amount").cast("double"))

# Caso B: Colunas extras no novo DataFrame → remover ou adicionar no existente
colunas_alvo = df_existente.columns
df_novo_alinhado = df_novo.select(*colunas_alvo)

# Caso C: Colunas faltantes no novo → adicionar com valor null
from pyspark.sql.functions import lit
df_novo = df_novo.withColumn("desconto", lit(None).cast("double"))
```

```python
# 3. Para schema evolution controlada, usar mergeSchema
df_novo.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .parquet("data/silver/vendas/")

# Ou configurar globalmente:
spark.conf.set("spark.sql.parquet.mergeSchema", "true")
```

**Prevenção:**
- Defina schemas explícitos (StructType) para cada camada e valide antes de gravar
- Nunca confie em `inferSchema` para dados que serão persistidos
- Use `mergeSchema` apenas quando a evolução é intencional (nova coluna adicionada pelo parceiro)
- Mantenha um contrato de dados documentado para cada fonte

---

## 3. Partition Discovery Falha / DataFrame Vazio

**Sintoma:**
```python
df = spark.read.parquet("data/silver/vendas/")
df.count()
# 0  ← DataFrame vazio, mas os arquivos existem!
```

Ou:
```
AnalysisException: [PATH_NOT_FOUND] Path does not exist:
  file:/home/jovyan/work/data/silver/vendas/ano=2023/mes=12/part-00000.snappy.parquet
```

**Causa:** A estrutura de diretórios não segue o padrão esperado pelo Spark para partition discovery (`coluna=valor/`). Problemas comuns:
- Diretório intermediário sem dados (só subpastas)
- Arquivos fora da estrutura de partição (ex: `_SUCCESS`, `.crc`)
- Path apontando para nível errado da hierarquia
- Partições com nomes inconsistentes

**Solução:**
```python
# 1. Verificar a estrutura real do diretório
import os
for root, dirs, files in os.walk("data/silver/vendas/"):
    for f in files:
        if not f.startswith(("_", ".")):
            print(os.path.join(root, f))

# Saída esperada:
# data/silver/vendas/ano=2023/mes=01/part-00000.snappy.parquet
# data/silver/vendas/ano=2023/mes=02/part-00000.snappy.parquet

# 2. Se o path está no nível errado, ajustar:
# ❌ Errado (um nível acima demais):
df = spark.read.parquet("data/silver/")

# ✅ Correto (nível raiz das partições):
df = spark.read.parquet("data/silver/vendas/")

# 3. Para leitura recursiva de arquivos em subpastas arbitrárias:
spark.conf.set("spark.sql.sources.partitionDiscovery.enabled", "true")
df = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet("data/silver/vendas/")
# ⚠️ Nota: recursiveFileLookup ignora partições (não cria colunas de partição)

# 4. Especificar basePath para forçar discovery a partir de um nível:
df = spark.read \
    .option("basePath", "data/silver/vendas/") \
    .parquet("data/silver/vendas/ano=2023/mes=01/")
```

**Prevenção:**
- Use sempre `partitionBy()` no write para garantir estrutura padronizada
- Mantenha a hierarquia consistente: `tabela/coluna=valor/arquivos.parquet`
- Evite misturar arquivos soltos e pastas de partição no mesmo diretório
- Verifique com `os.listdir()` antes de ler se a estrutura está correta

---

## 4. JSON Parsing Errors / Registros Corrompidos

**Sintoma:**
```python
df = spark.read.json("data/parceiro_b/eventos.json")
df.show()
# +--------------------+
# |     _corrupt_record|
# +--------------------+
# |{"event": "compra...|
# |{"event": "login"...|
# +--------------------+
```

Ou:
```
AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column.
```

**Causa:** Múltiplas causas possíveis:
- Arquivo JSON com registros multi-linha (um objeto JSON ocupa várias linhas) mas `multiLine` não está habilitado
- Arquivo malformado (vírgula extra, aspas não fechadas)
- Arrays aninhados que o Spark não consegue inferir como schema flat

**Solução:**
```python
# Caso 1: JSON multi-linha (pretty-printed)
# Arquivo contém:
# {
#   "event": "compra",
#   "valor": 150.00,
#   "timestamp": "2023-06-15T10:30:00"
# }

df = spark.read \
    .option("multiLine", "true") \
    .json("data/parceiro_b/eventos.json")

# Caso 2: Capturar e investigar registros corrompidos
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("data/parceiro_b/eventos.json")

# Filtrar registros bons vs corrompidos:
df_validos = df.filter(df["_corrupt_record"].isNull())
df_corrompidos = df.filter(df["_corrupt_record"].isNotNull())
print(f"Válidos: {df_validos.count()}, Corrompidos: {df_corrompidos.count()}")
df_corrompidos.show(truncate=False)  # Investigar o que deu errado

# Caso 3: Array de objetos na raiz do arquivo
# Se o arquivo é: [{"a": 1}, {"a": 2}, {"a": 3}]
df = spark.read \
    .option("multiLine", "true") \
    .json("data/parceiro_b/eventos_array.json")

# Caso 4: Schema explícito para evitar inferência incorreta
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema_evento = StructType([
    StructField("event", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
])

df = spark.read \
    .schema(schema_evento) \
    .option("multiLine", "true") \
    .json("data/parceiro_b/eventos.json")
```

**Prevenção:**
- Sempre use `mode("PERMISSIVE")` com `columnNameOfCorruptRecord` para capturar problemas
- Envie registros corrompidos para uma pasta de quarentena na camada Bronze
- Defina schema explícito para arquivos JSON de parceiros (não confie em inferência)
- Valide amostras do JSON antes de processar o lote completo

---

## 5. Problema de Arquivos Pequenos (Small Files Problem)

**Sintoma:**
```python
# A leitura de um diretório com milhares de arquivos pequenos é muito lenta:
import time
start = time.time()
df = spark.read.parquet("data/silver/vendas/")  # 5000 arquivos de 10KB cada
df.count()
print(f"Tempo: {time.time() - start:.1f}s")
# Tempo: 45.2s  ← absurdamente lento para 50MB de dados
```

```bash
# Verificar a situação:
ls data/silver/vendas/ano=2023/mes=01/ | wc -l
# 500  ← 500 arquivos tiny em uma única partição!

ls -la data/silver/vendas/ano=2023/mes=01/ | head -5
# -rw-r--r-- 1 jovyan users  8192 part-00000.snappy.parquet
# -rw-r--r-- 1 jovyan users 12288 part-00001.snappy.parquet
# (cada arquivo < 128KB = problema!)
```

**Causa:** Cada `append` gera novos arquivos sem consolidar os existentes. Quando o pipeline roda muitas vezes (ex: ingestão a cada minuto) ou o DataFrame tem muitas partições no momento do write, o diretório acumula milhares de arquivos pequenos. O overhead de abrir/fechar cada arquivo domina o tempo de leitura.

**Solução:**
```python
# 1. Compactar antes de gravar (coalesce/repartition):
df_silver.coalesce(1).write \
    .mode("overwrite") \
    .parquet("data/silver/vendas_compactado/")
# coalesce(1) = 1 arquivo por partição (bom para datasets < 1GB)

# 2. Controlar número de arquivos por partição:
df_silver.repartition(4).write \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .parquet("data/silver/vendas/")
# Resultado: ~4 arquivos por partição de ano/mes

# 3. Job de compactação periódico (maintenance):
df_fragmentado = spark.read.parquet("data/silver/vendas/")
df_fragmentado.repartition(4) \
    .write \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .parquet("data/silver/vendas_temp/")

# Depois de validar, substituir:
import shutil
shutil.rmtree("data/silver/vendas/")
shutil.move("data/silver/vendas_temp/", "data/silver/vendas/")

# 4. Configurar tamanho máximo por arquivo na escrita:
df_silver.write \
    .option("maxRecordsPerFile", 100000) \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .parquet("data/silver/vendas/")
```

**Prevenção:**
- Regra geral: arquivos Parquet devem ter entre 128MB e 1GB cada
- Evite `mode("append")` em alta frequência sem compactação periódica
- Use `coalesce()` antes do write quando o DataFrame tem poucas linhas
- Monitore o número de arquivos por partição regularmente

---

## 6. Dynamic Partition Overwrite Não Funciona

**Sintoma:**
```python
# Intenção: reprocessar APENAS Janeiro/2023, sem afetar outros meses
df_janeiro = df.filter((col("ano") == 2023) & (col("mes") == 1))

df_janeiro.write \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .parquet("data/silver/vendas/")

# Resultado: TODAS as partições foram deletadas! Só Janeiro sobreviveu!
```

**Causa:** O `mode("overwrite")` padrão do Spark apaga TODO o diretório de destino antes de gravar, independente do `partitionBy`. Para sobrescrever apenas as partições presentes no DataFrame, é necessário ativar o **dynamic partition overwrite**.

**Solução:**
```python
# ✅ Ativar dynamic partition overwrite ANTES da escrita:
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_janeiro.write \
    .partitionBy("ano", "mes") \
    .mode("overwrite") \
    .parquet("data/silver/vendas/")
# Agora APENAS ano=2023/mes=1 é sobrescrito. Outros meses permanecem intactos.

# Verificar configuração atual:
print(spark.conf.get("spark.sql.sources.partitionOverwriteMode"))
# "dynamic" ← correto
# "static"  ← padrão perigoso (apaga tudo)
```

```python
# Alternativa: Configurar na SparkSession ao criá-la
spark = SparkSession.builder \
    .appName("DataFlow-Aula03") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
```

**Prevenção:**
- **Sempre** configure `partitionOverwriteMode = dynamic` quando usar overwrite + partitionBy
- Adicione essa configuração no início de todo notebook que faz escrita particionada
- Faça backup ou snapshot antes de testar overwrite pela primeira vez
- Valide com `spark.read.parquet(path).select("ano", "mes").distinct().show()` após o write

---

## 7. unionByName Falha com Colunas Diferentes

**Sintoma:**
```python
df_parceiro_a = spark.read.csv("data/parceiro_a/vendas.csv", header=True)
df_parceiro_b = spark.read.json("data/parceiro_b/vendas.json")

df_unificado = df_parceiro_a.unionByName(df_parceiro_b)

# AnalysisException: Cannot resolve column name "desconto" among
# (order_id, customer_id, product_id, quantity, unit_price, total_amount);
```

Ou com tipos incompatíveis:
```
AnalysisException: Union can only be performed on tables with compatible column types.
The 3rd column of the second table is 'STRING' type which is not compatible with
'DOUBLE' at the same column of the first table.
```

**Causa:** Os DataFrames de parceiros diferentes têm:
- Colunas extras que não existem no outro (`desconto` só no parceiro B)
- Colunas com mesmo nome mas tipos diferentes (`total_amount` como String vs Double)
- Nomes levemente diferentes (`data_pedido` vs `order_date`)

**Solução:**
```python
# Caso 1: Colunas extras → usar allowMissingColumns
df_unificado = df_parceiro_a.unionByName(df_parceiro_b, allowMissingColumns=True)
# Colunas ausentes são preenchidas com null

# Caso 2: Tipos incompatíveis → alinhar antes do union
from pyspark.sql.functions import col

df_parceiro_b = df_parceiro_b \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("quantity", col("quantity").cast("integer"))

df_unificado = df_parceiro_a.unionByName(df_parceiro_b, allowMissingColumns=True)

# Caso 3: Nomes de colunas diferentes → renomear para schema canônico
df_parceiro_b = df_parceiro_b \
    .withColumnRenamed("data_pedido", "order_date") \
    .withColumnRenamed("valor_total", "total_amount") \
    .withColumnRenamed("cod_cliente", "customer_id")

# Caso 4: Função genérica para alinhar schemas
def alinhar_schema(df, schema_alvo):
    """Alinha DataFrame ao schema alvo: adiciona colunas faltantes, remove extras."""
    from pyspark.sql.functions import lit
    
    for campo in schema_alvo.fields:
        if campo.name not in df.columns:
            df = df.withColumn(campo.name, lit(None).cast(campo.dataType))
        else:
            df = df.withColumn(campo.name, col(campo.name).cast(campo.dataType))
    
    return df.select([campo.name for campo in schema_alvo.fields])

# Uso:
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

schema_canonico = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
])

df_a = alinhar_schema(df_parceiro_a, schema_canonico)
df_b = alinhar_schema(df_parceiro_b, schema_canonico)
df_unificado = df_a.unionByName(df_b)
```

**Prevenção:**
- Defina um schema canônico na camada Silver e force todos os parceiros a se conformarem
- Normalize schemas na camada Bronze → Silver (nunca faça union de dados raw)
- Use `allowMissingColumns=True` como padrão ao unir fontes heterogêneas
- Mantenha um mapeamento de-para documentado para cada parceiro

---

## 8. Parquet Write Falha Silenciosamente ou Perde Dados

**Sintoma:**
```python
# Cenário A: Write parece funcionar, mas dados sumiram
df_vendas.write.mode("overwrite").parquet("data/silver/vendas/")
df_verificacao = spark.read.parquet("data/silver/vendas/")
print(f"Antes: {df_vendas.count()}, Depois: {df_verificacao.count()}")
# Antes: 100000, Depois: 0  ← dados perdidos!
```

```python
# Cenário B: Erro de permissão/disco
df.write.parquet("data/silver/vendas/")
# java.io.IOException: No space left on device
# Ou:
# java.io.IOException: Permission denied
```

```python
# Cenário C: Overwrite destruiu dados que não deveriam ser afetados
df_janeiro.write.mode("overwrite").parquet("data/silver/vendas/")
# Deletou TODOS os meses! (ver Problema 6 - Dynamic Partition Overwrite)
```

**Causa:**
- **Cenário A:** O DataFrame estava vazio (filtro incorreto retornou 0 linhas) e o `overwrite` limpou o diretório
- **Cenário B:** Disco cheio ou permissões insuficientes no diretório de destino
- **Cenário C:** `mode("overwrite")` com `partitionOverwriteMode=static` (padrão) apaga tudo

**Solução:**
```python
# 1. SEMPRE validar antes de sobrescrever:
contagem = df_vendas.count()
if contagem == 0:
    raise ValueError("DataFrame vazio! Abortando write para não destruir dados existentes.")

print(f"Gravando {contagem} registros...")
df_vendas.write.mode("overwrite").parquet("data/silver/vendas/")

# 2. Verificar espaço em disco antes de gravar:
import shutil
uso = shutil.disk_usage("data/")
espaco_livre_gb = uso.free / (1024**3)
print(f"Espaço livre: {espaco_livre_gb:.1f} GB")
if espaco_livre_gb < 1.0:
    raise IOError("Espaço em disco insuficiente! Libere espaço antes de continuar.")

# 3. Padrão write-then-swap (atômico):
import os

caminho_temp = "data/silver/vendas_temp/"
caminho_final = "data/silver/vendas/"

# Gravar em diretório temporário
df_vendas.write.mode("overwrite").parquet(caminho_temp)

# Validar o que foi gravado
df_check = spark.read.parquet(caminho_temp)
assert df_check.count() == contagem, "Contagem diverge! Não substituir."

# Só então substituir
if os.path.exists(caminho_final):
    shutil.rmtree(caminho_final)
os.rename(caminho_temp, caminho_final)
print("✅ Dados gravados e validados com sucesso.")

# 4. Verificar permissões:
import os
print(oct(os.stat("data/silver/").st_mode))  # Deve ser 0o755 ou 0o775
```

**Prevenção:**
- Nunca faça `overwrite` sem verificar `.count()` antes — DataFrames vazios destroem tudo
- Use o padrão write-then-swap para escritas críticas
- Configure `partitionOverwriteMode = dynamic` (ver Problema 6)
- Implemente backups automáticos antes de sobrescrever dados de produção
- Monitore espaço em disco com alertas quando < 20% livre

---

## Quick Reference: Tabela de Diagnóstico Rápido

| Sintoma | Causa Provável | Solução Rápida |
|---------|---------------|----------------|
| Caracteres estranhos (Ã©, Ã£o) | Encoding errado na leitura | Adicionar `encoding="ISO-8859-1"` |
| `INCOMPATIBLE_DATA_FOR_TABLE` | Tipos diferentes no append | Cast das colunas antes de gravar |
| `Column does not exist` no write | Colunas extras/faltantes no append | `mergeSchema=true` ou alinhar schema |
| DataFrame vazio mas arquivos existem | Path no nível errado / partition discovery | Verificar estrutura de diretório |
| `_corrupt_record` no JSON | multiLine não habilitado | `option("multiLine", "true")` |
| Leitura muito lenta (muitos arquivos) | Small files problem | `coalesce()` ou compactação periódica |
| Overwrite apaga partições erradas | `partitionOverwriteMode=static` | Configurar `dynamic` |
| `Cannot resolve column name` no union | Schemas diferentes entre parceiros | `unionByName(allowMissingColumns=True)` |
| Tipos incompatíveis no union | String vs Double na mesma coluna | Cast para tipo comum antes do union |
| Dados sumiram após overwrite | DataFrame vazio + overwrite | Validar `.count() > 0` antes de write |
| `No space left on device` | Disco cheio | Limpar arquivos temporários e compactar |
| `Permission denied` na escrita | Permissões de diretório | `chmod 775` no diretório de destino |

---

## Fluxo de Diagnóstico: Árvore de Decisão

```
Erro na LEITURA?
├── CSV com caracteres estranhos → Problema 1 (Encoding)
├── JSON com _corrupt_record → Problema 4 (JSON Parsing)
├── DataFrame vazio mas arquivos existem → Problema 3 (Partition Discovery)
└── Leitura extremamente lenta → Problema 5 (Small Files)

Erro na ESCRITA?
├── Schema mismatch / AnalysisException → Problema 2 (Schema Mismatch)
├── Partições erradas foram deletadas → Problema 6 (Dynamic Overwrite)
├── Dados desapareceram → Problema 8 (Write silencioso)
└── Permission denied / No space → Problema 8 (Disco/Permissões)

Erro na UNIFICAÇÃO de fontes?
├── Column not found → Problema 7 (unionByName)
├── Tipos incompatíveis → Problema 7 (Cast + union)
└── Schemas divergentes → Problema 7 (Alinhar schema canônico)
```

---

> **Marina:** "A regra de ouro da ingestão multi-fonte: nunca confie nos dados que chegam. Valide encoding, valide schema, valide contagem. Se parece que funcionou sem erros, verifique de novo — os bugs mais perigosos são os silenciosos que corrompem dados sem avisar. Na DataFlow, todo pipeline de ingestão tem pelo menos três checkpoints de validação antes de gravar na camada Silver."
