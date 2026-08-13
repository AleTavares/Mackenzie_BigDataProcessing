# Troubleshooting — Aula 1: Problemas Comuns e Soluções

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Depois de ajudar dezenas de equipes a configurar Spark pela primeira vez, compilei os problemas mais comuns que encontro. A maioria tem solução simples — o segredo é saber interpretar a mensagem de erro. Vou compartilhar com vocês o que aprendi na prática."

---

## 1. SparkSession não conecta ao Master

**Sintoma:**
```
Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
...
java.net.ConnectException: Connection refused (Connection refused)
  spark://spark-master:7077
```

**Causa:** O container `spark-master` não está rodando, ainda está inicializando, ou o nome do host está incorreto na configuração do SparkSession.

**Solução:**
```bash
# 1. Verificar se o master está rodando e healthy:
docker compose -f shared/docker-compose.yml ps spark-master

# 2. Verificar os logs do master:
docker compose -f shared/docker-compose.yml logs spark-master

# 3. Testar conectividade de rede a partir do Jupyter:
docker exec jupyter-notebook ping -c 3 spark-master

# 4. Se o master não estiver healthy, reiniciar:
docker compose -f shared/docker-compose.yml restart spark-master
# Aguarde ~30 segundos antes de tentar novamente no notebook
```

**Prevenção:** Sempre verifique o Spark Master UI (http://localhost:8080) antes de iniciar o notebook. Se a página carregar e exibir "Spark Master at spark://spark-master:7077", o master está pronto para receber conexões.

---

## 2. Erro de memória insuficiente (OOM)

**Sintoma:**
```
java.lang.OutOfMemoryError: Java heap space
  at java.util.Arrays.copyOf(Arrays.java:3236)
```
Ou o container morre silenciosamente (status "Exited (137)").

**Causa:** O executor ou driver Spark está tentando processar mais dados do que a memória alocada permite. Comum ao executar `collect()` em DataFrames grandes ou ao usar `inferSchema` em arquivos muito grandes.

**Solução:**
```python
# Opção 1: Aumentar memória do driver no SparkSession
spark = SparkSession.builder \
    .appName("DataFlow-Aula01") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Opção 2: Evitar collect() em DataFrames grandes
# ❌ Ruim — traz tudo para a memória do driver:
todos_dados = df_vendas.collect()

# ✅ Bom — usar show() ou take() para amostra:
df_vendas.show(20)
amostra = df_vendas.take(100)
```

```bash
# Opção 3: Aumentar RAM no Docker Desktop
# Settings → Resources → Memory → 8 GB (mínimo) ou 12 GB (ideal)
```

**Prevenção:** Nunca use `collect()` sem antes verificar o tamanho do DataFrame com `df.count()`. Para DataFrames com mais de 100K linhas, trabalhe sempre com `show()`, `take()` ou `toPandas()` em subsets filtrados.

---

## 3. inferSchema falha ou tipos errados

**Sintoma:**
```python
df_vendas.printSchema()
# Mostra:
# root
#  |-- quantity: string (nullable = true)    ← deveria ser integer!
#  |-- unit_price: string (nullable = true)  ← deveria ser double!
#  |-- total_amount: string (nullable = true)
```

**Causa:** O `inferSchema` analisa apenas uma amostra das linhas e pode falhar se houver valores mistos, campos vazios nas primeiras linhas, ou se o separador do CSV estiver incorreto.

**Solução:**
```python
# Opção 1: Definir schema manualmente (mais confiável)
from pyspark.sql.types import StructType, StructField, StringType, \
    IntegerType, DoubleType, TimestampType

schema_vendas = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("order_date", TimestampType(), False),
    StructField("payment_method", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("shipping_state", StringType(), True),
    StructField("status", StringType(), True),
])

df_vendas = spark.read.csv("data/vendas_2023.csv", header=True, schema=schema_vendas)

# Opção 2: Aumentar a amostra do inferSchema
df_vendas = spark.read.csv(
    "data/vendas_2023.csv",
    header=True,
    inferSchema=True,
    samplingRatio=0.5  # Analisa 50% das linhas (padrão é ~1000 linhas)
)
```

**Prevenção:** Em produção, sempre defina o schema explicitamente. O `inferSchema=True` é prático para exploração rápida, mas nunca confie nele para pipelines automatizados.

---

## 4. Arquivo não encontrado

**Sintoma:**
```
AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/home/jovyan/work/data/vendas_2023.csv
```

**Causa:** O caminho do arquivo está incorreto, a pasta `data/` não foi montada corretamente no container, ou os datasets ainda não foram gerados.

**Solução:**
```python
# 1. Verificar qual diretório o Jupyter está usando:
import os
print(os.getcwd())          # Diretório atual
print(os.listdir("data/"))  # Listar conteúdo da pasta data

# 2. Se "data/" não existe, verificar o ponto de montagem:
print(os.listdir("/home/jovyan/work/"))  # Raiz do workspace Jupyter
```

```bash
# 3. Verificar se os datasets foram gerados:
ls datasets/aula_01/

# 4. Se estiverem vazios, gerar novamente:
python datasets/gerar_datasets.py

# 5. Reiniciar o Jupyter para aplicar o volume:
docker compose -f shared/docker-compose.yml restart jupyter-notebook
```

**Prevenção:** Antes de iniciar o lab, confirme que a pasta `data/` aparece no painel lateral do Jupyter e contém os arquivos esperados (`vendas_2023.csv`, `produtos.csv`).

---

## 5. Jupyter kernel morre

**Sintoma:**
- Mensagem: "Kernel Restarting — The kernel appears to have died. It will restart automatically."
- A célula exibe `[*]` indefinidamente e depois o kernel reinicia
- Todas as variáveis são perdidas após o restart

**Causa:** O kernel Python do Jupyter ficou sem memória (OOM killer do sistema operacional matou o processo). Isso ocorre quando o Spark tenta alocar mais memória do que o container permite.

**Solução:**
```python
# 1. Após o restart do kernel, recriar a SparkSession com menos memória:
spark = SparkSession.builder \
    .appName("DataFlow-Aula01") \
    .master("local[*]") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Evitar operações que explodem a memória:
# ❌ Ruim:
df_grande.toPandas()  # Converte TUDO para pandas na memória do driver

# ✅ Bom:
df_grande.limit(1000).toPandas()  # Apenas 1000 linhas para pandas
```

```bash
# 3. Aumentar limite de memória do container Jupyter no docker-compose.yml:
# deploy:
#   resources:
#     limits:
#       memory: 4g
```

**Prevenção:** Monitore o uso de memória no Docker Desktop (dashboard). Se o container Jupyter estiver usando >90% da memória alocada, reduza o tamanho das operações ou aumente o limite.

---

## 6. SparkSession já existe

**Sintoma:**
```
IllegalStateException: Only one SparkContext may be running in this JVM.
  Set spark.driver.allowMultipleContexts = true to force (not recommended)
```

**Causa:** Você tentou criar uma nova SparkSession enquanto já existe uma ativa. Isso acontece quando você executa a célula de criação da SparkSession mais de uma vez, ou quando reinicia o notebook sem encerrar a sessão anterior.

**Solução:**
```python
# Opção 1: Usar getOrCreate() (RECOMENDADO — já retorna a sessão existente)
spark = SparkSession.builder \
    .appName("DataFlow-Aula01") \
    .master("spark://spark-master:7077") \
    .getOrCreate()  # ← Reutiliza se já existir

# Opção 2: Encerrar a sessão existente antes de criar nova
spark.stop()  # Encerra a sessão atual

# Agora pode criar nova com configurações diferentes:
spark = SparkSession.builder \
    .appName("DataFlow-Aula01-v2") \
    .master("local[*]") \
    .getOrCreate()
```

**Prevenção:** Sempre use `.getOrCreate()` ao invés de `.create()`. Isso garante que a mesma sessão é reutilizada se já existir, evitando conflitos.

---

## 7. Resultado show() vazio

**Sintoma:**
```python
df_filtrado = df_vendas.filter(col("estado") == "SP")
df_filtrado.show()
# +--------+-----------+----------+
# |order_id|customer_id|quantidade|
# +--------+-----------+----------+
# +--------+-----------+----------+
# (nenhuma linha retornada)
```

**Causa:** O nome da coluna ou valor usado no filtro não corresponde exatamente ao que existe no DataFrame. Erros comuns: case sensitivity (`"SP"` vs `"sp"`), nome da coluna errado (`"estado"` vs `"shipping_state"`), espaços extras nos valores.

**Solução:**
```python
# 1. Verificar os nomes exatos das colunas:
df_vendas.printSchema()
df_vendas.columns  # Lista de nomes de colunas

# 2. Verificar valores únicos da coluna (para encontrar o valor correto):
df_vendas.select("shipping_state").distinct().show(50)

# 3. Usar filtro case-insensitive se necessário:
from pyspark.sql.functions import upper
df_filtrado = df_vendas.filter(upper(col("shipping_state")) == "SP")

# 4. Verificar se há espaços extras:
from pyspark.sql.functions import trim
df_limpo = df_vendas.withColumn("shipping_state", trim(col("shipping_state")))
```

**Prevenção:** Sempre execute `printSchema()` e `df.select("coluna").distinct().show()` antes de aplicar filtros. Isso evita perder tempo com nomes incorretos de colunas ou valores inexistentes.

---

## 8. Operação muito lenta

**Sintoma:**
- `groupBy(...).agg(...)` trava por vários minutos em dataset de apenas 100K registros
- A barra de progresso do Spark não avança
- O Spark UI (http://localhost:8080) mostra o job "Running" sem completar stages

**Causa:** Geralmente ocorre por shuffle excessivo (muitas partições), dados desbalanceados (data skew), ou falta de memória fazendo o Spark derramar dados para disco (spill).

**Solução:**
```python
# 1. Reduzir número de partições de shuffle (padrão: 200, excessivo para 100K linhas):
spark.conf.set("spark.sql.shuffle.partitions", "4")

# 2. Verificar o número atual de partições:
print(f"Partições do DataFrame: {df_vendas.rdd.getNumPartitions()}")

# 3. Reparticionar se necessário:
df_vendas = df_vendas.repartition(4)

# 4. Para datasets pequenos (<500K linhas), usar mode local:
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

**Prevenção:** Para datasets de até 1M de linhas no ambiente do lab, configure `spark.sql.shuffle.partitions` entre 4 e 8. O padrão de 200 partições é dimensionado para clusters de produção com terabytes de dados.

---

## 9. Import errors

**Sintoma:**
```
ModuleNotFoundError: No module named 'pyspark'
```
Ou:
```
ModuleNotFoundError: No module named 'pyspark.sql.functions'
```

**Causa:** O PySpark não está instalado no ambiente Python do kernel Jupyter, ou o kernel está usando um Python diferente do que tem o PySpark instalado.

**Solução:**
```python
# 1. Verificar se pyspark está instalado:
import subprocess
result = subprocess.run(["pip", "list"], capture_output=True, text=True)
print([l for l in result.stdout.split("\n") if "pyspark" in l.lower()])

# 2. Instalar pyspark se necessário (dentro do notebook):
!pip install pyspark==3.5.0

# 3. Verificar qual Python o kernel está usando:
import sys
print(sys.executable)
```

```bash
# 4. Se estiver usando Docker (cenário do lab), verificar se o container está correto:
docker exec jupyter-notebook pip list | grep pyspark

# 5. Reinstalar se necessário:
docker exec jupyter-notebook pip install pyspark==3.5.0
```

**Prevenção:** O container `jupyter/pyspark-notebook` do lab já vem com PySpark pré-instalado. Se você estiver vendo esse erro, provavelmente está executando o notebook fora do Docker (no Python local da máquina). Confirme que está acessando o Jupyter via http://localhost:8888 (container).

---

## 10. DataFrame operations retornando resultados errados

**Sintoma:**
```python
# Esperava filtrar pedidos com valor > 100 E estado = "SP"
# Mas o resultado inclui linhas de outros estados ou valores menores

df_filtrado = df_vendas.filter(
    col("total_amount") > 100 & col("shipping_state") == "SP"
)
```

**Causa:** Erro de precedência de operadores em Python. O operador `&` tem precedência maior que `>` e `==`, então a expressão é interpretada como `total_amount > (100 & col("shipping_state")) == "SP"` — completamente diferente do esperado.

**Solução:**
```python
# ✅ CORRETO — usar parênteses em CADA condição:
df_filtrado = df_vendas.filter(
    (col("total_amount") > 100) & (col("shipping_state") == "SP")
)

# ✅ Alternativa — usar múltiplos filter encadeados:
df_filtrado = df_vendas \
    .filter(col("total_amount") > 100) \
    .filter(col("shipping_state") == "SP")

# ❌ ERRADO — usar "and" ao invés de "&":
# df_filtrado = df_vendas.filter(
#     (col("total_amount") > 100) and (col("shipping_state") == "SP")
# )
# Isso gera: ValueError: Cannot convert column into bool
```

**Prevenção:** Regras de ouro para filtros no PySpark:
1. **Sempre** coloque cada condição entre parênteses: `(condição1) & (condição2)`
2. Use `&` para AND, `|` para OR, `~` para NOT (nunca `and`, `or`, `not`)
3. Na dúvida, encadeie múltiplos `.filter()` — é equivalente e mais legível

---

## Quick Reference: Comandos de Debugging

### Inspecionar o DataFrame

```python
# Estrutura (tipos das colunas)
df.printSchema()

# Primeiras N linhas formatadas
df.show(10, truncate=False)

# Estatísticas descritivas
df.describe().show()

# Contagem total de linhas
print(f"Total: {df.count()} linhas")

# Valores únicos de uma coluna
df.select("coluna").distinct().show()

# Verificar nulls por coluna
from pyspark.sql.functions import col, count, when, isnan, isnull
df.select([
    count(when(isnull(c), c)).alias(c) for c in df.columns
]).show()
```

### Monitorar o Spark

```python
# Ver configurações ativas da sessão
print(spark.sparkContext.getConf().getAll())

# Verificar número de partições
print(f"Partições: {df.rdd.getNumPartitions()}")

# Plano de execução (como o Spark vai processar)
df.explain(mode="simple")

# Verificar se a sessão está ativa
print(f"Sessão ativa: {spark.sparkContext._jsc.sc().isStopped() == False}")
```

### Docker e Ambiente

```bash
# Status dos containers
docker compose -f shared/docker-compose.yml ps

# Logs de um container específico
docker compose -f shared/docker-compose.yml logs spark-master --tail=50

# Uso de recursos (CPU/RAM) dos containers
docker stats --no-stream

# Reiniciar tudo do zero
docker compose -f shared/docker-compose.yml down
docker compose -f shared/docker-compose.yml up -d

# Resetar dados para estado original
./shared/reset_data.sh
```

### URLs Importantes

| Serviço | URL | Função |
|---------|-----|--------|
| Spark Master UI | http://localhost:8080 | Monitorar cluster, workers, jobs |
| Jupyter Notebook | http://localhost:8888 | Interface para escrever código |
| Spark Job UI | http://localhost:4040 | Detalhes do job em execução (só aparece com job ativo) |

---

> **Carlos:** "Com esse guia, vocês conseguem resolver 90% dos problemas que aparecem no primeiro contato com Spark. O mais importante é: leia a mensagem de erro com calma. Quase sempre ela indica exatamente onde está o problema. E lembrem-se — se nada funcionar, `docker compose down && docker compose up -d` resolve a maioria dos casos."
