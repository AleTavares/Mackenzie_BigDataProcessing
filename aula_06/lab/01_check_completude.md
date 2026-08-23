# Exercício 1 — Check de Completude (Nulls e NaN)

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Lembra que a Ana reportou que os relatórios estão saindo com números inconsistentes? A Marina convocou reunião de emergência e definiu: nenhum dado entra na camada Gold sem passar por validação. Nosso primeiro check será de **completude** — verificar se os campos obrigatórios estão preenchidos. Parece simples, mas você vai se surpreender com o estrago que 5% de nulls causam em agregações."

## Objetivos

Ao final deste exercício, você será capaz de:

- Carregar um dataset com problemas de qualidade e explorá-lo
- Identificar quais colunas possuem valores nulos, NaN ou vazios
- Construir uma função reutilizável `check_completeness()` que calcula % de preenchimento por coluna
- Definir thresholds de aceitação por coluna (ex: order_id = 100%, customer_id = 95%)
- Gerar um relatório pass/fail de completude
- Visualizar os resultados em formato tabular
- Entender o conceito de quarentena para registros que falham no check

## Pré-requisitos

- Ambiente Docker rodando (Spark + Jupyter)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `dados_sujos/vendas_problemas.parquet` disponível na pasta `data/aula_06/`

## Duração Estimada

⏱️ ~20 minutos

---

## Passo 1: Criar a SparkSession

**Descrição:** Vamos iniciar nossa sessão Spark com uma configuração orientada a qualidade de dados. A aplicação se chama "DataFlow-QualityCheck" para identificá-la facilmente no Spark UI.

**Código:**

```python
from pyspark.sql import SparkSession

# Criar SparkSession para o módulo de qualidade
spark = SparkSession.builder \
    .appName("DataFlow-Aula06-QualityCheck") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

print(f"✅ SparkSession criada com sucesso!")
print(f"   Versão do Spark: {spark.version}")
print(f"   App: {spark.sparkContext.appName}")
```

**Resultado esperado:**

```
✅ SparkSession criada com sucesso!
   Versão do Spark: 3.5.x
   App: DataFlow-Aula06-QualityCheck
```

---

## Passo 2: Carregar o Dataset com Problemas

**Descrição:** O dataset `vendas_problemas.parquet` contém ~51.500 registros de vendas com problemas intencionais de qualidade: nulls, duplicatas, valores negativos e datas futuras. Vamos carregá-lo e fazer uma exploração inicial.

**Código:**

```python
# Carregar o dataset "sujo" que a equipe de ingestão identificou
df_vendas = spark.read.parquet("data/aula_06/dados_sujos/vendas_problemas.parquet")

# Exploração inicial
print(f"📊 Registros carregados: {df_vendas.count():,}")
print(f"📋 Colunas: {len(df_vendas.columns)}")
print()
df_vendas.printSchema()
```

**Resultado esperado:**

```
📊 Registros carregados: 51,500
📋 Colunas: 12

root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- total_amount: double (nullable = true)
 |-- order_date: timestamp (nullable = true)
 |-- payment_method: string (nullable = true)
 |-- shipping_city: string (nullable = true)
 |-- shipping_state: string (nullable = true)
 |-- status: string (nullable = true)
 |-- partner_source: string (nullable = true)
```

**Código (visualizar amostra):**

```python
# Visualizar os primeiros registros
df_vendas.show(5, truncate=False)
```

**Explicação técnica:**

- O dataset foi gerado a partir de 50K registros limpos, com ~3% de duplicatas adicionadas (~1.500 registros extras)
- O formato Parquet preserva tipos de dados corretamente (diferente de CSV onde tudo vira string)
- Note que todas as colunas têm `nullable = true` — o schema permite nulos, mas nosso negócio pode exigir preenchimento

> **💡 Dica de Carlos:** "Sempre comece com `count()` e `printSchema()`. Se o count vier menor que o esperado, pode ter problema no path ou no formato. Se o schema mostrar tudo como string, a inferência falhou."

---

## Passo 3: Inspeção Manual — Onde Estão os Nulls?

**Descrição:** Antes de automatizar, vamos entender o problema manualmente. O Spark oferece funções para contar nulos, NaN e strings vazias. Vamos inspecionar cada coluna e quantificar o impacto.

**Código:**

```python
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum

# Contar nulos por coluna usando agregação
print("🔍 Contagem de valores nulos por coluna:")
print("=" * 55)

df_null_counts = df_vendas.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_vendas.columns
])

df_null_counts.show(truncate=False)
```

**Resultado esperado:**

```
🔍 Contagem de valores nulos por coluna:
=======================================================
+--------+-----------+----------+--------+----------+------------+----------+--------------+-------------+--------------+------+--------------+
|order_id|customer_id|product_id|quantity|unit_price|total_amount|order_date|payment_method|shipping_city|shipping_state|status|partner_source|
+--------+-----------+----------+--------+----------+------------+----------+--------------+-------------+--------------+------+--------------+
|0       |~2575      |0         |0       |0         |0           |0         |~2575         |~2575        |~2575         |0     |0             |
+--------+-----------+----------+--------+----------+------------+----------+--------------+-------------+--------------+------+--------------+
```

**Código (versão mais legível com percentuais):**

```python
# Versão detalhada com percentuais
total_registros = df_vendas.count()

print(f"📊 Total de registros: {total_registros:,}")
print(f"\n{'Coluna':<20} {'Nulos':>8} {'% Nulo':>10} {'Preenchido':>12}")
print("-" * 55)

for col_name in df_vendas.columns:
    null_count = df_vendas.filter(col(col_name).isNull()).count()
    pct_null = (null_count / total_registros) * 100
    pct_filled = 100 - pct_null
    
    # Emoji indica severidade
    emoji = "✅" if pct_null == 0 else "⚠️" if pct_null < 5 else "🚨"
    
    print(f"{emoji} {col_name:<18} {null_count:>8,} {pct_null:>9.2f}% {pct_filled:>10.2f}%")
```

**Resultado esperado:**

```
📊 Total de registros: 51,500

Coluna               Nulos     % Nulo   Preenchido
-------------------------------------------------------
✅ order_id                  0     0.00%     100.00%
⚠️ customer_id          ~2,575     5.00%      95.00%
✅ product_id                0     0.00%     100.00%
✅ quantity                  0     0.00%     100.00%
✅ unit_price                0     0.00%     100.00%
✅ total_amount              0     0.00%     100.00%
✅ order_date                0     0.00%     100.00%
⚠️ payment_method       ~2,575     5.00%      95.00%
⚠️ shipping_city        ~2,575     5.00%      95.00%
⚠️ shipping_state       ~2,575     5.00%      95.00%
✅ status                    0     0.00%     100.00%
✅ partner_source            0     0.00%     100.00%
```

**Explicação técnica:**

- `col(c).isNull()` — detecta valores `NULL` (ausência de valor)
- `isnan(c)` — detecta valores `NaN` (Not a Number, específico de colunas numéricas)
- `when(condição, valor_verdadeiro).otherwise(valor_falso)` — expressão condicional (como IF/ELSE)
- `spark_sum(...)` — soma agregada sobre todo o DataFrame
- Usamos um list comprehension `[... for c in df_vendas.columns]` para gerar a expressão para todas as colunas de uma vez

> **💡 Dica de Carlos:** "Note que as colunas com nulls são exatamente `customer_id`, `payment_method`, `shipping_city` e `shipping_state` — todas com ~5%. Isso é consistente com um problema sistêmico, provavelmente uma falha na extração de um parceiro específico. Na vida real, esse padrão nos diria onde investigar."

---

## Passo 4: Construir a Função `check_completeness()`

**Descrição:** Agora que entendemos o problema, vamos criar uma função reutilizável. A Ana precisa de algo que possa ser chamado em qualquer DataFrame, com thresholds configuráveis por coluna. Esta função será usada nos próximos exercícios e no framework final.

**Código:**

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, lit
from typing import Dict, List, Optional

def check_completeness(
    df: DataFrame,
    columns: List[str],
    threshold: float = 0.95
) -> List[Dict]:
    """
    Verifica a completude (preenchimento) de colunas em um DataFrame.
    
    Parâmetros:
        df: DataFrame PySpark a ser verificado
        columns: Lista de colunas para verificar
        threshold: Percentual mínimo de preenchimento (0.0 a 1.0)
                   Padrão: 0.95 (95%)
    
    Retorna:
        Lista de dicionários com resultado por coluna:
        - column: nome da coluna
        - total_records: total de registros
        - non_null_count: registros preenchidos
        - null_count: registros nulos/NaN
        - completeness: percentual de preenchimento (0.0 a 1.0)
        - threshold: threshold aplicado
        - passed: True se completeness >= threshold
    """
    total = df.count()
    results = []
    
    for col_name in columns:
        # Verificar se a coluna existe no DataFrame
        if col_name not in df.columns:
            results.append({
                "column": col_name,
                "total_records": total,
                "non_null_count": 0,
                "null_count": total,
                "completeness": 0.0,
                "threshold": threshold,
                "passed": False,
                "error": f"Coluna '{col_name}' não existe no DataFrame"
            })
            continue
        
        # Contar registros não-nulos
        # Para colunas numéricas, também verificar NaN
        col_type = str(df.schema[col_name].dataType)
        
        if "Double" in col_type or "Float" in col_type:
            # Colunas numéricas: null OU NaN são considerados ausentes
            non_null = df.filter(
                col(col_name).isNotNull() & ~isnan(col(col_name))
            ).count()
        else:
            # Colunas não-numéricas: apenas null
            non_null = df.filter(col(col_name).isNotNull()).count()
        
        null_count = total - non_null
        completeness = non_null / total if total > 0 else 0.0
        
        results.append({
            "column": col_name,
            "total_records": total,
            "non_null_count": non_null,
            "null_count": null_count,
            "completeness": round(completeness, 4),
            "threshold": threshold,
            "passed": completeness >= threshold
        })
    
    return results

print("✅ Função check_completeness() definida com sucesso!")
```

**Resultado esperado:**

```
✅ Função check_completeness() definida com sucesso!
```

**Explicação técnica:**

- A função aceita qualquer DataFrame PySpark — é genérica e reutilizável
- Distinguimos entre colunas numéricas (verifica `isNull` E `isnan`) e colunas de texto (apenas `isNull`)
- `NaN` (Not a Number) é diferente de `NULL`: NaN é um valor numérico especial (IEEE 754), NULL é ausência de valor. Ambos indicam dado faltante
- O `threshold` padrão de 0.95 (95%) significa que até 5% de nulos é aceitável
- Retornamos uma lista de dicts para facilitar conversão em DataFrame ou visualização

> **💡 Dica de Carlos:** "Essa função vai ser a base do nosso framework de qualidade. Note que ela é pura — recebe dados, retorna resultados, sem efeitos colaterais. Isso facilita testes e composição com outras verificações."

---

## Passo 5: Definir Thresholds e Executar o Check

**Descrição:** Cada coluna tem um nível de criticidade diferente para o negócio. A Ana definiu com o time de produto quais são os thresholds aceitáveis. Vamos aplicar a função com thresholds específicos.

**Código:**

```python
# Definir thresholds de completude por coluna (requisito de negócio)
# Conversado com Ana (PO) e aprovado por Marina (CTO)
colunas_criticas = {
    "order_id": 1.00,        # 100% — identificador único, jamais pode ser nulo
    "customer_id": 0.95,     # 95%  — aceita até 5% de anônimos/guests
    "product_id": 1.00,      # 100% — necessário para relatórios de produto
    "quantity": 1.00,        # 100% — campo numérico obrigatório
    "unit_price": 1.00,      # 100% — necessário para cálculo de faturamento
    "total_amount": 0.99,    # 99%  — campo financeiro crítico
    "order_date": 1.00,      # 100% — necessário para particionamento temporal
    "payment_method": 0.90,  # 90%  — informativo, pode faltar em migração
    "shipping_city": 0.90,   # 90%  — informativo
    "shipping_state": 0.90,  # 90%  — usado em relatórios regionais
}

# Executar o check para cada coluna com seu threshold específico
print("🔍 Executando check de completude...")
print("=" * 70)

all_results = []
for col_name, thresh in colunas_criticas.items():
    result = check_completeness(df_vendas, [col_name], threshold=thresh)
    all_results.extend(result)

print(f"✅ Check executado para {len(all_results)} colunas")
```

**Resultado esperado:**

```
🔍 Executando check de completude...
======================================================================
✅ Check executado para 10 colunas
```

---

## Passo 6: Gerar Relatório Pass/Fail

**Descrição:** Agora vamos transformar os resultados em um relatório visual que qualquer pessoa da equipe consiga interpretar. Usaremos um DataFrame Spark para formatar e exibir os resultados.

**Código:**

```python
# Gerar relatório formatado
print("\n📋 RELATÓRIO DE COMPLETUDE — DataFlow Analytics")
print("=" * 70)
print(f"{'Coluna':<18} {'Completude':>12} {'Threshold':>10} {'Status':>8} {'Nulos':>8}")
print("-" * 70)

passed_count = 0
failed_count = 0

for r in all_results:
    status = "✅ PASS" if r["passed"] else "❌ FAIL"
    completeness_pct = r["completeness"] * 100
    threshold_pct = r["threshold"] * 100
    
    if r["passed"]:
        passed_count += 1
    else:
        failed_count += 1
    
    print(f"{r['column']:<18} {completeness_pct:>10.2f}% {threshold_pct:>9.1f}% {status:>8} {r['null_count']:>8,}")

print("-" * 70)
print(f"\n📊 Resumo: {passed_count} PASSED | {failed_count} FAILED | "
      f"Total: {len(all_results)} checks")

# Veredicto geral
if failed_count == 0:
    print("🎉 RESULTADO: Todos os checks de completude passaram!")
else:
    print(f"⚠️  RESULTADO: {failed_count} coluna(s) abaixo do threshold definido.")
```

**Resultado esperado:**

```
📋 RELATÓRIO DE COMPLETUDE — DataFlow Analytics
======================================================================
Coluna              Completude  Threshold   Status    Nulos
----------------------------------------------------------------------
order_id              100.00%     100.0%  ✅ PASS        0
customer_id            95.00%      95.0%  ✅ PASS   ~2,575
product_id            100.00%     100.0%  ✅ PASS        0
quantity              100.00%     100.0%  ✅ PASS        0
unit_price            100.00%     100.0%  ✅ PASS        0
total_amount          100.00%      99.0%  ✅ PASS        0
order_date            100.00%     100.0%  ✅ PASS        0
payment_method         95.00%      90.0%  ✅ PASS   ~2,575
shipping_city          95.00%      90.0%  ✅ PASS   ~2,575
shipping_state         95.00%      90.0%  ✅ PASS   ~2,575
----------------------------------------------------------------------

📊 Resumo: 10 PASSED | 0 FAILED | Total: 10 checks
🎉 RESULTADO: Todos os checks de completude passaram!
```

**Explicação técnica:**

- Note que `customer_id` tem ~5% de nulos e o threshold é 95% — passa no limite!
- Todas as colunas informativas (`payment_method`, `shipping_city`, `shipping_state`) têm threshold mais relaxado (90%) e passam com folga
- Em um cenário real, se o threshold de `customer_id` fosse 96%, o check falharia
- Este relatório seria enviado por email ou Slack como parte do pipeline Airflow (veremos no exercício 05)

> **💡 Dica de Marina:** "Defina thresholds com base no impacto no negócio, não no que os dados historicamente mostram. Se o negócio exige 100% de completude em `order_id` para faturamento, esse é o threshold — mesmo que hoje esteja em 99.9%."

---

## Passo 7: Visualizar Resultados como DataFrame Spark

**Descrição:** Para integrar com dashboards e pipelines, vamos converter os resultados em um DataFrame Spark. Isso permite salvar em Parquet, enviar para ferramentas de BI ou usar em agregações futuras.

**Código:**

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    IntegerType, DoubleType, BooleanType
)

# Definir schema do relatório
schema_report = StructType([
    StructField("column", StringType(), False),
    StructField("total_records", IntegerType(), False),
    StructField("non_null_count", IntegerType(), False),
    StructField("null_count", IntegerType(), False),
    StructField("completeness", DoubleType(), False),
    StructField("threshold", DoubleType(), False),
    StructField("passed", BooleanType(), False),
])

# Converter resultados para DataFrame Spark
rows = [
    (
        r["column"],
        r["total_records"],
        r["non_null_count"],
        r["null_count"],
        r["completeness"],
        r["threshold"],
        r["passed"]
    )
    for r in all_results
]

df_report = spark.createDataFrame(rows, schema=schema_report)

# Exibir como tabela Spark
print("📊 Relatório de Completude (DataFrame Spark):")
df_report.show(truncate=False)
```

**Resultado esperado:**

```
📊 Relatório de Completude (DataFrame Spark):
+--------------+-------------+--------------+----------+------------+---------+------+
|column        |total_records|non_null_count|null_count|completeness|threshold|passed|
+--------------+-------------+--------------+----------+------------+---------+------+
|order_id      |51500        |51500         |0         |1.0         |1.0      |true  |
|customer_id   |51500        |48925         |2575      |0.95        |0.95     |true  |
|product_id    |51500        |51500         |0         |1.0         |1.0      |true  |
|quantity      |51500        |51500         |0         |1.0         |1.0      |true  |
|unit_price    |51500        |51500         |0         |1.0         |1.0      |true  |
|total_amount  |51500        |51500         |0         |1.0         |0.99     |true  |
|order_date    |51500        |51500         |0         |1.0         |1.0      |true  |
|payment_method|51500        |48925         |2575      |0.95        |0.9      |true  |
|shipping_city |51500        |48925         |2575      |0.95        |0.9      |true  |
|shipping_state|51500        |48925         |2575      |0.95        |0.9      |true  |
+--------------+-------------+--------------+----------+------------+---------+------+
```

**Código (filtrar apenas falhas):**

```python
# Filtrar apenas colunas que falharam (se houver)
df_failures = df_report.filter(col("passed") == False)

if df_failures.count() > 0:
    print("🚨 Colunas que falharam no check de completude:")
    df_failures.show(truncate=False)
else:
    print("✅ Nenhuma coluna falhou — todos os thresholds foram atendidos.")
    print("   (Lembre-se: isso NÃO significa que os dados estão perfeitos!)")
    print("   Os próximos exercícios verificam duplicatas, integridade e valores inválidos.")
```

**Resultado esperado:**

```
✅ Nenhuma coluna falhou — todos os thresholds foram atendidos.
   (Lembre-se: isso NÃO significa que os dados estão perfeitos!)
   Os próximos exercícios verificam duplicatas, integridade e valores inválidos.
```

**Explicação técnica:**

- Converter para DataFrame Spark permite salvar o relatório como arquivo: `df_report.write.parquet("datalake/quality_reports/")`
- Em produção, esse relatório seria persistido com timestamp para histórico de qualidade
- O campo `passed` como `BooleanType` facilita filtros e contagens downstream
- Um pipeline Airflow poderia ler esse DataFrame e decidir se interrompe a execução ou não

> **💡 Dica de Carlos:** "Salvar o histórico de checks de qualidade é fundamental. Daqui a 6 meses, quando alguém perguntar 'quando a qualidade do customer_id começou a cair?', você vai ter a resposta."

---

## Passo 8: Simular um Cenário de Falha (Threshold Mais Rígido)

**Descrição:** No cenário anterior todos os checks passaram porque os thresholds estavam calibrados para os dados atuais. Vamos simular o que acontece quando a Ana exige thresholds mais rígidos — como aconteceria se um novo regulamento exigisse 100% de completude em `customer_id`.

**Código:**

```python
# Cenário: regulamento exige customer_id 100% preenchido
print("🧪 Simulação: threshold de customer_id alterado para 100%")
print("=" * 70)

# Testar com threshold mais rígido
resultado_rigido = check_completeness(
    df_vendas, 
    ["customer_id"], 
    threshold=1.00  # 100% obrigatório
)

r = resultado_rigido[0]
status = "✅ PASS" if r["passed"] else "❌ FAIL"

print(f"\n  Coluna:       {r['column']}")
print(f"  Completude:   {r['completeness']*100:.2f}%")
print(f"  Threshold:    {r['threshold']*100:.1f}%")
print(f"  Nulos:        {r['null_count']:,}")
print(f"  Status:       {status}")

if not r["passed"]:
    gap = r["threshold"] - r["completeness"]
    registros_faltantes = r["null_count"]
    print(f"\n  ⚠️  GAP: faltam {gap*100:.2f} pontos percentuais")
    print(f"  ⚠️  {registros_faltantes:,} registros precisam ser corrigidos ou enviados para quarentena")
```

**Resultado esperado:**

```
🧪 Simulação: threshold de customer_id alterado para 100%
======================================================================

  Coluna:       customer_id
  Completude:   95.00%
  Threshold:    100.0%
  Nulos:        2,575
  Status:       ❌ FAIL

  ⚠️  GAP: faltam 5.00 pontos percentuais
  ⚠️  2,575 registros precisam ser corrigidos ou enviados para quarentena
```

**Explicação técnica:**

- Com threshold de 100%, qualquer null causa falha — comum para chaves primárias
- O **gap** indica quanto falta para atingir o threshold
- Na prática, registros que falham no check podem ser:
  1. **Corrigidos** (se a informação existir em outra fonte)
  2. **Enviados para quarentena** (isolados para análise manual)
  3. **Rejeitados** (deletados se não forem recuperáveis)
- A escolha entre essas opções é uma decisão de negócio, não técnica

> **💡 Dica de Ana (PO):** "Para o ShopBrasil, o `customer_id` é obrigatório porque usamos para calcular comissões. Pedidos sem customer_id precisam ir para quarentena até resolvermos com o parceiro."

---

## Passo 9: Preview — Conceito de Quarentena

**Descrição:** Quando registros falham no check de completude, não podemos simplesmente deletá-los. A DataFlow implementa um padrão de **quarentena**: dados inválidos são separados em um "espaço" à parte para investigação, enquanto dados válidos seguem no pipeline normalmente.

**Código:**

```python
from pyspark.sql.functions import current_timestamp, lit

# Separar registros com customer_id nulo (quarentena)
df_validos = df_vendas.filter(col("customer_id").isNotNull())
df_quarentena = df_vendas.filter(col("customer_id").isNull())

# Adicionar metadados de quarentena
df_quarentena = df_quarentena \
    .withColumn("quarantine_reason", lit("customer_id IS NULL")) \
    .withColumn("quarantine_check", lit("completeness")) \
    .withColumn("quarantine_ts", current_timestamp())

print(f"📊 Separação de dados:")
print(f"   ✅ Registros válidos:      {df_validos.count():,}")
print(f"   🔒 Registros em quarentena: {df_quarentena.count():,}")
print(f"   📁 Total original:         {df_vendas.count():,}")

# Verificar que a soma bate
assert df_validos.count() + df_quarentena.count() == df_vendas.count(), \
    "ERRO: soma não bate com o total!"
print(f"\n   ✓ Validação: soma dos splits = total original")
```

**Resultado esperado:**

```
📊 Separação de dados:
   ✅ Registros válidos:      ~48,925
   🔒 Registros em quarentena: ~2,575
   📁 Total original:         51,500

   ✓ Validação: soma dos splits = total original
```

**Código (visualizar registros em quarentena):**

```python
# Olhar uma amostra dos registros em quarentena
print("\n🔒 Amostra de registros em quarentena:")
df_quarentena.select(
    "order_id", "customer_id", "total_amount", 
    "quarantine_reason", "quarantine_ts"
).show(5, truncate=False)
```

**Resultado esperado:**

```
🔒 Amostra de registros em quarentena:
+------------------------------------+-----------+------------+----------------------+--------------------+
|order_id                            |customer_id|total_amount|quarantine_reason     |quarantine_ts       |
+------------------------------------+-----------+------------+----------------------+--------------------+
|abc123..                            |null       |449.97      |customer_id IS NULL   |2024-01-15 10:30:...|
|def456..                            |null       |89.90       |customer_id IS NULL   |2024-01-15 10:30:...|
|ghi789..                            |null       |1299.00     |customer_id IS NULL   |2024-01-15 10:30:...|
|...                                 |null       |...         |customer_id IS NULL   |2024-01-15 10:30:...|
+------------------------------------+-----------+------------+----------------------+--------------------+
only showing top 5 rows
```

**Explicação técnica:**

- **Quarentena** = isolamento de dados suspeitos, não exclusão
- Adicionamos metadados (`reason`, `check`, `timestamp`) para auditoria
- O `assert` garante que nenhum registro se perde na separação — princípio de **conservação de dados**
- Em produção, os dados em quarentena seriam salvos em um path separado: `datalake/quarantine/completeness/date=2024-01-15/`
- No exercício 05, veremos como integrar essa quarentena em um pipeline Airflow com alertas automáticos

> **💡 Dica de Marina:** "Quarentena é reversível — se depois descobrirmos que os nulls eram um bug da extração e temos os dados corrigidos do parceiro, podemos reprocessar os registros da quarentena. Deletar é irreversível. Sempre prefira quarentena."

---

## Resumo do Exercício

Neste exercício você implementou o primeiro check de qualidade do Data Quality Program da DataFlow:

| Etapa | O que fizemos | Função/Técnica |
|-------|---------------|----------------|
| Carregar dados | Leitura do dataset com problemas | `spark.read.parquet()` |
| Inspeção manual | Contar nulos por coluna | `isNull()`, `when()`, `spark_sum()` |
| Função reutilizável | `check_completeness()` genérica | Aceita qualquer DataFrame + threshold |
| Thresholds de negócio | Definir limites por coluna | Acordado com PO (Ana) |
| Relatório pass/fail | Resultado formatado por coluna | DataFrame Spark com schema tipado |
| Quarentena (preview) | Separar dados válidos de inválidos | `filter()` + metadados de auditoria |

### A Função que Você Criou

```python
check_completeness(df, columns, threshold) -> List[Dict]
```

Essa função será reutilizada nos próximos exercícios como parte do `DataQualityFramework`.

### Conceitos-Chave

1. **Completude** = percentual de registros não-nulos em uma coluna
2. **NULL vs NaN**: NULL é ausência de valor; NaN é valor numérico especial. Ambos indicam dado faltante
3. **Threshold** = limite mínimo aceitável definido pelo negócio, não pela tecnologia
4. **Quarentena** = isolamento de dados suspeitos para investigação, sem perda de informação
5. **Conservação de dados** = registros válidos + quarentena = total original (nada se perde)

> **Carlos:** "Ótimo! O check de completude é o mais básico, mas pega muitos problemas reais. No próximo exercício, vamos atacar as **duplicatas** — a Ana mencionou que o faturamento está inflado, e duplicatas são a causa mais provável. Vamos usar `groupBy` + `count` para detectá-las e `dropDuplicates` para resolvê-las."

---

## Próximo Exercício

➡️ **Exercício 2 — Check de Unicidade** (`02_check_unicidade.md`): detecção de duplicatas com groupBy, window functions e estratégias de deduplicação.
