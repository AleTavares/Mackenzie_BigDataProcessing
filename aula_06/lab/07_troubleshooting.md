# Troubleshooting — Aula 6: Qualidade de Dados e Monitoramento

## Contexto

> **Marina Silva (Arquiteta de Dados):** "Depois de implementar o DataQualityFramework na DataFlow Analytics, achei que teríamos menos problemas. Na verdade, trocamos problemas de dados sujos por problemas de configuração e calibração dos checks. Falsos positivos bloqueando pipelines, thresholds que funcionam em dev mas quebram em prod, checks que demoram horas em datasets grandes... Vou compartilhar os erros mais comuns e como resolvemos cada um."

---

## 1. Falsos Positivos em Checks de Qualidade

**Sintoma:**
```python
# Check de completude falha, mas os dados estão corretos!
result = dq.check_completeness(df_vendas, ["customer_id"], threshold=1.00)
print(result.passed)  # False
print(result.metric_value)  # 0.95

# Ao investigar os nulls:
df_vendas.filter(col("customer_id").isNull()).select("order_type").distinct().show()
# +----------+
# |order_type|
# +----------+
# |guest     |  ← Compras de visitantes (sem cadastro) — é LEGÍTIMO!
# +----------+
```

```python
# Ou check de validade falha em valores negativos que são estornos:
result = dq.check_validity(df_vendas, {"amount_positive": "total_amount > 0"})
print(result.passed)  # False

df_vendas.filter(col("total_amount") < 0).select("status").distinct().show()
# +--------+
# |status  |
# +--------+
# |refund  |  ← Estornos TÊM valor negativo — é correto!
# +--------+
```

**Causa:** As regras de qualidade foram definidas de forma genérica demais, sem considerar exceções legítimas do negócio. Dados válidos são flagrados como inválidos porque as regras não refletem todas as situações reais (compras de visitantes, estornos, campos opcionais por tipo de pedido).

**Solução:**
```python
# ✅ Solução 1: Regras condicionais (excluir casos legítimos)
# Em vez de "customer_id nunca pode ser null",
# use "customer_id não pode ser null EXCETO para pedidos guest"
result = dq.check_validity(df_vendas, {
    "customer_required": "customer_id IS NOT NULL OR order_type = 'guest'"
})

# ✅ Solução 2: Filtrar o DataFrame antes do check
df_vendas_cadastrados = df_vendas.filter(col("order_type") != "guest")
result = dq.check_completeness(df_vendas_cadastrados, ["customer_id"], threshold=1.00)

# ✅ Solução 3: Regra de validade que aceita estornos
result = dq.check_validity(df_vendas, {
    "amount_valid": "total_amount > 0 OR status = 'refund'"
})
```

```python
# ✅ Solução 4: Segmentar checks por tipo de dado
configs_por_tipo = {
    "vendas_normais": {
        "filter": "status NOT IN ('refund', 'cancelled')",
        "validity": {"amount_positive": "total_amount > 0"}
    },
    "estornos": {
        "filter": "status = 'refund'",
        "validity": {"amount_negative": "total_amount < 0"}
    }
}

for nome, cfg in configs_por_tipo.items():
    df_segmento = df_vendas.filter(expr(cfg["filter"]))
    result = dq.check_validity(df_segmento, cfg["validity"])
    print(f"{nome}: {'✅' if result.passed else '❌'} ({result.metric_value:.1%})")
```

**Prevenção:**
- Antes de definir regras, analise os dados com `distinct()` e `groupBy()` para entender todas as variações legítimas
- Documente exceções de negócio junto com cada regra de qualidade
- Use regras condicionais (`OR`, `CASE WHEN`) em vez de regras absolutas
- Revise regras com o Product Owner sempre que um novo tipo de dado surgir (ex: novo status, novo parceiro)

---

## 2. Threshold Muito Rígido Bloqueia Pipeline

**Sintoma:**
```python
# Pipeline Airflow falha toda noite no quality gate:
result = dq.check_completeness(
    df_vendas, 
    ["payment_method", "shipping_city", "promo_code"],
    threshold=1.00  # 100% para TODOS os campos
)
# ❌ FAIL — payment_method: 97%, shipping_city: 95%, promo_code: 30%

report = dq.generate_report()
if not report["gate_passed"]:
    raise Exception("Quality gate falhou!")  # Pipeline para TODA NOITE!
```

```
# Log do Airflow:
[2024-01-15 03:00:15] ERROR - Quality gate falhou! Score: 74%
[2024-01-15 03:00:15] ERROR - Task quality_check failed
# Nenhum dado chega na camada Gold há 5 dias...
```

**Causa:** Threshold de 100% aplicado a campos que são legitimamente opcionais (`promo_code` só existe quando há promoção, `shipping_city` pode faltar em pedidos de download digital). O quality gate trata todos os checks com a mesma severidade, bloqueando o pipeline por campos não-críticos.

**Solução:**
```python
# ✅ Solução 1: Classificar campos por criticidade
config = {
    "completeness": {
        "columns": ["order_id", "customer_id", "product_id"],
        "threshold": 1.00  # Campos obrigatórios: 100%
    }
}

# Campos opcionais com threshold relaxado:
config_opcional = {
    "completeness": {
        "columns": ["payment_method", "shipping_city"],
        "threshold": 0.90  # Aceita até 10% de nulls
    }
}

# Campos condicionais: não verificar se não se aplica
# promo_code só é obrigatório quando has_promo = True
df_com_promo = df_vendas.filter(col("has_promo") == True)
dq.check_completeness(df_com_promo, ["promo_code"], threshold=0.95)
```

```python
# ✅ Solução 2: Usar severity para controlar o gate
# Checks "critical" bloqueiam o pipeline
# Checks "warning" apenas geram alerta
result_critical = CheckResult(
    check_name="completeness_order_id",
    passed=True,
    metric_value=1.0,
    threshold=1.0,
    details={},
    severity="critical"  # ← Bloqueia se falhar
)

result_warning = CheckResult(
    check_name="completeness_promo_code",
    passed=False,
    metric_value=0.30,
    threshold=0.90,
    details={},
    severity="warning"  # ← Apenas alerta, não bloqueia
)

# No generate_report():
# gate_passed = todos os CRITICAL passaram (ignora warnings)
```

```python
# ✅ Solução 3: Threshold adaptativo baseado no histórico
import statistics

# Ler histórico de métricas (últimos 30 dias)
historico = [0.95, 0.96, 0.94, 0.95, 0.97, 0.95, 0.93, 0.96]  # completude diária
media = statistics.mean(historico)
desvio = statistics.stdev(historico)

# Threshold = média - 2 desvios padrão (aceita variação natural)
threshold_adaptativo = media - (2 * desvio)
print(f"Threshold adaptativo: {threshold_adaptativo:.2%}")
# Threshold adaptativo: ~91.5% (em vez de 100% fixo)

result = dq.check_completeness(df_vendas, ["customer_id"], threshold=threshold_adaptativo)
```

**Prevenção:**
- Classifique colunas em **obrigatórias** (threshold alto, severity critical) e **opcionais** (threshold baixo, severity warning)
- Use `severity` no `CheckResult` para que o gate só bloqueie em falhas críticas
- Comece com thresholds conservadores (80-90%) e aumente gradualmente conforme os dados estabilizam
- Monitore o histórico de métricas para definir thresholds realistas baseados em dados reais

---

## 3. Performance de Checks em Datasets Grandes

**Sintoma:**
```python
# Check de unicidade em 2 bilhões de registros:
import time
start = time.time()
result = dq.check_uniqueness(df_vendas_historico, ["order_id"])
elapsed = time.time() - start
print(f"Tempo: {elapsed:.0f}s")
# Tempo: 2847s  ← Quase 50 MINUTOS para um único check!

# O pipeline Airflow tem timeout de 30 minutos e falha:
# [ERROR] Task quality_check exceeded timeout of 1800 seconds
```

```python
# Ou check de integridade referencial que faz full join:
result = dq.check_referential_integrity(
    df_vendas_2bi,      # 2 bilhões de linhas
    df_clientes_50m,    # 50 milhões de clientes
    "customer_id", "customer_id"
)
# OutOfMemoryError: Java heap space
```

**Causa:** Os checks fazem scan completo do dataset (full table scan) a cada execução. Para datasets com bilhões de registros, operações como `groupBy` (unicidade), `join` (integridade referencial) e `count` (completude) são extremamente custosas. O Spark precisa shuffle todos os dados entre executores.

**Solução:**
```python
# ✅ Solução 1: Amostragem estatística para checks não-críticos
# Em vez de verificar 2 bilhões, amostrar 1% (20 milhões)
df_amostra = df_vendas_historico.sample(fraction=0.01, seed=42)
print(f"Amostra: {df_amostra.count():,} registros")

# Check na amostra (resultado estatisticamente representativo)
result = dq.check_completeness(df_amostra, ["customer_id"], threshold=0.95)
# Tempo: ~15 segundos em vez de 50 minutos!

# Para unicidade, amostragem pode subestimar duplicatas.
# Use approx_count_distinct em vez de count(distinct):
from pyspark.sql.functions import approx_count_distinct, count

total = df_vendas_historico.count()
distintos_aprox = df_vendas_historico.select(
    approx_count_distinct("order_id", rsd=0.01)  # 1% de erro relativo
).collect()[0][0]

unicidade_aprox = distintos_aprox / total
print(f"Unicidade (aproximada): {unicidade_aprox:.4%}")
```

```python
# ✅ Solução 2: Verificar apenas dados novos (incremental)
from pyspark.sql.functions import col, current_date, date_sub

# Em vez de verificar todo o histórico, verificar apenas últimas 24h
df_novos = df_vendas_historico.filter(
    col("ingestion_date") >= date_sub(current_date(), 1)
)
print(f"Registros novos (24h): {df_novos.count():,}")

# Checks apenas nos dados novos
result = dq.check_completeness(df_novos, ["order_id", "customer_id"], threshold=0.95)
# Tempo: ~30 segundos (verificando ~5M registros em vez de 2B)
```

```python
# ✅ Solução 3: Integridade referencial com broadcast join
from pyspark.sql.functions import broadcast

# Se a tabela de referência é pequena (< 100MB), usar broadcast
# Evita shuffle da tabela grande
df_orfaos = df_vendas_2bi.join(
    broadcast(df_clientes_50m.select("customer_id")),
    on="customer_id",
    how="left_anti"  # Registros SEM correspondência
)

orfaos_count = df_orfaos.count()
integridade = 1 - (orfaos_count / df_vendas_2bi.count())
print(f"Integridade referencial: {integridade:.2%}")
```

```python
# ✅ Solução 4: Cache do DataFrame se múltiplos checks na sequência
df_vendas_novos = df_vendas_historico.filter(
    col("ingestion_date") >= date_sub(current_date(), 1)
).cache()

# Primeiro count materializa o cache
print(f"Registros: {df_vendas_novos.count():,}")

# Checks subsequentes usam dados em memória (muito mais rápido)
r1 = dq.check_completeness(df_vendas_novos, ["order_id", "customer_id"])
r2 = dq.check_uniqueness(df_vendas_novos, ["order_id"])
r3 = dq.check_validity(df_vendas_novos, {"qty_pos": "quantity > 0"})

# Liberar cache após todos os checks
df_vendas_novos.unpersist()
```

**Prevenção:**
- Use verificação **incremental** (apenas dados novos) como padrão; full scan apenas em auditoria periódica
- Para datasets > 100M registros, use `approx_count_distinct` em vez de `countDistinct`
- Use `broadcast` join quando a tabela de referência cabe em memória (< 100MB)
- Cache o DataFrame se for verificado por múltiplos checks na mesma execução
- Configure timeout adequado no Airflow: checks em dados grandes precisam de mais tempo

---

## 4. Quarentena Acumula sem Investigação

**Sintoma:**
```python
# Verificar tamanho da quarentena:
df_quarentena = spark.read.parquet("datalake/quarantine/")
print(f"Registros em quarentena: {df_quarentena.count():,}")
# Registros em quarentena: 847,293  ← Quase 1 milhão sem ação!

# Quarentena crescendo ~5.000 registros/dia há 6 meses:
df_quarentena.groupBy("quarantine_reason").count().orderBy(col("count").desc()).show()
# +----------------------------------+------+
# |quarantine_reason                 |count |
# +----------------------------------+------+
# |customer_id IS NULL               |523000|  ← 62% são o mesmo problema!
# |total_amount <= 0                 |198000|
# |product_id not in reference       |126293|
# +----------------------------------+------+
```

**Causa:** O pipeline envia dados para quarentena corretamente, mas ninguém revisa, investiga ou resolve os problemas. A quarentena se torna um "cemitério de dados" — os registros entram mas nunca saem. Sem alertas, SLAs ou donos definidos, o acúmulo passa despercebido.

**Solução:**
```python
# ✅ Solução 1: Alertas automáticos quando quarentena excede limite
from pyspark.sql.functions import col, count, current_date, datediff

# Verificar volume da quarentena
df_quarentena = spark.read.parquet("datalake/quarantine/")
total_quarentena = df_quarentena.count()

# Alerta se quarentena > 10.000 registros não processados
LIMITE_QUARENTENA = 10000
if total_quarentena > LIMITE_QUARENTENA:
    print(f"🚨 ALERTA: Quarentena com {total_quarentena:,} registros (limite: {LIMITE_QUARENTENA:,})")
    # Em produção: enviar Slack/email para o time

# Verificar idade dos registros mais antigos
df_quarentena.select(
    count("*").alias("total"),
    spark_min("quarantine_ts").alias("mais_antigo"),
    datediff(current_date(), spark_min("quarantine_ts")).alias("dias_sem_acao")
).show()
```

```python
# ✅ Solução 2: Relatório de quarentena por categoria (para priorização)
def relatorio_quarentena(spark, quarantine_path):
    """Gera relatório priorizado da quarentena."""
    df = spark.read.parquet(quarantine_path)
    
    resumo = df.groupBy("quarantine_reason").agg(
        count("*").alias("total_registros"),
        spark_min("quarantine_ts").alias("primeiro_registro"),
        spark_max("quarantine_ts").alias("ultimo_registro")
    ).orderBy(col("total_registros").desc())
    
    print("📋 RELATÓRIO DE QUARENTENA — Priorização")
    print("=" * 70)
    resumo.show(truncate=False)
    
    total = df.count()
    print(f"\n📊 Total em quarentena: {total:,}")
    print(f"   Categorias distintas: {resumo.count()}")
    
    return resumo

relatorio_quarentena(spark, "datalake/quarantine/")
```

```python
# ✅ Solução 3: Política de retenção (quarentena não é eterna)
from pyspark.sql.functions import col, current_timestamp, datediff

# Registros em quarentena há mais de 30 dias sem resolução → arquivar
df_quarentena = spark.read.parquet("datalake/quarantine/")

df_recente = df_quarentena.filter(
    datediff(current_date(), col("quarantine_ts")) <= 30
)
df_expirado = df_quarentena.filter(
    datediff(current_date(), col("quarantine_ts")) > 30
)

# Mover expirados para arquivo morto (cold storage)
df_expirado.write.mode("append").parquet("datalake/archive/quarantine_expired/")

# Manter apenas registros recentes na quarentena ativa
df_recente.write.mode("overwrite").parquet("datalake/quarantine/")

print(f"✅ Quarentena limpa: {df_recente.count():,} ativos, {df_expirado.count():,} arquivados")
```

**Prevenção:**
- Defina um **dono** (pessoa ou equipe) responsável por revisar a quarentena semanalmente
- Configure alertas automáticos quando a quarentena exceder um limite (ex: 10K registros)
- Implemente política de retenção: registros > 30 dias sem ação → arquivar ou descartar
- Priorize por volume: resolva primeiro a categoria com mais registros (maior impacto)
- Inclua métricas de quarentena no dashboard de observabilidade do pipeline

---

## 5. Quality Gate Inconsistente entre Ambientes

**Sintoma:**
```python
# EM DEV (dataset pequeno ~1.000 registros): tudo passa ✅
dq.check_completeness(df_dev, ["customer_id"], threshold=0.95)
# metric_value: 0.98 — PASSED!

dq.check_uniqueness(df_dev, ["order_id"])
# metric_value: 1.00 — PASSED!

# EM PROD (dataset real ~50M registros): falha ❌
dq.check_completeness(df_prod, ["customer_id"], threshold=0.95)
# metric_value: 0.91 — FAILED!

dq.check_uniqueness(df_prod, ["order_id"])
# metric_value: 0.97 — FAILED! (threshold era 1.00)
```

```
# O desenvolvedor diz: "Funciona na minha máquina!"
# Pipeline em produção falha toda noite.
```

**Causa:** Diferenças fundamentais entre os ambientes:
1. **Volume**: Dev usa amostra limpa de 1K registros; Prod tem 50M com variações reais
2. **Diversidade de dados**: Dev não tem parceiros com problemas de encoding ou campos opcionais
3. **Temporalidade**: Dev usa snapshot fixo; Prod recebe dados novos diariamente de fontes heterogêneas
4. **Duplicatas**: Em Dev não existem porque o dataset foi curado manualmente

**Solução:**
```python
# ✅ Solução 1: Usar amostra REAL de produção em dev
# Exportar amostra representativa (preservando distribuição de problemas)
df_amostra_prod = df_prod.sample(fraction=0.001, seed=42)  # 0.1% = ~50K registros
df_amostra_prod.write.mode("overwrite").parquet("data/dev/amostra_producao.parquet")

# Dev agora testa com dados reais (incluindo nulls, duplicatas, etc.)
df_dev = spark.read.parquet("data/dev/amostra_producao.parquet")
```

```python
# ✅ Solução 2: Configuração por ambiente com thresholds diferentes
import os

ENV = os.getenv("ENVIRONMENT", "dev")

QUALITY_CONFIG = {
    "dev": {
        "completeness_threshold": 0.99,  # Rígido em dev para pegar problemas cedo
        "uniqueness_threshold": 1.00,
        "gate_mode": "warn"              # Não bloqueia, apenas avisa
    },
    "prod": {
        "completeness_threshold": 0.95,  # Realista para produção
        "uniqueness_threshold": 0.99,
        "gate_mode": "block"             # Bloqueia se falhar
    }
}

config = QUALITY_CONFIG[ENV]
result = dq.check_completeness(df, ["customer_id"], threshold=config["completeness_threshold"])

if not result.passed and config["gate_mode"] == "block":
    raise Exception(f"Quality gate falhou em {ENV}!")
elif not result.passed and config["gate_mode"] == "warn":
    print(f"⚠️ Warning: completude abaixo do threshold em {ENV}")
```

```python
# ✅ Solução 3: Testes de qualidade no CI/CD com dados sintéticos problemáticos
def criar_dataset_teste_com_problemas(spark, n=10000):
    """Cria dataset de teste que simula problemas reais de produção."""
    import random
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    dados = []
    for i in range(n):
        customer_id = f"CUST-{i}" if random.random() > 0.05 else None  # 5% nulls
        order_id = f"ORD-{i}" if random.random() > 0.02 else f"ORD-{i-1}"  # 2% duplicatas
        amount = round(random.uniform(10, 500), 2)
        if random.random() < 0.01:
            amount = -amount  # 1% estornos
        dados.append((order_id, customer_id, amount))
    
    return spark.createDataFrame(dados, ["order_id", "customer_id", "total_amount"])

# Usar em testes automatizados para garantir que o pipeline lida com problemas
df_teste = criar_dataset_teste_com_problemas(spark)
result = dq.check_completeness(df_teste, ["customer_id"], threshold=0.95)
assert result.passed, f"Check falhou com {result.metric_value:.2%} — ajustar threshold ou regra"
```

**Prevenção:**
- Use amostras REAIS de produção como dados de teste em dev (anonimizadas se necessário)
- Mantenha configuração de thresholds por ambiente (`dev`, `staging`, `prod`)
- Execute checks em staging com dados de volume similar a prod antes do deploy
- Documente as diferenças conhecidas entre ambientes e seus impactos nos checks

---

## 6. Duplicatas Legítimas Confundidas com Erros

**Sintoma:**
```python
# Check de unicidade detecta "duplicatas" em order_items:
result = dq.check_uniqueness(df_items, ["customer_id", "product_id", "order_date"])
print(result.passed)  # False
print(result.metric_value)  # 0.94 — 6% de "duplicatas"

# Investigar as "duplicatas":
df_items.groupBy("customer_id", "product_id", "order_date") \
    .count() \
    .filter(col("count") > 1) \
    .show(5)
# +------------+----------+----------+-----+
# |customer_id |product_id|order_date|count|
# +------------+----------+----------+-----+
# |CUST-1234   |PROD-567  |2024-01-15|    2|  ← Cliente comprou 2x no mesmo dia!
# |CUST-5678   |PROD-123  |2024-01-15|    3|  ← Presente para 3 pessoas!
# +------------+----------+----------+-----+
# São compras legítimas, NÃO duplicatas de ingestão!
```

**Causa:** A chave de unicidade escolhida não representa a chave natural de negócio. O mesmo cliente pode comprar o mesmo produto várias vezes — isso é uma transação legítima, não uma duplicata. Duplicatas reais são causadas por re-ingestão ou retry de sistema, não por comportamento do usuário.

**Solução:**
```python
# ✅ Solução 1: Usar a chave correta (identificador único da transação)
# ❌ ERRADO — combinação que permite repetições legítimas:
result = dq.check_uniqueness(df_items, ["customer_id", "product_id", "order_date"])

# ✅ CORRETO — usar o identificador único da transação:
result = dq.check_uniqueness(df_items, ["order_id"])
# order_id é gerado pelo sistema e nunca se repete para transações diferentes

# Se não tem ID único, usar combinação com timestamp preciso:
result = dq.check_uniqueness(df_items, ["order_id", "line_item_id"])
```

```python
# ✅ Solução 2: Distinguir duplicatas de ingestão vs repetições legítimas
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Duplicatas de INGESTÃO: registros com TODOS os campos idênticos
# (incluindo campos técnicos como ingestion_timestamp)
window_full = Window.partitionBy(df_items.columns).orderBy("ingestion_ts")
df_com_rank = df_items.withColumn("rank", row_number().over(window_full))

duplicatas_ingestao = df_com_rank.filter(col("rank") > 1).count()
total = df_items.count()
print(f"Duplicatas de ingestão: {duplicatas_ingestao} ({duplicatas_ingestao/total:.2%})")

# Repetições LEGÍTIMAS: mesmo cliente+produto mas order_id diferente
# → NÃO são duplicatas, são transações válidas
```

```python
# ✅ Solução 3: Check de unicidade com chave composta adequada
# Para tabela de PEDIDOS: order_id é a chave
result_pedidos = dq.check_uniqueness(df_pedidos, ["order_id"])

# Para tabela de ITENS: order_id + line_number é a chave
result_itens = dq.check_uniqueness(df_itens, ["order_id", "line_number"])

# Para tabela de EVENTOS: event_id ou (session_id + timestamp + event_type)
result_eventos = dq.check_uniqueness(df_eventos, ["event_id"])
```

**Prevenção:**
- Antes de configurar check de unicidade, entenda a **chave natural de negócio** da tabela
- Pergunte: "É possível que a mesma combinação ocorra legitimamente?" Se sim, a chave está errada
- Use identificadores de sistema (UUID, sequence) como chave de unicidade quando disponíveis
- Documente a chave de unicidade de cada tabela no catálogo de dados

---

## 7. `left_anti` Join com Null Keys

**Sintoma:**
```python
# Check de integridade referencial retorna resultado estranho:
df_orfaos = df_vendas.join(
    df_clientes,
    on=df_vendas["customer_id"] == df_clientes["customer_id"],
    how="left_anti"
)
print(f"Registros órfãos: {df_orfaos.count()}")
# Registros órfãos: 5,150
# Mas só deveria ter ~500 órfãos reais!

# Os 4,650 extras são registros com customer_id = NULL:
df_orfaos.filter(col("customer_id").isNull()).count()
# 4,650  ← NULLs são considerados "não encontrados" no join!
```

**Causa:** Em SQL e PySpark, `NULL = NULL` retorna `NULL` (não `True`). Portanto, registros com chave NULL nunca encontram correspondência em um join — nem `inner`, nem `left_anti`. No `left_anti`, isso significa que TODOS os registros com chave NULL aparecem como "órfãos", inflando o resultado de integridade referencial.

**Solução:**
```python
# ✅ Solução 1: Filtrar nulls ANTES do join de integridade
# Registros com customer_id null são problema de COMPLETUDE, não de integridade
df_vendas_com_id = df_vendas.filter(col("customer_id").isNotNull())

df_orfaos = df_vendas_com_id.join(
    df_clientes,
    on=df_vendas_com_id["customer_id"] == df_clientes["customer_id"],
    how="left_anti"
)
print(f"Registros órfãos (sem nulls): {df_orfaos.count()}")
# Registros órfãos (sem nulls): 500  ← Resultado correto!
```

```python
# ✅ Solução 2: Implementar no check_referential_integrity do framework
def check_referential_integrity(
    self, df_source: DataFrame, df_reference: DataFrame,
    source_col: str, ref_col: str
) -> CheckResult:
    """Verifica integridade referencial, ignorando NULLs na source."""
    
    # Filtrar nulls da coluna de join (nulls são problema de completude)
    df_source_filtered = df_source.filter(col(source_col).isNotNull())
    total_com_chave = df_source_filtered.count()
    
    if total_com_chave == 0:
        return CheckResult(
            check_name=f"referential_{source_col}",
            passed=True,
            metric_value=1.0,
            threshold=0.95,
            details={"warning": "Todos os registros têm chave NULL"},
            severity="warning"
        )
    
    # left_anti: registros em source que NÃO existem em reference
    df_orfaos = df_source_filtered.join(
        df_reference.select(col(ref_col)).distinct(),
        on=df_source_filtered[source_col] == df_reference[ref_col],
        how="left_anti"
    )
    
    orfaos_count = df_orfaos.count()
    integridade = 1 - (orfaos_count / total_com_chave)
    
    return CheckResult(
        check_name=f"referential_{source_col}",
        passed=integridade >= 0.95,
        metric_value=round(integridade, 4),
        threshold=0.95,
        details={
            "total_com_chave": total_com_chave,
            "orfaos": orfaos_count,
            "nulls_ignorados": df_source.count() - total_com_chave
        },
        severity="critical"
    )
```

```python
# ✅ Solução 3: Reportar nulls separadamente no relatório
total = df_vendas.count()
nulls = df_vendas.filter(col("customer_id").isNull()).count()
com_chave = total - nulls

df_orfaos = df_vendas.filter(col("customer_id").isNotNull()).join(
    df_clientes, on="customer_id", how="left_anti"
)

print(f"📊 Análise de customer_id:")
print(f"   Total registros:    {total:,}")
print(f"   Com chave (não-null): {com_chave:,}")
print(f"   Sem chave (null):     {nulls:,} ← verificar com check_completeness")
print(f"   Órfãos reais:         {df_orfaos.count():,} ← verificar com check_referential_integrity")
```

**Prevenção:**
- **Sempre** filtre `isNotNull()` na coluna de join antes de fazer `left_anti` para integridade referencial
- Separe os checks: completude verifica nulls, integridade referencial verifica apenas registros com chave preenchida
- Documente no framework que `check_referential_integrity` ignora NULLs por design
- Lembre: em SQL, `NULL = NULL` é `NULL` (não `TRUE`) — isso afeta TODOS os tipos de join

---

## 8. `check_validity` com Expressões SQL Inválidas

**Sintoma:**
```python
# check_validity falha com erro críptico do PySpark:
result = dq.check_validity(df_vendas, {
    "date_valid": "order_date <= GETDATE()",  # ← Função SQL Server, não Spark!
})

# Erro:
# AnalysisException: [UNRESOLVED_ROUTINE] Cannot resolve function `GETDATE`
```

```python
# Ou expressão com sintaxe Python em vez de SQL:
result = dq.check_validity(df_vendas, {
    "amount_check": "total_amount != None",  # ← Python, não SQL!
})

# Erro:
# AnalysisException: Column 'None' does not exist
```

```python
# Ou referência a coluna que não existe:
result = dq.check_validity(df_vendas, {
    "status_valid": "order_status IN ('active', 'completed')",  # Coluna é "status", não "order_status"!
})

# Erro:
# AnalysisException: [UNRESOLVED_COLUMN] Column 'order_status' does not exist.
# Did you mean one of: order_id, customer_id, status, ...?
```

**Causa:** O método `check_validity` usa `pyspark.sql.functions.expr()` internamente para avaliar as regras. O `expr()` aceita **Spark SQL** (não Python nem SQL Server). Erros comuns:
1. Funções de outros bancos (`GETDATE()`, `NVL()`, `ISNULL()`)
2. Sintaxe Python (`None`, `True`, `and`, `or`)
3. Nomes de colunas incorretos (typo ou nome do banco original)
4. Operadores errados (`!=` funciona, mas `<>` é mais idiomático em SQL)

**Solução:**
```python
# ✅ Solução 1: Usar funções Spark SQL corretas
regras_corretas = {
    # ❌ GETDATE() → ✅ current_date() / current_timestamp()
    "date_valid": "order_date <= current_date()",
    
    # ❌ None → ✅ IS NOT NULL
    "amount_not_null": "total_amount IS NOT NULL",
    
    # ❌ ISNULL(x, 0) → ✅ COALESCE(x, 0) ou NVL(x, 0) [NVL funciona no Spark!]
    "amount_filled": "COALESCE(total_amount, 0) > 0",
    
    # ❌ LEN(x) → ✅ LENGTH(x)
    "id_format": "LENGTH(order_id) = 36",
    
    # ❌ DATEADD → ✅ date_add / date_sub
    "date_recent": "order_date >= date_sub(current_date(), 365)",
}

result = dq.check_validity(df_vendas, regras_corretas)
```

```python
# ✅ Solução 2: Validar expressões ANTES de executar o check
from pyspark.sql.functions import expr

def validar_regras(df, regras):
    """Valida se todas as expressões SQL são válidas antes de executar o check."""
    erros = []
    for nome, expressao in regras.items():
        try:
            # Tenta parsear a expressão (sem executar)
            df.select(expr(expressao))
        except Exception as e:
            erros.append(f"  ❌ '{nome}': {expressao}\n     Erro: {str(e)[:100]}")
    
    if erros:
        print("🚨 Regras com erro de sintaxe SQL:")
        for erro in erros:
            print(erro)
        return False
    
    print(f"✅ Todas as {len(regras)} regras são válidas sintaticamente")
    return True

# Validar antes de usar no check:
regras = {
    "qty_positive": "quantity > 0",
    "price_valid": "unit_price > 0",
    "date_ok": "order_date <= current_date()"
}

if validar_regras(df_vendas, regras):
    result = dq.check_validity(df_vendas, regras)
```

```python
# ✅ Solução 3: Tabela de conversão SQL Server/MySQL → Spark SQL
CONVERSAO_SQL = """
| SQL Server / MySQL       | Spark SQL equivalente          |
|--------------------------|--------------------------------|
| GETDATE()                | current_timestamp()            |
| ISNULL(x, default)       | COALESCE(x, default)           |
| LEN(x)                   | LENGTH(x)                      |
| DATEADD(day, 7, col)     | date_add(col, 7)               |
| DATEDIFF(day, a, b)      | datediff(b, a)                 |
| CONVERT(INT, x)          | CAST(x AS INT)                 |
| TOP 10                   | LIMIT 10                       |
| x = NULL                 | x IS NULL                      |
| IIF(cond, a, b)          | IF(cond, a, b) ou CASE WHEN    |
"""
print(CONVERSAO_SQL)
```

```python
# ✅ Solução 4: Verificar nomes de colunas disponíveis
# Antes de definir regras, verificar quais colunas existem:
print(f"Colunas disponíveis: {df_vendas.columns}")

# Regra com verificação de coluna:
def check_validity_safe(dq, df, regras):
    """Executa check_validity com validação prévia de colunas e sintaxe."""
    regras_validas = {}
    
    for nome, expressao in regras.items():
        try:
            df.select(expr(expressao))
            regras_validas[nome] = expressao
        except Exception as e:
            print(f"⚠️ Regra '{nome}' ignorada: {e}")
    
    if regras_validas:
        return dq.check_validity(df, regras_validas)
    else:
        print("🚨 Nenhuma regra válida para executar!")
        return None
```

**Prevenção:**
- Use sempre **Spark SQL** nas expressões do `check_validity` (não Python, não SQL Server)
- Valide as expressões com `df.select(expr(regra))` antes de usar em checks de produção
- Mantenha uma tabela de conversão de funções do seu banco de origem para Spark SQL
- Verifique os nomes exatos das colunas com `df.columns` antes de referenciar nas regras
- Em caso de dúvida, consulte: https://spark.apache.org/docs/latest/api/sql/

---

## Quick Reference: Tabela de Diagnóstico Rápido

| Sintoma | Causa Provável | Solução Rápida |
|---------|---------------|----------------|
| Check falha mas dados são válidos | Regra não considera exceções legítimas | Adicionar condição `OR` para casos válidos |
| Pipeline bloqueado toda noite | Threshold 100% em campos opcionais | Usar severity "warning" para não-críticos |
| Check demora 50+ minutos | Full scan em bilhões de registros | Verificar apenas dados novos (incremental) |
| Quarentena com 800K+ registros | Ninguém revisa/resolve | Alertas + política de retenção (30 dias) |
| Passa em dev, falha em prod | Dados de teste não refletem produção | Usar amostra real de prod em dev |
| Unicidade falha com compras repetidas | Chave errada (não é a chave de negócio) | Usar `order_id` em vez de combinação de campos |
| Integridade inflada com nulls | `NULL = NULL` retorna NULL no join | Filtrar `isNotNull()` antes do `left_anti` |
| `UNRESOLVED_ROUTINE` no check_validity | Função SQL Server em vez de Spark SQL | Usar equivalente Spark: `current_date()` etc. |
| `Column does not exist` na regra | Typo no nome da coluna na expressão | Verificar `df.columns` antes de definir regras |
| Score geral baixo sem motivo claro | Muitos checks warning falhando | Separar critical (gate) de warning (alerta) |

---

## Fluxo de Diagnóstico: Árvore de Decisão

```
Check falha mas dados parecem corretos?
├── Dados legítimos flagrados como inválidos → Problema 1 (Falsos Positivos)
├── Campo opcional tratado como obrigatório → Problema 2 (Threshold rígido)
├── Compras repetidas contadas como duplicatas → Problema 6 (Chave errada)
└── Nulls inflando contagem de órfãos → Problema 7 (left_anti + nulls)

Pipeline bloqueado ou lento?
├── Quality gate bloqueia toda noite → Problema 2 (Threshold/Severity)
├── Check demora dezenas de minutos → Problema 3 (Performance)
├── Funciona em dev, falha em prod → Problema 5 (Ambientes diferentes)
└── Erro de sintaxe no check_validity → Problema 8 (SQL inválido)

Problemas operacionais?
├── Quarentena cresce sem controle → Problema 4 (Sem investigação)
├── Ninguém sabe o que está em quarentena → Problema 4 (Falta relatório)
└── Thresholds definidos "no olho" → Problema 5 (Sem baseline de prod)

Erros técnicos no código?
├── UNRESOLVED_ROUTINE / function not found → Problema 8 (Função SQL errada)
├── Column does not exist → Problema 8 (Nome de coluna errado)
├── Resultado inesperado no join → Problema 7 (NULLs na chave)
└── OutOfMemoryError em checks → Problema 3 (Falta sampling/broadcast)
```

---

## Comandos Úteis para Debug

```python
# === Investigar falsos positivos ===
# Ver distribuição de valores na coluna que falhou:
df.groupBy("coluna_suspeita").count().orderBy(col("count").desc()).show(20)

# Ver registros que falharam em uma regra específica:
from pyspark.sql.functions import expr
df.filter(~expr("sua_regra_aqui")).show(10, truncate=False)

# === Performance de checks ===
# Contar registros sem collect (lazy evaluation):
df.count()  # Força execução

# Ver plano de execução do check:
df.filter(expr("quantity > 0")).explain(mode="formatted")

# Verificar tamanho do DataFrame em memória:
df.cache()
print(f"Tamanho em cache: {df.storageLevel}")

# === Quarentena ===
# Resumo rápido da quarentena:
spark.read.parquet("datalake/quarantine/") \
    .groupBy("quarantine_reason") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(truncate=False)

# === Validação de expressões SQL ===
# Testar uma expressão antes de usar no framework:
from pyspark.sql.functions import expr
try:
    df.select(expr("sua_expressao")).show(1)
    print("✅ Expressão válida")
except Exception as e:
    print(f"❌ Erro: {e}")

# === Null analysis ===
# Contar nulls em todas as colunas de uma vez:
from pyspark.sql.functions import sum as spark_sum, when, col
df.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]).show()
```

---

> **Marina:** "A regra de ouro da qualidade de dados: seus checks são tão bons quanto seu entendimento do negócio. Regras genéricas demais geram falsos positivos; regras específicas demais deixam bugs passar. O segredo é calibrar com dados reais, revisar com o time de negócio regularmente, e tratar a quarentena como um backlog — com dono, prioridade e SLA. Na DataFlow, todo check que bloqueia pipeline precisa ter sido validado em produção por pelo menos 2 semanas em modo 'warning' antes de virar 'critical'."

