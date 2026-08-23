# Exercício 4 — Sistema de Quarentena Unificado

## Contexto

> **Marina Silva (CTO):** "Vocês já implementaram checks individuais de completude, unicidade e integridade referencial. Muito bom. Mas no mundo real, esses checks não rodam isolados — precisamos de um **sistema unificado de quarentena**. Todo registro que falhar em QUALQUER validação vai para quarentena com metadados explicando o motivo. Registros limpos seguem para a camada Silver. E precisamos de um relatório consolidado: quantos registros falharam, por qual motivo, de qual parceiro. Quero isso rodando antes do próximo board meeting."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "O conceito é simples: um registro pode falhar em múltiplas regras ao mesmo tempo. Por exemplo, uma venda pode ter `customer_id` nulo E `total_amount` negativo. O sistema precisa capturar TODAS as falhas de cada registro, não só a primeira. E no final, a soma de registros válidos + quarentena deve ser igual ao total original — nenhum dado se perde."

## Objetivos

Ao final deste exercício, você será capaz de:

- Definir múltiplas regras de validação (completude + unicidade + integridade + validade)
- Aplicar TODAS as regras simultaneamente a cada registro do dataset
- Adicionar metadados de quarentena: regra(s) violada(s), timestamp, severidade
- Separar o dataset em `df_valid` e `df_quarantine`
- Persistir a quarentena em `datalake/quarantine/` particionada por motivo e data
- Gerar um relatório consolidado de quarentena
- Validar o princípio de conservação: `df_valid.count() + df_quarantine.count() == total`

## Pré-requisitos

- Ambiente Docker rodando (Spark + Jupyter)
- Jupyter Notebook acessível em http://localhost:8888
- Dataset `dados_sujos/vendas_problemas.parquet` disponível na pasta `data/aula_06/`
- Tabelas de referência disponíveis:
  - `data/aula_06/dados_sujos/clientes_referencia.parquet`
  - `data/aula_06/dados_sujos/produtos_referencia.parquet`
- Exercícios 01, 02 e 03 concluídos (funções `check_completeness`, `check_uniqueness`, `check_referential_integrity` já implementadas)

## Duração Estimada

⏱️ ~15 minutos

---

## Problema

A DataFlow precisa de um sistema de quarentena que:

1. **Aplique 4 tipos de validação** em cada registro:
   - **Completude**: campos obrigatórios (`order_id`, `customer_id`, `product_id`, `order_date`) não podem ser nulos
   - **Unicidade**: não pode haver duplicatas de `order_id`
   - **Integridade referencial**: `customer_id` deve existir na tabela de clientes, `product_id` na tabela de produtos
   - **Validade de domínio**: `quantity > 0`, `unit_price > 0`, `total_amount > 0`, `order_date` não pode estar no futuro

2. **Marque cada registro** com as regras que ele violou (um registro pode violar múltiplas regras)

3. **Separe** em dois DataFrames:
   - `df_valid` — registros que passaram em TODAS as regras
   - `df_quarantine` — registros que falharam em pelo menos uma regra, com metadados

4. **Persista** a quarentena em Parquet particionado por `quarantine_reason` e `quarantine_date`

5. **Gere relatório** com:
   - Contagem por motivo de quarentena
   - Percentual total de quarentena vs. total
   - Distribuição de quarentena por `partner_source`

6. **Valide a conservação**: nenhum registro se perde na separação

---

## Comportamento Esperado

Ao executar sua solução com o dataset `vendas_problemas.parquet`, você deve obter resultados aproximados:

```
📊 RELATÓRIO DE QUARENTENA — DataFlow Analytics
================================================================
Total de registros analisados:     ~51,500
Registros válidos (Silver):        ~42,000 - ~44,000
Registros em quarentena:           ~7,500 - ~9,500
Taxa de quarentena:                ~15% - ~18%

📋 Quarentena por motivo:
  - completude (campos nulos):     ~2,500
  - unicidade (duplicatas):        ~1,500
  - integridade_referencial:       ~2,400
  - validade_dominio:              ~2,500
  (Nota: um registro pode aparecer em múltiplos motivos)

📋 Quarentena por parceiro:
  - parceiro_a:  ~XX%
  - parceiro_b:  ~YY%
  - parceiro_c:  ~ZZ%

✓ Validação de conservação: PASSED
  df_valid + df_quarantine == total_original
```

---

## Sua Tarefa

Implemente o sistema de quarentena seguindo estas etapas:

### Etapa 1: Definir as Regras de Validação

Crie colunas de flag para cada regra violada. Cada flag deve ser `True` quando o registro FALHA na validação.

### Etapa 2: Aplicar Todas as Regras Simultaneamente

Use `withColumn()` para adicionar as flags ao DataFrame. Para integridade referencial, use `left join` com as tabelas de referência e verifique se o match existe.

### Etapa 3: Adicionar Metadados de Quarentena

Para registros que falharam em pelo menos uma regra, adicione:
- `quarantine_reasons`: lista/string com todas as regras violadas (ex: `"completude|validade_dominio"`)
- `quarantine_ts`: timestamp do momento da análise
- `quarantine_severity`: "critical" se falhou em completude ou unicidade, "warning" caso contrário

### Etapa 4: Separar Válidos e Quarentena

Filtre registros sem nenhuma flag ativada → `df_valid`. O restante → `df_quarantine`.

### Etapa 5: Persistir Quarentena

Salve em Parquet particionado:
```
datalake/quarantine/
  reason=completude/
    date=2024-01-15/
  reason=unicidade/
    date=2024-01-15/
  reason=integridade_referencial/
    date=2024-01-15/
  reason=validade_dominio/
    date=2024-01-15/
```

### Etapa 6: Gerar Relatório

Produza o relatório consolidado com contagens e percentuais.

### Etapa 7: Validar Conservação

Assegure-se de que nenhum registro se perdeu na separação.

---

## Dicas

<details>
<summary>💡 Dica 1 — Como criar flags de validação com PySpark</summary>

Use `when()` para criar colunas booleanas:

```python
from pyspark.sql.functions import when, col, current_timestamp

df = df.withColumn(
    "flag_completude",
    when(col("campo_obrigatorio").isNull(), True).otherwise(False)
)
```

Para verificar múltiplos campos, combine com `|` (OR):

```python
df = df.withColumn(
    "flag_completude",
    when(
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("product_id").isNull() |
        col("order_date").isNull(),
        True
    ).otherwise(False)
)
```

</details>

<details>
<summary>💡 Dica 2 — Como verificar integridade referencial inline (sem separar)</summary>

Em vez de usar `left_anti join` (que separa os DataFrames), use `left join` e verifique se o match resultou em NULL:

```python
from pyspark.sql.functions import broadcast

df_com_ref = df.join(
    broadcast(df_clientes.select(col("customer_id").alias("_ref_customer_id"))),
    col("customer_id") == col("_ref_customer_id"),
    "left"
).withColumn(
    "flag_integridade_customer",
    when(
        col("customer_id").isNotNull() & col("_ref_customer_id").isNull(),
        True
    ).otherwise(False)
).drop("_ref_customer_id")
```

A ideia: se `customer_id` está preenchido mas o join não encontrou match (`_ref_customer_id` ficou NULL), então é um órfão.

</details>

<details>
<summary>💡 Dica 3 — Como combinar motivos em uma string</summary>

Use `concat_ws` com `when` para montar a lista de motivos:

```python
from pyspark.sql.functions import concat_ws, array, when, lit

# Abordagem com array + concat
df = df.withColumn(
    "quarantine_reasons",
    concat_ws("|",
        when(col("flag_completude"), lit("completude")),
        when(col("flag_unicidade"), lit("unicidade")),
        when(col("flag_integridade"), lit("integridade_referencial")),
        when(col("flag_validade"), lit("validade_dominio"))
    )
)
```

`concat_ws` ignora valores NULL automaticamente — se um flag é False, o `when` retorna NULL e o motivo não aparece na string.

</details>

<details>
<summary>💡 Dica 4 — Como detectar duplicatas inline (flag por registro)</summary>

Use Window Function com `row_number()` para marcar registros duplicados:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_dedup = Window.partitionBy("order_id").orderBy("order_date")

df = df.withColumn("_row_num", row_number().over(window_dedup))
df = df.withColumn(
    "flag_unicidade",
    when(col("_row_num") > 1, True).otherwise(False)
).drop("_row_num")
```

Assim, a primeira ocorrência recebe `row_number = 1` (válida), e as duplicatas recebem `row_number > 1` (quarentena).

</details>

<details>
<summary>💡 Dica 5 — Como verificar validade de domínio (valores inválidos)</summary>

Combine múltiplas condições de domínio:

```python
from pyspark.sql.functions import current_date

df = df.withColumn(
    "flag_validade",
    when(
        (col("quantity") <= 0) |
        (col("unit_price") <= 0) |
        (col("total_amount") <= 0) |
        (col("order_date") > current_date()),
        True
    ).otherwise(False)
)
```

</details>

<details>
<summary>💡 Dica 6 — Como particionar a persistência por múltiplas colunas</summary>

Para salvar em Parquet particionado por motivo E data:

```python
from pyspark.sql.functions import to_date, current_date

df_quarantine \
    .withColumn("quarantine_date", current_date()) \
    .write \
    .mode("overwrite") \
    .partitionBy("quarantine_reason", "quarantine_date") \
    .parquet("datalake/quarantine/")
```

**Atenção:** se `quarantine_reasons` contém múltiplos motivos separados por `|`, considere "explodir" o registro em múltiplas linhas (uma por motivo) usando `explode(split(...))` antes de particionar. Alternativamente, particione pelo motivo principal (o primeiro da lista).

</details>

---

## Critérios de Validação

Seu exercício está completo quando:

| # | Critério | Como verificar |
|---|----------|----------------|
| 1 | Todas as 4 regras estão implementadas | O relatório mostra contagens para completude, unicidade, integridade e validade |
| 2 | Um registro pode ter múltiplos motivos | Existem registros com `quarantine_reasons` contendo `\|` (pipe) |
| 3 | Metadados estão presentes | `df_quarantine` contém colunas `quarantine_reasons`, `quarantine_ts`, `quarantine_severity` |
| 4 | A separação está correta | `df_valid` tem ZERO flags ativadas; `df_quarantine` tem pelo menos uma |
| 5 | Conservação de dados | `assert df_valid.count() + df_quarantine.count() == total_original` |
| 6 | Persistência funciona | Os arquivos Parquet existem em `datalake/quarantine/` com partições visíveis |
| 7 | Relatório com distribuição por parceiro | O output mostra contagem de quarentena agrupada por `partner_source` |

---

## Perguntas para Reflexão

Antes de seguir para o próximo exercício, pense:

1. **Ordem dos checks importa?** Se um registro tem `customer_id` nulo, faz sentido verificar integridade referencial nele?
2. **Performance**: aplicar todos os checks em uma passada (com `withColumn` sequencial) é mais eficiente que rodar cada check separadamente? Por quê?
3. **Explosão de linhas**: se um registro falha em 3 regras e você usa `explode` para particionar, ele aparece 3 vezes na quarentena. Isso é problema? Como lidar?
4. **Idempotência**: se você rodar esse notebook duas vezes, o resultado é o mesmo? O que pode mudar? (Dica: pense no timestamp)

---

## Próximo Exercício

➡️ **Exercício 5 — Integração com Airflow** (`05_dag_qualidade.md`): orquestrar os checks de qualidade como tasks em uma DAG Airflow, com alertas automáticos quando a taxa de quarentena ultrapassa um threshold.
