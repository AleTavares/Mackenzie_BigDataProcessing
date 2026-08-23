# Exercício 4 — Camada Silver: Normalização de Schemas e Unificação (Intermediário)

## Contexto

> **Carlos Mendes (Engenheiro de Dados Sênior):** "A Bronze está impecável — 160 mil registros de 3 parceiros, persistidos em Parquet com metadados completos. Agora vem o desafio real: cada parceiro enviou colunas com nomes diferentes, tipos incompatíveis e campos extras. O Parceiro A usa nomes em português (`cod_pedido`, `qtd`, `valor_total`), enquanto B e C usam inglês padronizado (`order_id`, `quantity`, `total_amount`). Na Silver, nosso trabalho é normalizar tudo em um schema único e limpo. Também é aqui que aplicamos as primeiras validações de negócio — registros com `total_amount` negativo ou `order_date` nulo não passam. Eles vão para a quarentena."

> **Marina Silva (CTO):** "Carlos, esse é exatamente o tipo de trabalho que separa um data lake profissional de um 'data swamp'. Se a Silver não estiver bem feita, tudo que construímos na Gold vai herdar os problemas. Faz direito!"

## Objetivos

Ao final deste exercício, você será capaz de:

- Ler dados da camada Bronze (persistida no exercício anterior)
- Normalizar colunas do Parceiro A de português para o schema padrão em inglês
- Unificar tipos de dados entre os 3 parceiros (casting)
- Aplicar validações de negócio para separar dados válidos e inválidos
- Implementar quarentena para registros que falham nas validações
- Unir os 3 parceiros em um único DataFrame com `unionByName`
- Selecionar o schema final da Silver (sem colunas extras de parceiros específicos)
- Validar o DataFrame unificado (contagens, nulls, consistência)

## Pré-requisitos

- SparkSession ativa (criada no Exercício 1)
- Camada Bronze persistida em `datalake/bronze/vendas/` (Exercício 3 concluído)
- ~160.000 registros na Bronze:
  - `_source=parceiro_a`: ~50.000 registros (colunas em português)
  - `_source=parceiro_b`: ~30.000 registros (colunas em inglês + `api_exported_at`)
  - `_source=parceiro_c`: ~80.000 registros (colunas em inglês)

## Duração Estimada

⏱️ ~20 minutos

## Nível de Dificuldade

🟡 **Intermediário** — As instruções descrevem O QUE fazer e fornecem dicas, mas NÃO incluem a solução completa. Você deve construir o código usando os conceitos aprendidos nos exercícios guiados.

---

## Schema de Referência: O Que a Silver Deve Ter

Antes de começar, este é o schema final padronizado que todos os parceiros devem seguir na Silver:

```
root
 |-- order_id: string (nullable = false)
 |-- customer_id: string (nullable = false)
 |-- product_id: string (nullable = true)
 |-- quantity: long (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- total_amount: double (nullable = false)
 |-- order_date: date (nullable = false)
 |-- payment_method: string (nullable = true)
 |-- shipping_city: string (nullable = true)
 |-- shipping_state: string (nullable = true)
 |-- status: string (nullable = true)
 |-- _source: string (nullable = true)
```

**Regras de negócio para validação:**
- `order_id` não pode ser nulo
- `total_amount` deve ser maior que 0
- `order_date` não pode ser nulo

---

## Exercício 4.1: Ler os 3 Parceiros da Bronze

### O que fazer

Leia os dados de cada parceiro separadamente a partir da camada Bronze (`datalake/bronze/vendas/`), filtrando por `_source`. Você pode ler o diretório completo e filtrar, ou ler cada partição diretamente.

### Dicas

1. Use `spark.read.parquet("datalake/bronze/vendas/")` para ler toda a Bronze
2. Filtre por `_source` para separar cada parceiro: `.filter(col("_source") == "parceiro_a")`
3. Verifique o schema de cada um com `.printSchema()` — note as diferenças entre os parceiros
4. Confirme as contagens para garantir que a leitura está correta

### Critérios de Validação

- [ ] `df_a` contém ~50.000 registros com colunas em português (`cod_pedido`, `qtd`, etc.)
- [ ] `df_b` contém ~30.000 registros com colunas em inglês + coluna extra `api_exported_at`
- [ ] `df_c` contém ~80.000 registros com colunas em inglês
- [ ] A soma dos 3 DataFrames é ~160.000 (igual ao total da Bronze)

---

## Exercício 4.2: Normalizar Colunas do Parceiro A (Português → Inglês)

### O que fazer

O Parceiro A usa nomes de colunas em português. Renomeie-as para o schema padrão em inglês. Use `.withColumnRenamed()` para cada coluna que precisa mudar.

### Mapeamento de Colunas (Parceiro A → Silver)

| Parceiro A (original) | Silver (padrão) |
|------------------------|-----------------|
| `cod_pedido` | `order_id` |
| `cod_cliente` | `customer_id` |
| `cod_produto` | `product_id` |
| `qtd` | `quantity` |
| `preco_unit` | `unit_price` |
| `valor_total` | `total_amount` |
| `data_pedido` | `order_date` |
| `forma_pagamento` | `payment_method` |
| `cidade_entrega` | `shipping_city` |
| `uf_entrega` | `shipping_state` |
| `situacao` | `status` |

### Dicas

1. Encadeie `.withColumnRenamed("nome_antigo", "nome_novo")` para cada coluna
2. Alternativamente, use `.toDF(*nova_lista_de_nomes)` — mas cuidado com a ordem!
3. Outra opção elegante: use um dicionário de mapeamento e um loop:
   ```python
   mapeamento = {"cod_pedido": "order_id", "cod_cliente": "customer_id", ...}
   for antigo, novo in mapeamento.items():
       df_a = df_a.withColumnRenamed(antigo, novo)
   ```
4. Após renomear, valide com `.printSchema()` que as colunas agora batem com o schema da Silver

### Critérios de Validação

- [ ] Todas as 11 colunas do mapeamento foram renomeadas corretamente
- [ ] Nenhuma coluna com nome em português permanece no DataFrame
- [ ] O `.printSchema()` mostra nomes em inglês consistentes com o schema de referência
- [ ] A contagem de registros não mudou (~50.000)

---

## Exercício 4.3: Unificar Tipos de Dados (Casting)

### O que fazer

Após normalizar os nomes, os tipos de dados podem estar inconsistentes entre os parceiros. O Parceiro A pode ter `quantity` como `int` enquanto B e C têm como `bigint` (long). Faça o casting necessário para que os 3 parceiros tenham os mesmos tipos.

### Tipos esperados na Silver

| Coluna | Tipo esperado |
|--------|---------------|
| `order_id` | string |
| `customer_id` | string |
| `product_id` | string |
| `quantity` | long (bigint) |
| `unit_price` | double |
| `total_amount` | double |
| `order_date` | date |
| `payment_method` | string |
| `shipping_city` | string |
| `shipping_state` | string |
| `status` | string |

### Dicas

1. Use `.withColumn("quantity", col("quantity").cast("long"))` para converter tipos
2. Faça o cast de `total_amount` para `double` em todos os parceiros
3. Se `order_date` estiver como string, converta com `to_date(col("order_date"), "yyyy-MM-dd")` — ajuste o formato conforme necessário
4. Aplique os mesmos casts nos 3 DataFrames para garantir uniformidade
5. Valide com `.dtypes` que os tipos estão corretos

### Critérios de Validação

- [ ] `quantity` é `bigint` (long) nos 3 parceiros
- [ ] `total_amount` é `double` nos 3 parceiros
- [ ] `order_date` é `date` nos 3 parceiros
- [ ] Nenhum erro de cast (registros nulos inesperados após conversão)

---

## Exercício 4.4: Aplicar Validações de Negócio e Quarentena

### O que fazer

Antes de unir os dados, aplique as regras de negócio para separar registros válidos dos inválidos. Registros que falham nas validações vão para a **quarentena** (serão investigados depois). Registros válidos seguem para a Silver.

### Regras de Validação

| Regra | Condição para ser VÁLIDO |
|-------|--------------------------|
| R1 | `order_id` não é nulo |
| R2 | `total_amount > 0` |
| R3 | `order_date` não é nulo |

### Dicas

1. Construa uma condição composta para registros válidos:
   ```python
   condicao_valido = (
       col("order_id").isNotNull() &
       (col("total_amount") > 0) &
       col("order_date").isNotNull()
   )
   ```
2. Separe válidos e inválidos com `.filter(condicao_valido)` e `.filter(~condicao_valido)`
3. Aplique em cada parceiro separadamente (ou após a união — você decide a estratégia)
4. Para a quarentena, adicione colunas de metadados:
   - `_quarantine_reason`: qual regra falhou
   - `_quarantine_ts`: timestamp de quando foi quarentenado
5. Use `when` para indicar qual regra falhou:
   ```python
   .withColumn("_quarantine_reason",
       when(col("order_id").isNull(), "order_id nulo")
       .when(col("total_amount") <= 0, "total_amount <= 0")
       .when(col("order_date").isNull(), "order_date nulo")
   )
   ```

### Critérios de Validação

- [ ] Registros válidos atendem TODAS as 3 regras simultaneamente
- [ ] Registros em quarentena violam pelo menos 1 regra
- [ ] A soma de válidos + quarentena = total de registros lidos da Bronze
- [ ] A quarentena contém metadados (`_quarantine_reason`, `_quarantine_ts`)
- [ ] Mostre a contagem de quarentenados por motivo — espere uma pequena porcentagem (~1-3%)

---

## Exercício 4.5: Unir os 3 Parceiros com `unionByName`

### O que fazer

Agora que os 3 parceiros têm schemas normalizados, una-os em um único DataFrame. Use `unionByName` em vez de `union` — ele faz o match por **nome de coluna** (não por posição), o que é mais seguro quando schemas podem ter colunas em ordens diferentes.

### Dicas

1. Use `unionByName` com `allowMissingColumns=True` para lidar com colunas que existem em um parceiro mas não em outro:
   ```python
   df_unificado = df_a_valido \
       .unionByName(df_b_valido, allowMissingColumns=True) \
       .unionByName(df_c_valido, allowMissingColumns=True)
   ```
2. O `allowMissingColumns=True` preenche com `null` colunas que não existem em um dos parceiros (ex: `api_exported_at` do Parceiro B não existe em A e C)
3. Após a união, verifique o schema resultante — pode ter colunas extras que precisam ser removidas

### Critérios de Validação

- [ ] O DataFrame unificado contém registros de todos os 3 parceiros
- [ ] A contagem total = soma dos válidos de A + B + C
- [ ] A coluna `_source` identifica a origem de cada registro
- [ ] Não há duplicatas introduzidas pela união

---

## Exercício 4.6: Selecionar Schema Final da Silver

### O que fazer

O DataFrame unificado pode conter colunas extras de parceiros específicos (ex: `api_exported_at` do Parceiro B, `origem` do Parceiro A). Selecione apenas as colunas do schema padrão da Silver para manter o DataFrame limpo e padronizado.

### Colunas do Schema Final Silver

```python
colunas_silver = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_date",
    "payment_method",
    "shipping_city",
    "shipping_state",
    "status",
    "_source"
]
```

### Dicas

1. Use `.select(colunas_silver)` para selecionar apenas as colunas desejadas
2. Isso descarta automaticamente colunas extras como `api_exported_at`, `_ingestion_ts`, `_file_origin`, `origem`, etc.
3. O `_source` é mantido na Silver para rastreabilidade (saber de qual parceiro veio cada registro)
4. Valide com `.printSchema()` que o schema final tem exatamente 12 colunas

### Critérios de Validação

- [ ] O DataFrame final tem exatamente 12 colunas (as listadas acima)
- [ ] Nenhuma coluna extra de parceiros específicos permanece
- [ ] A coluna `_source` está presente para rastreabilidade
- [ ] A contagem de registros não mudou após o select

---

## Exercício 4.7: Validar o DataFrame Silver Final

### O que fazer

Antes de persistir, faça validações finais no DataFrame unificado. Garanta que os dados estão consistentes, sem nulls em campos obrigatórios e com contagens que fazem sentido.

### Validações Obrigatórias

| Validação | O que verificar |
|-----------|-----------------|
| Contagem total | ~155K-160K registros (descontando quarentena) |
| Nulls em campos obrigatórios | `order_id`, `total_amount`, `order_date` devem ter 0 nulls |
| Distribuição por `_source` | Parceiro A ~50K, B ~30K, C ~80K (aproximado) |
| Valores de `status` | Apenas valores válidos: pending, shipped, delivered, cancelled |
| `total_amount` positivo | 100% dos registros com total_amount > 0 |

### Dicas

1. Para verificar nulls em campos obrigatórios:
   ```python
   from pyspark.sql.functions import count, when, isnull

   df_silver.select([
       count(when(isnull(c), c)).alias(f"nulls_{c}")
       for c in ["order_id", "total_amount", "order_date"]
   ]).show()
   ```
2. Verifique a distribuição por source: `df_silver.groupBy("_source").count().show()`
3. Verifique valores de status: `df_silver.select("status").distinct().show()`
4. Use `assert` para validações programáticas que devem passar (zero tolerance)

### Critérios de Validação

- [ ] Zero nulls em `order_id`, `total_amount` e `order_date`
- [ ] Contagem total coerente (soma dos válidos dos 3 parceiros)
- [ ] Distribuição por `_source` proporcional ao esperado
- [ ] Todos os valores de `status` são válidos
- [ ] Todos os `total_amount` são positivos
- [ ] `printSchema()` mostra exatamente 12 colunas com tipos corretos

---

## Exercício 4.8: Persistir a Silver (Parquet Particionado)

### O que fazer

Grave o DataFrame Silver validado em Parquet, particionado por `_source`. Diferente da Bronze (que é `append`), na Silver usamos `mode("overwrite")` — pois a Silver é recalculada a cada execução do pipeline.

### Dicas

1. Caminho de destino: `datalake/silver/vendas/`
2. Use `mode("overwrite")` — na Silver, recalculamos tudo a cada run
3. Particione por `_source` para manter isolamento e permitir leitura seletiva
4. Após gravar, faça um round-trip test: leia de volta e compare contagens
5. Também persista a quarentena em `datalake/silver/quarentena/` para análise posterior

### Critérios de Validação

- [ ] Silver gravada em `datalake/silver/vendas/` no formato Parquet
- [ ] Particionada por `_source`
- [ ] Round-trip test passa (contagem lida == contagem gravada)
- [ ] Quarentena salva separadamente em `datalake/silver/quarentena/`
- [ ] Estrutura no disco mostra partições corretas (`_source=parceiro_a/`, etc.)

---

## Resumo e Métricas Esperadas

Ao completar este exercício, seu pipeline deve produzir:

```
╔══════════════════════════════════════════════════════════════════╗
║             RESUMO DA CAMADA SILVER                              ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  📊 Registros na Bronze (entrada):    ~160,000                   ║
║  ✅ Registros na Silver (válidos):    ~155,000 - 159,000         ║
║  🔒 Registros em Quarentena:          ~1,000 - 5,000             ║
║  📉 Taxa de rejeição:                 ~1% - 3%                   ║
║                                                                  ║
║  📋 Schema: 12 colunas padronizadas                              ║
║  📂 Formato: Parquet (Snappy)                                    ║
║  📂 Particionamento: _source                                     ║
║  ✏️  Modo de escrita: overwrite                                   ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### Conceitos Praticados

| Conceito | Operação |
|----------|----------|
| Normalização de schema | `withColumnRenamed()` para mapear colunas |
| Unificação de tipos | `.cast("long")`, `.cast("double")`, `to_date()` |
| Validação de negócio | Filtros com condições compostas |
| Quarentena | Separar válidos/inválidos com metadados |
| União padronizada | `unionByName(allowMissingColumns=True)` |
| Schema enforcement | `.select(colunas_silver)` para schema fixo |
| Persistência Silver | `mode("overwrite")` + `partitionBy` |

### Diferenças Bronze vs Silver

| Aspecto | Bronze | Silver |
|---------|--------|--------|
| Dados | Raw, como veio | Normalizados, limpos |
| Schema | Varia por parceiro | Unificado, padronizado |
| Tipos | Inferidos | Enforced (casting explícito) |
| Validação | Nenhuma | Regras de negócio aplicadas |
| Registros inválidos | Todos entram | Quarentenados separadamente |
| Modo de escrita | `append` (imutável) | `overwrite` (recalculável) |
| Colunas extras | Todas preservadas | Apenas as do schema padrão |

> **Carlos:** "Silver pronta! Os dados agora estão normalizados, validados e unificados. Qualquer análise que a Ana precisar pode partir da Silver com confiança — sem se preocupar com encoding, nomes de colunas ou tipos incompatíveis. No próximo exercício, vamos particionar por data e no desafio construiremos a Gold com agregações de negócio."

---

## Próximo Exercício

➡️ **Exercício 5 — Persistência Particionada por Data** (`05_persistencia_particionada.md`): particionar a Silver por `order_date` para otimizar consultas temporais e simular processamento incremental

