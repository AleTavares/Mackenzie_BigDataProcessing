# Exercício 5 — Persistência Particionada por Data (Intermediário)

## Contexto

> **Marina Silva (CTO):** "Carlos, precisamos particionar nossos dados por data. Quando a Ana pede o relatório de uma semana específica, não faz sentido o Spark varrer 155 mil registros se ele só precisa de 7 dias. Com particionamento por data, o engine lê apenas os diretórios relevantes — é o chamado *partition pruning*. Além disso, quero que o pipeline diário seja idempotente: se rodarmos duas vezes no mesmo dia, o resultado deve ser o mesmo, sem duplicatas."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Perfeito, Marina. Vou particionar a Silver por `order_date` e configurar o *dynamic partition overwrite* — assim cada execução sobrescreve apenas a partição do dia processado, sem tocar nas demais. Também vou controlar o número de arquivos por partição com `coalesce()` para evitar o problema de *small files* que destrói a performance do cluster."

## Objetivos

Ao final deste exercício, você será capaz de:

- Particionar a Silver por `order_date` e entender a estrutura de diretórios criada
- Comparar escrita com e sem particionamento (tamanho e quantidade de arquivos)
- Usar `explain()` para visualizar partition pruning em ação
- Configurar dynamic partition overwrite para escrita idempotente
- Controlar o número de arquivos por partição com `coalesce()`
- Decidir quando particionar por dia, mês ou ano baseado no volume de dados

## Pré-requisitos

- SparkSession ativa (criada no Exercício 1)
- Camada Silver validada (Exercício 4 concluído)
- DataFrame `df_silver` com ~155K-160K registros e schema:
  - `order_id`, `customer_id`, `product_id`, `quantity`, `unit_price`, `total_amount`
  - `order_date` (tipo `date`), `payment_method`, `shipping_city`, `shipping_state`, `status`, `_source`

## Duração Estimada

⏱️ ~15 minutos

## Nível

🟡 **Intermediário** — As instruções descrevem O QUE fazer e fornecem dicas, mas NÃO incluem a solução completa. Você deve construir o código usando os conceitos aprendidos nos exercícios anteriores.

---

## Exercício 5.1: Escrita Particionada por `order_date`

### O que fazer

Persista o DataFrame Silver em Parquet, particionado pela coluna `order_date`. Após a escrita, explore a estrutura de diretórios criada no disco para entender como o Spark organiza os dados fisicamente.

### Dicas

1. Use `.write.mode("overwrite").partitionBy("order_date").parquet("caminho/destino/")`
2. O caminho sugerido é: `datalake/silver/vendas_por_data/`
3. Após gravar, liste os diretórios criados — cada valor de `order_date` vira uma pasta
4. Para listar a estrutura, você pode usar:
   ```python
   import os
   partitions = os.listdir("datalake/silver/vendas_por_data/")
   print(f"Total de partições: {len(partitions)}")
   for p in sorted(partitions)[:10]:
       print(f"  📂 {p}")
   ```
5. Conte quantos arquivos `.parquet` existem dentro de uma partição específica

### Critérios de Validação

- [ ] A escrita cria subdiretórios no formato `order_date=YYYY-MM-DD/`
- [ ] O número de partições corresponde ao número de datas distintas no DataFrame
- [ ] Cada partição contém pelo menos 1 arquivo `.parquet`
- [ ] A leitura de volta do diretório particionado retorna o mesmo número total de registros

---

## Exercício 5.2: Comparar Com e Sem Particionamento

### O que fazer

Grave o mesmo DataFrame Silver de duas formas: (1) sem particionamento e (2) particionado por `order_date`. Compare o tamanho total em disco e a quantidade de arquivos gerados em cada caso.

### Dicas

1. Grave sem particionamento em `datalake/silver/vendas_sem_particao/`:
   ```python
   df_silver.write.mode("overwrite").parquet("datalake/silver/vendas_sem_particao/")
   ```
2. Para calcular o tamanho total de um diretório:
   ```python
   import os
   def tamanho_diretorio(path):
       total = 0
       for dirpath, dirnames, filenames in os.walk(path):
           for f in filenames:
               fp = os.path.join(dirpath, f)
               total += os.path.getsize(fp)
       return total
   ```
3. Conte o número de arquivos `.parquet` em cada caso
4. Compare e analise: qual ocupa mais espaço? Qual tem mais arquivos? Por quê?

### Critérios de Validação

- [ ] Duas versões gravadas: com e sem particionamento
- [ ] Tamanho total em disco calculado para ambas (espere que o particionado seja ligeiramente maior por overhead de diretórios)
- [ ] Número de arquivos comparado (particionado terá mais arquivos distribuídos)
- [ ] Análise documentada explicando o trade-off: mais arquivos vs leitura seletiva

---

## Exercício 5.3: Demonstrar Partition Pruning com `explain()`

### O que fazer

Leia o diretório particionado e aplique um filtro por data. Use `.explain(True)` para visualizar o plano de execução e confirmar que o Spark aplica **PartitionFilters** — ou seja, ele lê apenas os diretórios das datas filtradas, ignorando as demais partições.

### Dicas

1. Leia o diretório particionado:
   ```python
   df_particionado = spark.read.parquet("datalake/silver/vendas_por_data/")
   ```
2. Aplique um filtro por data (ex: uma semana específica):
   ```python
   from pyspark.sql.functions import col
   df_filtrado = df_particionado.filter(
       (col("order_date") >= "2023-06-01") & (col("order_date") <= "2023-06-07")
   )
   ```
3. Execute `df_filtrado.explain(True)` e procure por **PartitionFilters** no plano físico
4. Compare com o mesmo filtro aplicado ao DataFrame **sem** particionamento — note a diferença no plano
5. No plano sem particionamento, o filtro aparece como **PushedFilters** (menos eficiente) ou no nível do scan completo

### Formato esperado na saída do explain

Procure por algo similar a:
```
+- FileScan parquet [...]
   PartitionFilters: [isnotnull(order_date), (order_date >= 2023-06-01), (order_date <= 2023-06-07)]
   PushedFilters: []
   ReadSchema: ...
```

### Critérios de Validação

- [ ] O `explain()` do DataFrame particionado mostra `PartitionFilters` com a condição de data
- [ ] O `explain()` do DataFrame sem partição NÃO mostra `PartitionFilters` (o filtro fica em outro nível)
- [ ] A contagem do resultado filtrado é consistente em ambos os casos (mesmo número de registros)
- [ ] Você entende a diferença: partition pruning elimina diretórios inteiros ANTES de ler os arquivos

---

## Exercício 5.4: Dynamic Partition Overwrite (Escrita Idempotente)

### O que fazer

Configure o Spark para usar **dynamic partition overwrite** — um modo de escrita que sobrescreve APENAS as partições que estão presentes no DataFrame sendo gravado, sem afetar as demais. Isso é essencial para pipelines idempotentes (rodar 2x produz o mesmo resultado).

### Cenário Prático

Simule o reprocessamento de um dia específico:
1. Filtre o `df_silver` para um único dia (ex: `2023-06-15`)
2. Configure o dynamic partition overwrite
3. Grave apenas esse dia no diretório particionado
4. Verifique que as outras partições (outros dias) continuam intactas

### Dicas

1. Habilite o dynamic partition overwrite **antes** da escrita:
   ```python
   spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   ```
2. O modo padrão é `"static"` — que sobrescreve TODAS as partições (perigoso!)
3. Com `"dynamic"`, ao gravar com `mode("overwrite")`, apenas as partições presentes nos dados são afetadas
4. Para validar:
   - Conte os registros totais ANTES da escrita
   - Grave apenas 1 dia com overwrite dinâmico
   - Conte os registros totais DEPOIS — devem ser os mesmos (o dia foi sobrescrito, não duplicado)
5. Compare com o comportamento `"static"` (sem configurar): o que aconteceria?

### Critérios de Validação

- [ ] A configuração `partitionOverwriteMode` está definida como `"dynamic"`
- [ ] Ao gravar dados de um único dia, apenas a partição daquele dia é sobrescrita
- [ ] As partições de outros dias permanecem inalteradas (contagem total estável)
- [ ] Rodar a mesma escrita 2x produz resultado idêntico (idempotência comprovada)
- [ ] Você sabe explicar a diferença entre overwrite estático e dinâmico

---

## Exercício 5.5: Controlar Arquivos por Partição com `coalesce()`

### O que fazer

Por padrão, o Spark pode gerar muitos arquivos pequenos em cada partição (um por task do executor). Use `coalesce(1)` para consolidar cada partição em um único arquivo, evitando o problema de **small files** que degrada performance em leituras futuras.

### Dicas

1. Primeiro, verifique quantos arquivos o Spark cria por padrão em uma partição:
   ```python
   import os
   arquivos = os.listdir("datalake/silver/vendas_por_data/order_date=2023-06-15/")
   print(f"Arquivos na partição: {len(arquivos)}")
   ```
2. Aplique `coalesce(1)` antes de gravar para forçar 1 arquivo por partição:
   ```python
   df_silver.coalesce(1).write.mode("overwrite") \
       .partitionBy("order_date") \
       .parquet("datalake/silver/vendas_por_data_compacto/")
   ```
3. Compare o número de arquivos antes/depois do coalesce
4. **Atenção**: `coalesce(1)` pode ser lento para DataFrames muito grandes — é indicado quando cada partição tem poucos registros (cenário de dados diários)
5. Diferença entre `coalesce()` e `repartition()`:
   - `coalesce(N)`: reduz partições sem shuffle (mais eficiente para reduzir)
   - `repartition(N)`: redistribui com shuffle (para aumentar ou redistribuir uniformemente)
6. Regra prática: almejar arquivos entre 128MB e 1GB por arquivo Parquet

### Critérios de Validação

- [ ] Sem coalesce: múltiplos arquivos por partição (padrão do Spark)
- [ ] Com `coalesce(1)`: exatamente 1 arquivo por partição
- [ ] O conteúdo é idêntico (mesma contagem de registros)
- [ ] Você entende quando usar `coalesce()` vs `repartition()`
- [ ] Você sabe o risco: `coalesce(1)` com partições muito grandes → OOM no executor

---

## Exercício 5.6: Boas Práticas — Quando Particionar por Dia, Mês ou Ano?

### O que fazer

Analise o volume de dados do DataFrame Silver por diferentes granularidades temporais e determine qual estratégia de particionamento é mais adequada. Crie colunas auxiliares (`year`, `month`, `year_month`) e compare os resultados.

### Dicas

1. Crie colunas auxiliares para diferentes granularidades:
   ```python
   from pyspark.sql.functions import year, month, date_format

   df_analise = df_silver \
       .withColumn("year", year("order_date")) \
       .withColumn("month", month("order_date")) \
       .withColumn("year_month", date_format("order_date", "yyyy-MM"))
   ```
2. Calcule estatísticas por cada granularidade:
   - Quantas partições seriam criadas?
   - Qual o número médio de registros por partição?
   - Qual o tamanho estimado por partição?
3. Use esta tabela de referência para decidir:

   | Registros por Partição | Recomendação |
   |------------------------|--------------|
   | < 1.000 | Partição muito granular → agrupar (ex: usar mês) |
   | 1.000 - 100.000 | Bom equilíbrio para dados diários |
   | 100.000 - 1.000.000 | Bom equilíbrio para dados mensais |
   | > 1.000.000 | Considerar partição mais granular (ex: dia) |

4. Para o dataset da DataFlow (~155K registros distribuídos em ~365 dias), qual é a média por dia?
5. Se fosse um dataset 100x maior, a decisão mudaria?

### Critérios de Validação

- [ ] Contagem de registros calculada para as 3 granularidades (dia, mês, ano)
- [ ] Média de registros por partição calculada para cada granularidade
- [ ] Decisão justificada sobre a melhor granularidade para o cenário DataFlow
- [ ] Explicação de quando cada estratégia é mais adequada
- [ ] Consideração sobre o impacto em queries típicas (ex: relatório semanal → dia é melhor)

---

## Resumo e Métricas Esperadas

Ao completar este exercício, você terá entendido:

```
╔══════════════════════════════════════════════════════════════════╗
║        RESUMO: PERSISTÊNCIA PARTICIONADA                         ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  📂 Particionamento:  order_date (YYYY-MM-DD)                    ║
║  📊 Partições criadas: ~365 (1 por dia de dados)                 ║
║  📄 Arquivos/partição: 1 (com coalesce) ou N (sem)               ║
║  ⚡ Partition pruning:  Confirma PartitionFilters no explain     ║
║  🔄 Idempotência:      dynamic partition overwrite               ║
║  🎯 Granularidade:     dia (ideal para ~155K registros/365 dias) ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### Conceitos Praticados

| Conceito | Operação |
|----------|----------|
| Particionamento | `partitionBy("order_date")` |
| Partition pruning | `explain(True)` → PartitionFilters |
| Escrita idempotente | `partitionOverwriteMode = "dynamic"` |
| Controle de arquivos | `coalesce(1)` antes do write |
| Análise de granularidade | `year()`, `month()`, `date_format()` |
| Comparação de estratégias | Contagem por granularidade |

### Trade-offs do Particionamento

| Fator | Poucos Partições (ano) | Muitas Partições (dia) |
|-------|------------------------|------------------------|
| Arquivos no disco | Poucos, grandes | Muitos, pequenos |
| Partition pruning | Menos efetivo | Mais efetivo |
| Listagem de diretórios | Rápida | Lenta (muitos dirs) |
| Escrita incremental | Sobrescreve muito | Sobrescreve pontual |
| Small files problem | Improvável | Provável sem coalesce |

> **Marina:** "Excelente trabalho! Agora nosso data lake está organizado para queries rápidas por data e o pipeline pode rodar diariamente sem risco de duplicatas. No próximo exercício — o desafio — vamos construir a camada Gold com agregações de negócio que a Ana precisa para os dashboards."

---

## Próximo Exercício

➡️ **Exercício 6 — Camada Gold: Agregações de Negócio (Desafio)** (`06_camada_gold.md`): construir métricas agregadas (faturamento diário, ticket médio por estado, top produtos) prontas para consumo por dashboards e relatórios
