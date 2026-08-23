# Exercício 6 — Desafio: Camada Gold — Agregações de Negócio

## Contexto

> **Ana Rodrigues (Product Owner):** "Marina, o Roberto pediu dashboards prontos para a reunião de segunda com os investidores. Ele quer ver três coisas: faturamento diário por estado para o mapa de calor, ranking dos top 10 produtos para o relatório de portfólio, e uma visão 360 de cada cliente para o time comercial. Os dados já estão na Silver, certo? Preciso disso consolidado e pronto para consumo — sem que o time de BI precise fazer joins ou agregações na hora."

> **Marina Silva (CTO):** "Ana, é exatamente para isso que existe a camada Gold. É onde transformamos dados limpos em informação de negócio. Carlos construiu a Silver com 155 mil registros unificados e validados — agora vamos criar tabelas analíticas pré-agregadas que respondem diretamente às perguntas do Roberto. A Gold é o 'último metro' antes do dashboard."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "O desafio aqui é pensar do ponto de vista de quem consome: a Ana e o Roberto não querem ver `order_id` e `customer_id` em colunas separadas — eles querem KPIs prontos. Faturamento, ticket médio, lifetime value. E tudo precisa ser persistido em Parquet para performance máxima nas consultas."

## Objetivos

Ao final deste exercício, você será capaz de:

- Projetar tabelas Gold orientadas a casos de uso de negócio (não a schemas técnicos)
- Implementar agregações com `groupBy`, `agg`, `sum`, `avg`, `count`, `min`, `max`
- Calcular métricas de negócio: faturamento, ticket médio, lifetime value, número de canais
- Usar `countDistinct` para métricas de diversidade (canais, produtos)
- Persistir múltiplas tabelas Gold em Parquet otimizado
- Validar integridade das tabelas geradas (contagens, nulls, consistência)

## Pré-requisitos

- SparkSession ativa
- Camada Silver persistida e particionada (Exercícios 4 e 5 concluídos)
- DataFrame Silver com ~155K+ registros e schema:
  ```
  order_id, customer_id, product_id, quantity, unit_price, total_amount,
  order_date (date), payment_method, shipping_city, shipping_state, status, _source
  ```

## Duração Estimada

⏱️ ~15-20 minutos

## Nível de Dificuldade

🔴 **Desafio** — Orientação mínima. Você recebe os requisitos de negócio, hints e critérios de validação, mas deve construir toda a implementação de forma independente. Este exercício simula um cenário real: a PO deu os requisitos e você precisa entregar.

---

## Os 3 Requisitos de Negócio da Ana

### Gold Table 1: Faturamento Diário por Estado

> **Ana:** "Preciso alimentar um mapa de calor que mostra a evolução do faturamento por estado ao longo do tempo. Cada ponto do gráfico é um par (data, estado) com o faturamento total, número de pedidos e ticket médio daquele dia naquele estado."

**Schema esperado:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `order_date` | date | Data do faturamento |
| `shipping_state` | string | UF do estado |
| `revenue` | double | Faturamento total (soma de `total_amount`) |
| `orders` | long | Quantidade de pedidos |
| `avg_ticket` | double | Ticket médio (`revenue / orders`) |

---

### Gold Table 2: Top 10 Produtos por Faturamento

> **Ana:** "O time comercial quer saber quais são nossos top 10 produtos em faturamento total. Para cada produto, preciso do faturamento total, quantidade vendida e preço médio praticado."

**Schema esperado:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `product_id` | string | Identificador do produto |
| `total_revenue` | double | Faturamento total do produto |
| `total_quantity` | long | Total de unidades vendidas |
| `avg_price` | double | Preço médio unitário praticado |

**Atenção:** A tabela deve conter **exatamente** 10 registros (os 10 maiores).

---

### Gold Table 3: Visão 360° do Cliente

> **Ana:** "O CRM precisa de uma tabela com a visão completa de cada cliente: quanto ele já gastou no total (lifetime value), quantos pedidos fez, quando foi a primeira e última compra, e de quantos canais/parceiros diferentes ele comprou. Isso é essencial para segmentação."

**Schema esperado:**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `customer_id` | string | Identificador do cliente |
| `lifetime_value` | double | Soma total gasta pelo cliente |
| `total_orders` | long | Número de pedidos realizados |
| `first_purchase` | date | Data da primeira compra |
| `last_purchase` | date | Data da última compra |
| `num_channels` | long | Quantidade de canais distintos (`_source`) |

---

## Hints

<details>
<summary>💡 Hint 1: Estrutura geral</summary>

Leia a Silver, aplique `groupBy` + `agg` com funções diferentes para cada tabela Gold. Persista cada uma em diretório separado dentro de `datalake/gold/`.

```python
# Estrutura de diretórios esperada:
# datalake/gold/faturamento_diario_estado/
# datalake/gold/top_produtos/
# datalake/gold/visao_cliente/
```
</details>

<details>
<summary>💡 Hint 2: Funções úteis para as agregações</summary>

```python
from pyspark.sql.functions import (
    col, sum, avg, count, min, max,
    countDistinct, round
)
```

- `sum("total_amount")` → faturamento
- `count("order_id")` → número de pedidos
- `avg("total_amount")` → ticket médio
- `min("order_date")` → primeira compra
- `max("order_date")` → última compra
- `countDistinct("_source")` → número de canais

</details>

<details>
<summary>💡 Hint 3: Top 10 — como limitar resultados</summary>

Para obter apenas os top 10, há duas abordagens:
- `.orderBy(col("total_revenue").desc()).limit(10)` — simples e eficaz
- Window function com `row_number()` — mais flexível mas mais complexo

Para este exercício, `.limit(10)` é suficiente.
</details>

<details>
<summary>💡 Hint 4: Persistência em Parquet</summary>

Use `mode("overwrite")` para tabelas Gold (são recalculadas a cada execução):

```python
df_gold.write.mode("overwrite").parquet("datalake/gold/nome_tabela/")
```

Gold tables geralmente NÃO são particionadas (são pequenas e otimizadas para leitura completa). Exceção: `faturamento_diario_estado` pode se beneficiar de particionamento por estado se o volume crescer.
</details>

<details>
<summary>💡 Hint 5: Validação básica</summary>

Após criar cada tabela, valide:
- `.count()` retorna o número esperado de registros
- Nenhum null em colunas-chave (use `.filter(col("key").isNull()).count() == 0`)
- Valores fazem sentido de negócio (faturamento > 0, datas no range esperado)
</details>

---

## Critérios de Validação

### Gold Table 1: Faturamento Diário por Estado

- [ ] Schema correto com 5 colunas: `order_date`, `shipping_state`, `revenue`, `orders`, `avg_ticket`
- [ ] Número de registros = (datas distintas) × (estados distintos presentes por data)
- [ ] Zero nulls em `order_date` e `shipping_state` (são a chave)
- [ ] `revenue` > 0 em todos os registros
- [ ] `avg_ticket` ≈ `revenue / orders` (conferir consistência)
- [ ] Soma de `revenue` em toda a tabela ≈ soma de `total_amount` na Silver
- [ ] Persistida em `datalake/gold/faturamento_diario_estado/`

### Gold Table 2: Top 10 Produtos por Faturamento

- [ ] Schema correto com 4 colunas: `product_id`, `total_revenue`, `total_quantity`, `avg_price`
- [ ] Exatamente 10 registros
- [ ] Ordenada por `total_revenue` decrescente (primeiro = maior faturamento)
- [ ] Zero nulls em `product_id`
- [ ] `total_revenue` > 0, `total_quantity` > 0, `avg_price` > 0
- [ ] Persistida em `datalake/gold/top_produtos/`

### Gold Table 3: Visão 360° do Cliente

- [ ] Schema correto com 6 colunas: `customer_id`, `lifetime_value`, `total_orders`, `first_purchase`, `last_purchase`, `num_channels`
- [ ] Número de registros = número de `customer_id` distintos na Silver
- [ ] Zero nulls em `customer_id` (é a chave primária)
- [ ] `lifetime_value` > 0, `total_orders` >= 1
- [ ] `first_purchase` <= `last_purchase` para todo cliente
- [ ] `num_channels` entre 1 e 3 (máximo de parceiros)
- [ ] Soma de `lifetime_value` ≈ soma de `total_amount` na Silver
- [ ] Persistida em `datalake/gold/visao_cliente/`

### Validação Geral

- [ ] As 3 tabelas Gold estão persistidas em Parquet em `datalake/gold/`
- [ ] Round-trip test: ler de volta cada tabela e confirmar contagens
- [ ] A soma de faturamento é consistente entre Gold Table 1, Gold Table 3 e o total da Silver

---

## BÔNUS: Schema Evolution — A Silver Mudou, e Agora?

> **Marina:** "Carlos, acabamos de fechar com um quarto parceiro que envia uma coluna extra: `discount_percentage`. Ela vai aparecer na Silver a partir de amanhã. As tabelas Gold que você construiu vão continuar funcionando sem quebrar? Prove."

### O que fazer

1. Adicione uma coluna nova ao DataFrame Silver (simule o novo parceiro):
   ```python
   df_silver_v2 = df_silver.withColumn("discount_percentage", lit(0.0))
   ```
2. Persista a Silver v2 (com a coluna extra) sobre a mesma pasta
3. Re-execute o pipeline Gold **sem modificar nenhum código da Gold**
4. Verifique que as 3 tabelas Gold são geradas corretamente — a coluna extra é simplesmente ignorada

### Critério de Validação (Bônus)

- [ ] A Silver agora tem 13 colunas (12 originais + `discount_percentage`)
- [ ] As 3 tabelas Gold foram recriadas sem erro
- [ ] As contagens e valores das tabelas Gold são idênticos (antes e depois da evolução)
- [ ] Nenhuma modificação foi necessária no código das agregações Gold

---

## Resumo e Métricas Esperadas

```
╔══════════════════════════════════════════════════════════════════╗
║             RESUMO DA CAMADA GOLD                                ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  📊 Gold Table 1: Faturamento Diário/Estado                      ║
║     → Registros: (datas) × (estados) — variável                  ║
║     → Granularidade: 1 registro = 1 dia + 1 estado               ║
║                                                                  ║
║  📊 Gold Table 2: Top Produtos                                   ║
║     → Registros: exatamente 10                                   ║
║     → Granularidade: 1 registro = 1 produto                      ║
║                                                                  ║
║  📊 Gold Table 3: Visão Cliente 360°                             ║
║     → Registros: = clientes distintos na Silver                  ║
║     → Granularidade: 1 registro = 1 cliente                      ║
║                                                                  ║
║  📂 Formato: Parquet (sem particionamento)                       ║
║  ✏️  Modo de escrita: overwrite (recalculável)                    ║
║  🎯 Consumidores: Dashboards, BI, CRM                            ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

### Arquitetura Medallion Completa (Aula 3)

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│     BRONZE      │      │     SILVER      │      │      GOLD       │
│                 │      │                 │      │                 │
│ • Raw data      │ ──── │ • Normalizado   │ ──── │ • Agregado      │
│ • Multi-formato │      │ • Schema único  │      │ • KPIs prontos  │
│ • 3 parceiros   │      │ • Validado      │      │ • 3 tabelas     │
│ • ~160K registros│     │ • ~155K+ registros│    │ • Business-ready│
│ • append mode   │      │ • overwrite     │      │ • overwrite     │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

### Conceitos Praticados

| Conceito | Operação |
|----------|----------|
| Agregação de negócio | `groupBy().agg(sum, avg, count, min, max)` |
| Métricas derivadas | Ticket médio, lifetime value, num_channels |
| Limitação de resultados | `.orderBy().limit(10)` |
| Diversidade de canais | `countDistinct("_source")` |
| Persistência Gold | `mode("overwrite").parquet(...)` sem partição |
| Validação de integridade | Conferência cruzada entre camadas |
| Schema evolution | Resiliência a colunas novas na Silver |

### Diferenças Silver vs Gold

| Aspecto | Silver | Gold |
|---------|--------|------|
| Granularidade | 1 registro = 1 transação | 1 registro = 1 agregação |
| Volume | Alto (~155K+) | Baixo (10 a poucos milhares) |
| Schema | Transacional (order_id, etc.) | Analítico (KPIs, métricas) |
| Consumidor | Engenheiros de dados | Analistas, POs, dashboards |
| Atualização | Recalcula tudo | Recalcula a partir da Silver |
| Particionamento | Por data (otimizar filtros) | Geralmente sem (dados pequenos) |

> **Ana:** "Perfeito! Com essas 3 tabelas Gold, posso montar os dashboards do Roberto em 30 minutos. Mapa de calor por estado, ranking de produtos e segmentação de clientes — tudo pronto. Obrigada, Carlos!"

> **Marina:** "Ótimo trabalho! Agora temos o pipeline completo: Bronze → Silver → Gold. Na próxima aula, vamos automatizar tudo isso com Apache Airflow para que rode diariamente sem intervenção manual."

---

## Entregável

Ao final deste desafio, seu notebook deve conter:

- [ ] As 3 tabelas Gold criadas e persistidas em `datalake/gold/`
- [ ] Validações de cada tabela (contagem, nulls, consistência)
- [ ] Conferência cruzada: soma de faturamento Gold ≈ soma Silver
- [ ] (Bônus) Teste de schema evolution bem-sucedido

---

## Próxima Aula

➡️ **Aula 4 — Introdução ao Apache Airflow**: automatizar o pipeline Bronze → Silver → Gold com DAGs programáticas, scheduling diário e monitoramento via web UI.
