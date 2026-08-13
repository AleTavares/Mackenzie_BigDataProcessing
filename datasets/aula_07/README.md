# 📊 Datasets - Aula 07: Pipeline End-to-End em Produção

## Contexto Narrativo

A **DataFlow Analytics** está pronta para colocar seu pipeline completo em produção. Após meses de desenvolvimento incremental, Marina (CTO) exige que todo o fluxo — desde a ingestão de dados brutos até a geração de métricas de negócio — funcione de forma automatizada, idempotente, observável e com checks de qualidade integrados. Este é o cenário mais próximo da realidade profissional dos alunos.

## Estrutura de Diretórios

```
aula_07/
└── producao/
    ├── incoming/                       # Dados brutos (simula chegada diária)
    │   ├── 2023-12-01/
    │   │   └── vendas.parquet
    │   ├── 2023-12-02/
    │   │   └── vendas.parquet
    │   ├── 2023-12-03/
    │   │   └── vendas.parquet
    │   ├── 2023-12-04/
    │   │   └── vendas.parquet
    │   ├── 2023-12-05/
    │   │   └── vendas.parquet
    │   ├── 2023-12-06/
    │   │   └── vendas.parquet
    │   └── 2023-12-07/
    │       └── vendas.parquet
    ├── produtos.parquet               # Catálogo de produtos
    └── clientes.parquet               # Base de clientes
```

## Arquivos

### producao/incoming/ — Dados de Vendas Diários

Arquivos Parquet simulando a chegada diária de dados de vendas. Cada subpasta representa um dia de dados recebidos.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Diretórios** | 7 (um por dia) |
| **Período** | 1 a 7 de Dezembro de 2023 |
| **Registros totais** | ~200.000 |
| **Registros por dia** | ~28.500 (média) |
| **Compressão** | Snappy |
| **Tamanho total** | ~25 MB |
| **Padrão de partição** | `YYYY-MM-DD/vendas.parquet` |
| **Seed** | 112 |

#### Schema (cada vendas.parquet)

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `order_id` | String (UUID) | Não | Identificador único do pedido | `a7b8c9d0-...` |
| `customer_id` | String | Não | ID do cliente (CUST_NNNNN) | `CUST_06789` |
| `product_id` | String | Não | ID do produto (PROD_NNNN) | `PROD_0456` |
| `quantity` | Integer | Não | Quantidade (1 a 20) | `4` |
| `unit_price` | Double | Não | Preço unitário em R$ | `349.90` |
| `total_amount` | Double | Não | Valor total calculado | `1399.60` |
| `order_date` | Timestamp | Não | Data do pedido | `2023-12-03` |
| `payment_method` | String | Sim | Forma de pagamento | `credit_card` |
| `shipping_city` | String | Sim | Cidade de entrega | `Recife` |
| `shipping_state` | String | Sim | UF de entrega | `PE` |
| `status` | String | Não | Status do pedido | `delivered` |
| `partner_source` | String | Sim | Parceiro de origem | `parceiro_b` |

---

### producao/produtos.parquet — Catálogo de Produtos

Base de produtos para enriquecimento (join) no pipeline.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Registros** | 5.000 |
| **Compressão** | Snappy |
| **Tamanho** | ~400 KB |
| **Seed** | 113 |

#### Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `product_id` | String | Não | ID do produto (chave primária) | `PROD_0001` |
| `product_name` | String | Não | Nome do produto | `Plataforma Digital Premium` |
| `category` | String | Não | Categoria principal | `Informática` |
| `subcategory` | String | Não | Subcategoria | `Notebooks` |
| `unit_price` | Double | Não | Preço base em R$ | `2499.00` |
| `weight_kg` | Double | Não | Peso em kg | `2.30` |
| `is_active` | Boolean | Não | Produto ativo (85% True) | `True` |

---

### producao/clientes.parquet — Base de Clientes

Base de clientes para enriquecimento (join) no pipeline.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Registros** | 50.000 |
| **Compressão** | Snappy |
| **Tamanho** | ~3 MB |
| **Seed** | 114 |

#### Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `customer_id` | String | Não | ID do cliente (chave primária) | `CUST_00001` |
| `customer_name` | String | Não | Nome completo | `Maria Oliveira Santos` |
| `email` | String | Não | E-mail do cliente | `maria.santos@email.com` |
| `phone` | String | Não | Telefone brasileiro | `(21) 99876-5432` |
| `city` | String | Não | Cidade do cliente | `Niterói` |
| `state` | String | Não | UF do cliente | `RJ` |
| `registration_date` | Timestamp | Não | Data de cadastro (2020-2023) | `2022-05-10` |
| `segment` | String | Não | Segmento de fidelidade | `Prata` |

**Segmentos**: Bronze (50%), Prata (30%), Ouro (15%), Platina (5%)

## Relações entre Datasets

```
producao/incoming/YYYY-MM-DD/vendas.parquet
    ├── customer_id → producao/clientes.parquet (customer_id)
    └── product_id  → producao/produtos.parquet (product_id)
```

## Pipeline End-to-End Esperado

O pipeline completo que o aluno deve implementar:

```
incoming/ (raw)
    ↓ [Ingestão + Validação]
Bronze (raw com metadados)
    ↓ [Limpeza + Normalização]
Silver (dados limpos e tipados)
    ↓ [Agregações + Joins]
Gold (métricas de negócio)
```

### Etapas do Pipeline:

1. **Ingestão**: Ler dados do dia de `incoming/YYYY-MM-DD/`
2. **Qualidade**: Aplicar checks (nulls, duplicatas, consistência)
3. **Enriquecimento**: Join com `produtos.parquet` e `clientes.parquet`
4. **Transformação**: Calcular métricas, classificar tickets
5. **Persistência**: Salvar em camadas Bronze → Silver → Gold
6. **Logging**: Registrar métricas de execução

## Como Regenerar

```bash
python gerar_datasets.py --aula 7
```

## Uso no Lab

Estes dados são utilizados nos exercícios da Aula 7 para:
- Containerizar Spark job como script Python com argumentos CLI
- Implementar logging estruturado no processamento
- Implementar escrita idempotente (overwrite por partição de data)
- Criar DAG Airflow que orquestra o pipeline via SparkSubmitOperator
- Adicionar checks de qualidade como task intermediária no DAG
- Integrar todos os componentes: Docker + Spark + Airflow + Data Quality
- Simular reprocessamento de dias anteriores (backfill)
- Montar Docker Compose completo com pipeline E2E automatizado
