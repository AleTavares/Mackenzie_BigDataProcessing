# 📊 Datasets - Curso Big Data Processing

## Visão Geral

Esta pasta contém todos os datasets sintéticos utilizados ao longo do curso **Big Data Processing** (MBA em Engenharia de Dados - Universidade Mackenzie). Os dados simulam o cenário da empresa fictícia **DataFlow Analytics**, uma startup brasileira de análise de dados para e-commerce e varejo.

Todos os datasets são **sintéticos** — nenhum dado real de empresas ou pessoas é utilizado. Os dados são gerados com a biblioteca Faker (locale pt_BR) e seeds fixas para garantir reprodutibilidade.

## Como Regenerar os Datasets

```bash
# Gerar todos os datasets
python gerar_datasets.py --all

# Gerar apenas uma aula específica
python gerar_datasets.py --aula 1

# Gerar múltiplas aulas
python gerar_datasets.py --aula 1 2 3

# Listar datasets disponíveis
python gerar_datasets.py --list

# Usar seed customizada
python gerar_datasets.py --all --seed 123
```

**Dependências**: `pandas`, `numpy`, `faker`, `pyarrow`

## Datasets por Aula

| Aula | Pasta | Descrição | Formato | Registros | Tamanho Aprox. |
|------|-------|-----------|---------|-----------|----------------|
| 01 | `aula_01/` | Fundamentos Spark - Vendas e Produtos | CSV | 100K + 5K | ~15 MB |
| 02 | `aula_02/` | Transformações Avançadas - Multi-fonte | Parquet, JSON | 1M + 500K | ~80 MB |
| 03 | `aula_03/` | Ingestão Multi-formato - 3 Parceiros | CSV, JSON, Parquet | 160K | ~25 MB |
| 04 | `aula_04/` | Intro Airflow - Vendas Particionadas | CSV (particionado) | 30K | ~5 MB |
| 06 | `aula_06/` | Qualidade de Dados - Dados com Problemas | CSV, Parquet | 50K+ | ~12 MB |
| 07 | `aula_07/` | Pipeline E2E - Produção Completa | Parquet | 200K + 5K + 50K | ~35 MB |

## Schema Principal: Vendas

O schema de vendas é o modelo de dados central do curso, utilizado em todas as aulas com variações de formato e volume.

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `order_id` | String (UUID) | Identificador único do pedido | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| `customer_id` | String | Identificador do cliente | `CUST_00042` |
| `product_id` | String | Identificador do produto | `PROD_0123` |
| `quantity` | Integer | Quantidade comprada (1-20) | `3` |
| `unit_price` | Double | Preço unitário (R$ 10-5000) | `149.90` |
| `total_amount` | Double | Valor total (quantity × unit_price) | `449.70` |
| `order_date` | Timestamp | Data do pedido (ano 2023) | `2023-07-15 14:30:00` |
| `payment_method` | String | Forma de pagamento | `pix` |
| `shipping_city` | String | Cidade de entrega | `São Paulo` |
| `shipping_state` | String | UF de entrega (2 letras) | `SP` |
| `status` | String | Status do pedido | `delivered` |
| `partner_source` | String | Fonte/parceiro de origem | `parceiro_a` |

### Valores Válidos

- **payment_method**: `credit_card`, `debit_card`, `pix`, `boleto`
- **status**: `pending`, `shipped`, `delivered`, `cancelled`
- **partner_source**: `parceiro_a`, `parceiro_b`, `parceiro_c`
- **shipping_state**: Todos os 27 estados brasileiros (AC, AL, ..., TO)

### Regras de Validação

- `quantity > 0`
- `unit_price >= 0`
- `total_amount == quantity × unit_price`
- `order_date` não pode ser data futura
- `status` deve ser um dos valores válidos

## Contexto Narrativo

Os datasets acompanham a evolução da **DataFlow Analytics**:

1. **Aula 1** — A empresa recebe seus primeiros dados de vendas (100K registros) e precisa processá-los com Spark
2. **Aula 2** — O volume cresce 10x (1M registros) e surgem dados de clientes e categorias para cruzar
3. **Aula 3** — Três novos parceiros comerciais enviam dados em formatos diferentes (CSV legado, JSON API, Parquet)
4. **Aula 4** — Os dados passam a chegar diariamente e precisam ser processados de forma automatizada
5. **Aula 6** — Dados com problemas de qualidade são descobertos e precisam ser tratados
6. **Aula 7** — Pipeline completo de produção com todos os componentes integrados

## Seed e Reprodutibilidade

- **Seed padrão**: `42`
- **Locale Faker**: `pt_BR` (nomes, cidades e telefones brasileiros)
- A execução com mesma seed sempre produz os mesmos dados
- Alterações no script podem invalidar reprodutibilidade de versões anteriores
