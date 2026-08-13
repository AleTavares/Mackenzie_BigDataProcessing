# 📊 Datasets - Aula 03: Ingestão e Persistência de Dados

## Contexto Narrativo

A **DataFlow Analytics** fechou parcerias com 3 novos fornecedores de dados. Cada parceiro envia dados em formato diferente: CSV legado de um sistema antigo, JSON dumps de uma API REST, e Parquet de um data lake moderno. Marina (CTO) precisa de um processo robusto de ingestão que normalize tudo em um formato unificado no data lake da empresa, implementando a arquitetura medallion (Bronze → Silver → Gold).

## Estrutura de Diretórios

```
aula_03/
├── parceiro_a/                    # CSV legado (sistema antigo)
│   ├── vendas_legacy_01_2023.csv
│   ├── vendas_legacy_06_2023.csv
│   └── vendas_legacy_12_2023.csv
├── parceiro_b/                    # JSON API dumps (paginado)
│   ├── api_dump_page_001.json
│   ├── api_dump_page_002.json
│   └── api_dump_page_003.json
└── parceiro_c/                    # Parquet otimizado
    └── vendas_parceiro_c.parquet
```

## Arquivos

### parceiro_a/ — CSV Legado

Dados exportados de um sistema ERP antigo com encoding Latin-1 e separador ponto-e-vírgula. Simula um parceiro que não modernizou sua stack.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | CSV (ISO-8859-1, separador `;`) |
| **Arquivos** | 3 (exports mensais: janeiro, junho, dezembro) |
| **Registros totais** | ~50.000 (distribuídos entre os 3 meses) |
| **Tamanho total** | ~8 MB |
| **Encoding** | ISO-8859-1 (Latin-1) |
| **Separador** | `;` (ponto-e-vírgula) |
| **Seed** | 52 |

#### Schema (colunas em português)

| Coluna | Tipo | Descrição | Equivalente Schema Principal |
|--------|------|-----------|------------------------------|
| `cod_pedido` | String (UUID) | Código do pedido | `order_id` |
| `cod_cliente` | String | Código do cliente | `customer_id` |
| `cod_produto` | String | Código do produto | `product_id` |
| `qtd` | Integer | Quantidade | `quantity` |
| `preco_unit` | Double | Preço unitário (R$) | `unit_price` |
| `valor_total` | Double | Valor total | `total_amount` |
| `data_pedido` | Timestamp | Data do pedido | `order_date` |
| `forma_pagamento` | String | Forma de pagamento | `payment_method` |
| `cidade_entrega` | String | Cidade de entrega | `shipping_city` |
| `uf_entrega` | String | UF de entrega | `shipping_state` |
| `situacao` | String | Situação do pedido | `status` |
| `origem` | String | Origem do dado | `partner_source` |

**⚠️ Atenção**: Os nomes das colunas estão em português e o encoding pode causar problemas se não tratado corretamente.

#### Leitura em PySpark

```python
df_parceiro_a = spark.read.csv(
    "data/parceiro_a/*.csv",
    header=True,
    sep=";",
    encoding="ISO-8859-1",
    inferSchema=True
)
```

---

### parceiro_b/ — JSON API Dumps

Dados exportados de uma API REST, com paginação simulada e metadados de exportação. Representa um parceiro com sistema moderno baseado em microserviços.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | JSON (UTF-8, multiLine) |
| **Arquivos** | 3 (páginas de 10.000 registros cada) |
| **Registros totais** | 30.000 |
| **Tamanho total** | ~12 MB |
| **Seed** | 62 |

#### Estrutura do JSON

```json
{
  "api_version": "2.1",
  "exported_at": "2023-12-15T10:30:00Z",
  "page": 1,
  "total_pages": 3,
  "data": [
    {
      "order_id": "uuid-here",
      "customer_id": "CUST_12345",
      "product_id": "PROD_0678",
      "quantity": 2,
      "unit_price": 199.90,
      "total_amount": 399.80,
      "order_date": "2023-08-10",
      "payment_method": "pix",
      "shipping_city": "Rio de Janeiro",
      "shipping_state": "RJ",
      "status": "delivered",
      "partner_source": "parceiro_b"
    }
  ]
}
```

#### Schema dos Registros (dentro de `data[]`)

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `order_id` | String (UUID) | Identificador do pedido | `c3d4e5f6-...` |
| `customer_id` | String | ID do cliente | `CUST_23456` |
| `product_id` | String | ID do produto | `PROD_0789` |
| `quantity` | Integer | Quantidade | `4` |
| `unit_price` | Double | Preço unitário | `59.90` |
| `total_amount` | Double | Valor total | `239.60` |
| `order_date` | String | Data do pedido (formato ISO) | `2023-08-10` |
| `payment_method` | String | Forma de pagamento | `credit_card` |
| `shipping_city` | String | Cidade de entrega | `Curitiba` |
| `shipping_state` | String | UF de entrega | `PR` |
| `status` | String | Status do pedido | `shipped` |
| `partner_source` | String | Parceiro de origem | `parceiro_b` |

#### Leitura em PySpark

```python
df_parceiro_b = spark.read.json(
    "data/parceiro_b/*.json",
    multiLine=True
)
# Acessar dados: df_parceiro_b.select("data.*")
```

---

### parceiro_c/ — Parquet Otimizado

Dados de um data lake moderno já otimizado com compressão Snappy. Representa um parceiro tecnicamente avançado.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Arquivos** | 1 (`vendas_parceiro_c.parquet`) |
| **Registros** | 80.000 |
| **Compressão** | Snappy |
| **Tamanho** | ~5 MB |
| **Seed** | 72 |

#### Schema

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-----------|---------|
| `order_id` | String (UUID) | Identificador do pedido | `d4e5f6a7-...` |
| `customer_id` | String | ID do cliente | `CUST_34567` |
| `product_id` | String | ID do produto | `PROD_0234` |
| `quantity` | Integer | Quantidade | `1` |
| `unit_price` | Double | Preço unitário | `1299.00` |
| `total_amount` | Double | Valor total | `1299.00` |
| `order_date` | Timestamp | Data do pedido | `2023-05-20 09:45:00` |
| `payment_method` | String | Forma de pagamento | `debit_card` |
| `shipping_city` | String | Cidade de entrega | `Porto Alegre` |
| `shipping_state` | String | UF de entrega | `RS` |
| `status` | String | Status do pedido | `delivered` |
| `partner_source` | String | Parceiro de origem | `parceiro_c` |

#### Leitura em PySpark

```python
df_parceiro_c = spark.read.parquet("data/parceiro_c/")
```

## Mapeamento de Colunas para Normalização

Para unificar os 3 parceiros na camada Silver, o aluno deve mapear:

| Schema Unificado | Parceiro A | Parceiro B | Parceiro C |
|------------------|-----------|-----------|-----------|
| `order_id` | `cod_pedido` | `order_id` | `order_id` |
| `customer_id` | `cod_cliente` | `customer_id` | `customer_id` |
| `product_id` | `cod_produto` | `product_id` | `product_id` |
| `quantity` | `qtd` | `quantity` | `quantity` |
| `unit_price` | `preco_unit` | `unit_price` | `unit_price` |
| `total_amount` | `valor_total` | `total_amount` | `total_amount` |
| `order_date` | `data_pedido` | `order_date` | `order_date` |
| `payment_method` | `forma_pagamento` | `payment_method` | `payment_method` |
| `shipping_city` | `cidade_entrega` | `shipping_city` | `shipping_city` |
| `shipping_state` | `uf_entrega` | `shipping_state` | `shipping_state` |
| `status` | `situacao` | `status` | `status` |
| `partner_source` | `origem` | `partner_source` | `partner_source` |

## Como Regenerar

```bash
python gerar_datasets.py --aula 3
```

## Uso no Lab

Estes dados são utilizados nos exercícios da Aula 3 para:
- Ler CSV com encoding especial e separadores customizados
- Ler JSON multi-arquivo com metadados de API
- Ler Parquet otimizado
- Implementar camada Bronze (ingestão raw sem transformação)
- Normalizar schemas para camada Silver (renomear colunas, unificar tipos)
- Persistir com particionamento por data
- Implementar camada Gold com agregações de negócio
