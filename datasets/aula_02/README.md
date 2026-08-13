# 📊 Datasets - Aula 02: Transformações Avançadas com Spark

## Contexto Narrativo

A **DataFlow Analytics** cresceu 10x em 6 meses. Ana (Product Owner) precisa de relatórios que cruzam dados de vendas com clientes e categorias de produtos. Carlos (Engenheiro Sênior) mostra que é preciso dominar joins, window functions e otimizações para lidar com o volume crescente.

## Arquivos

### vendas_2023_completo.parquet

Dataset ampliado de vendas com 1 milhão de registros em formato Parquet otimizado.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Registros** | ~1.000.000 |
| **Tamanho** | ~50 MB |
| **Compressão** | Snappy (padrão PyArrow) |
| **Período** | Janeiro a Dezembro de 2023 |
| **Seed** | 43 |

#### Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `order_id` | String (UUID) | Não | Identificador único do pedido | `b2c3d4e5-f6a7-8901-bcde-f12345678901` |
| `customer_id` | String | Não | ID do cliente (CUST_NNNNN) | `CUST_12345` |
| `product_id` | String | Não | ID do produto (PROD_NNNN) | `PROD_0456` |
| `quantity` | Integer | Não | Quantidade (1 a 20) | `5` |
| `unit_price` | Double | Não | Preço unitário em R$ | `89.90` |
| `total_amount` | Double | Não | Valor total calculado | `449.50` |
| `order_date` | Timestamp | Não | Data/hora do pedido | `2023-09-22 10:15:00` |
| `payment_method` | String | Sim | Forma de pagamento | `credit_card` |
| `shipping_city` | String | Sim | Cidade de entrega | `Belo Horizonte` |
| `shipping_state` | String | Sim | UF de entrega | `MG` |
| `status` | String | Não | Status do pedido | `shipped` |
| `partner_source` | String | Sim | Parceiro de origem | `parceiro_b` |

---

### clientes.parquet

Base completa de clientes da DataFlow Analytics.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Registros** | 500.000 |
| **Tamanho** | ~25 MB |
| **Compressão** | Snappy |
| **Seed** | 44 |

#### Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `customer_id` | String | Não | ID do cliente (chave primária) | `CUST_00001` |
| `customer_name` | String | Não | Nome completo (Faker pt_BR) | `João da Silva` |
| `email` | String | Não | E-mail do cliente | `joao.silva@email.com` |
| `phone` | String | Não | Telefone (formato brasileiro) | `(11) 98765-4321` |
| `city` | String | Não | Cidade do cliente | `Campinas` |
| `state` | String | Não | UF do cliente (2 letras) | `SP` |
| `registration_date` | Timestamp | Não | Data de cadastro (2020-2023) | `2021-03-15` |
| `segment` | String | Não | Segmento de fidelidade | `Ouro` |

**Segmentos**: Bronze (50%), Prata (30%), Ouro (15%), Platina (5%)

---

### categorias.json

Hierarquia de categorias e subcategorias de produtos.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | JSON (UTF-8, indentado) |
| **Categorias** | 10 |
| **Subcategorias** | 50 (5 por categoria) |
| **Tamanho** | ~3 KB |

#### Estrutura

```json
{
  "categorias": [
    {
      "category_id": "CAT_01",
      "category_name": "Eletrônicos",
      "subcategories": [
        {
          "subcategory_id": "SUBCAT_001",
          "subcategory_name": "Smartphones"
        }
      ]
    }
  ],
  "generated_at": "2024-01-01T00:00:00"
}
```

## Relações entre Datasets

```
vendas_2023_completo.parquet
    ├── customer_id → clientes.parquet (customer_id)
    └── product_id  → (usa categorias.json para lookup de categoria)
```

- **Join vendas ↔ clientes**: via coluna `customer_id`
- **Lookup de categorias**: via `product_id` → categoria correspondente

## Como Regenerar

```bash
python gerar_datasets.py --aula 2
```

## Uso no Lab

Estes dados são utilizados nos exercícios da Aula 2 para:
- Realizar joins entre vendas, clientes e categorias (inner, left, right, full, cross, semi, anti)
- Aplicar window functions: `row_number`, `rank`, `dense_rank`, `lag`, `lead`
- Criar ranking de clientes por faturamento em cada estado
- Analisar tendências de compra com `lag` (comparação com compra anterior)
- Implementar UDFs para classificação de ticket
- Otimizar queries com `explain()`, broadcast join e cache
