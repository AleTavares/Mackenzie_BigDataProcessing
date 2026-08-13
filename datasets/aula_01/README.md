# 📊 Datasets - Aula 01: Fundamentos de Big Data e Apache Spark

## Contexto Narrativo

A **DataFlow Analytics** acaba de fechar contrato com seu primeiro grande cliente de e-commerce. Marina (CTO) percebe que os scripts Python com pandas não aguentam mais o volume de dados de vendas (100K+ registros) e decide adotar Apache Spark para processamento distribuído.

## Arquivos

### vendas_2023.csv

Dataset principal de vendas da DataFlow Analytics referente ao ano de 2023.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | CSV (UTF-8, separador `,`) |
| **Registros** | ~100.000 |
| **Tamanho** | ~15 MB |
| **Período** | Janeiro a Dezembro de 2023 |
| **Seed** | 42 |

#### Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `order_id` | String (UUID) | Não | Identificador único do pedido | `a1b2c3d4-e5f6-7890-abcd-ef1234567890` |
| `customer_id` | String | Não | ID do cliente (formato CUST_NNNNN) | `CUST_00042` |
| `product_id` | String | Não | ID do produto (formato PROD_NNNN) | `PROD_0123` |
| `quantity` | Integer | Não | Quantidade comprada (1 a 20) | `3` |
| `unit_price` | Double | Não | Preço unitário em R$ (10.00 a 5000.00) | `149.90` |
| `total_amount` | Double | Não | Valor total = quantity × unit_price | `449.70` |
| `order_date` | Timestamp | Não | Data/hora do pedido | `2023-07-15 14:30:00` |
| `payment_method` | String | Sim | Forma de pagamento | `pix` |
| `shipping_city` | String | Sim | Cidade de entrega | `São Paulo` |
| `shipping_state` | String | Sim | UF de entrega (2 letras) | `SP` |
| `status` | String | Não | Status do pedido | `delivered` |
| `partner_source` | String | Sim | Parceiro de origem dos dados | `parceiro_a` |

### produtos.csv

Catálogo de produtos vendidos pela DataFlow Analytics.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | CSV (UTF-8, separador `,`) |
| **Registros** | 5.000 |
| **Tamanho** | ~400 KB |
| **Seed** | 42 |

#### Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `product_id` | String | Não | ID do produto (PROD_0001 a PROD_5000) | `PROD_0042` |
| `product_name` | String | Não | Nome do produto (gerado via Faker) | `Solução Digital Avançada` |
| `category` | String | Não | Categoria principal | `Eletrônicos` |
| `subcategory` | String | Não | Subcategoria | `Smartphones` |
| `unit_price` | Double | Não | Preço base em R$ (10.00 a 5000.00) | `299.90` |
| `weight_kg` | Double | Não | Peso em kg (0.1 a 30.0) | `1.25` |
| `is_active` | Boolean | Não | Produto ativo no catálogo (85% True) | `True` |

## Categorias Disponíveis

- Eletrônicos, Moda, Casa e Decoração, Esportes, Livros
- Saúde e Beleza, Alimentos, Brinquedos, Automotivo, Informática

## Distribuições Importantes

- **Pagamentos**: credit_card (35%), pix (30%), debit_card (20%), boleto (15%)
- **Status**: delivered (65%), shipped (15%), pending (10%), cancelled (10%)
- **Estados**: SP (~19%), RJ (~12%), MG (~9%), PR (~6%), RS (~6%), outros
- **Datas**: Distribuição triangular com mais vendas no segundo semestre

## Como Regenerar

```bash
python gerar_datasets.py --aula 1
```

## Uso no Lab

Estes dados são utilizados nos exercícios da Aula 1 para:
- Criar SparkSession e ler CSV
- Explorar schema com `printSchema()` e `show()`
- Realizar agregações com `groupBy`, `sum`, `avg`, `count`
- Filtrar e ordenar dados com `filter` e `orderBy`
- Comparar performance pandas vs Spark
