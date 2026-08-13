# 📊 Datasets - Aula 06: Qualidade de Dados e Monitoramento

## Contexto Narrativo

A **DataFlow Analytics** cresceu tanto que agora processa dados de 50+ clientes. Roberto (CEO) está preocupado com compliance (LGPD) e Ana (Product Owner) reportou que relatórios estão saindo com números inconsistentes. Marina (CTO) implementa um programa de qualidade de dados com validações automáticas, quarentena de registros inválidos e monitoramento contínuo.

## Estrutura de Diretórios

```
aula_06/
└── dados_sujos/
    ├── vendas_problemas.csv          # Dataset com problemas intencionais
    ├── vendas_problemas.parquet      # Mesmo dataset em Parquet
    └── vendas_referencia.parquet     # Dataset limpo para comparação
```

## Arquivos

### dados_sujos/vendas_problemas.csv

Dataset de vendas com **problemas de qualidade propositais** para exercícios de detecção e tratamento.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | CSV (UTF-8, separador `,`) |
| **Registros** | ~51.500 (50K originais + ~3% duplicatas) |
| **Tamanho** | ~8 MB |
| **Seed base** | 102 (geração) / 103 (problemas) |

### dados_sujos/vendas_problemas.parquet

Mesmo dataset com problemas, em formato Parquet (para exercícios de leitura otimizada).

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Registros** | ~51.500 |
| **Compressão** | Snappy |
| **Tamanho** | ~3 MB |

### dados_sujos/vendas_referencia.parquet

Dataset **limpo** de referência (sem problemas de qualidade). Usado para comparação e validação dos resultados de limpeza.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | Apache Parquet |
| **Registros** | 50.000 |
| **Compressão** | Snappy |
| **Tamanho** | ~3 MB |
| **Qualidade** | 100% limpo (gerado antes da introdução de problemas) |

## Schema

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `order_id` | String (UUID) | Não | Identificador do pedido | `f6a7b8c9-...` |
| `customer_id` | String | **Sim*** | ID do cliente | `CUST_05678` |
| `product_id` | String | Não | ID do produto | `PROD_0345` |
| `quantity` | Integer | Não | Quantidade | `3` |
| `unit_price` | Double | Não | Preço unitário | `199.90` |
| `total_amount` | Double | Não | Valor total | `599.70` |
| `order_date` | Timestamp | Não | Data do pedido | `2023-04-10` |
| `payment_method` | String | **Sim*** | Forma de pagamento | `pix` |
| `shipping_city` | String | **Sim*** | Cidade de entrega | `Fortaleza` |
| `shipping_state` | String | **Sim*** | UF de entrega | `CE` |
| `status` | String | Não | Status do pedido | `delivered` |
| `partner_source` | String | Sim | Parceiro de origem | `parceiro_c` |

*\* Colunas com nulls introduzidos intencionalmente (~5%)*

## Problemas de Qualidade Introduzidos

O dataset `vendas_problemas.csv` contém os seguintes problemas **intencionais**:

| # | Problema | Percentual | Colunas Afetadas | Descrição |
|---|----------|------------|------------------|-----------|
| 1 | **Valores nulos** | ~5% | `shipping_city`, `shipping_state`, `payment_method`, `customer_id` | Nulls aleatórios em campos que deveriam ser obrigatórios |
| 2 | **Duplicatas** | ~3% | Todas (linhas inteiras) | ~1.500 registros duplicados adicionados ao final |
| 3 | **Valores negativos** | ~1% | `total_amount` | Valores de venda com sinal trocado (negativos) |
| 4 | **Inconsistência calculada** | ~2% | `total_amount` | `total_amount ≠ quantity × unit_price` |
| 5 | **Status inválidos** | ~0.5% | `status` | Valores como `"INVALIDO"`, `"erro"`, `"NULL"`, `""` |
| 6 | **Datas futuras** | ~0.8% | `order_date` | Datas em 2025 (impossíveis para dados de 2023) |

## Expectativas de Qualidade

Para cada dimensão, o aluno deve implementar checks e comparar com a referência:

| Dimensão | Referência (limpo) | Problemático | Meta após limpeza |
|----------|--------------------|--------------|--------------------|
| Completude | 100% | ~95% | > 99% |
| Unicidade | 100% | ~97% | 100% |
| Validade (status) | 100% | ~99.5% | 100% |
| Consistência (cálculo) | 100% | ~98% | 100% |
| Atualidade (datas) | 100% | ~99.2% | 100% |

## Como Regenerar

```bash
python gerar_datasets.py --aula 6
```

## Uso no Lab

Estes dados são utilizados nos exercícios da Aula 6 para:
- Implementar check de completude (detectar nulls/NaN) com PySpark
- Implementar check de unicidade (detectar duplicatas)
- Implementar check de integridade referencial (left_anti join)
- Verificar consistência calculada (`total_amount == quantity × unit_price`)
- Validar domínio de valores (status válidos, datas no passado)
- Implementar sistema de quarentena (separar válidos de inválidos)
- Criar métricas de qualidade e dashboards
- Integrar checks em DAG Airflow com alertas automáticos
- Comparar resultado da limpeza com `vendas_referencia.parquet`
