# 📊 Datasets - Aula 04: Introdução ao Apache Airflow

## Contexto Narrativo

Carlos (Engenheiro Sênior) está cansado de rodar pipelines manualmente todo dia às 6h da manhã. A **DataFlow Analytics** precisa automatizar seus processos de dados. Marina (CTO) decide implantar o Apache Airflow como orquestrador. Carlos precisa transformar seus scripts em DAGs automatizadas que processam vendas diárias.

## Estrutura de Diretórios

```
aula_04/
└── vendas_diarias/
    ├── date=2023-11-01/
    │   └── data.csv
    ├── date=2023-11-02/
    │   └── data.csv
    ├── date=2023-11-03/
    │   └── data.csv
    │   ...
    └── date=2023-11-30/
        └── data.csv
```

## Arquivos

### vendas_diarias/ — Particionamento por Data

Dataset de vendas particionado por data, simulando a chegada diária de dados para processamento automatizado via Airflow.

| Propriedade | Valor |
|-------------|-------|
| **Formato** | CSV (UTF-8, separador `,`) |
| **Partições** | 30 (um diretório por dia) |
| **Período** | 1 a 30 de Novembro de 2023 |
| **Registros totais** | ~30.000 |
| **Registros por dia** | ~1.000 (média) |
| **Tamanho total** | ~5 MB |
| **Padrão de partição** | `date=YYYY-MM-DD/data.csv` |
| **Seed** | 82 |

#### Schema (cada data.csv)

| Coluna | Tipo | Nullable | Descrição | Exemplo |
|--------|------|----------|-----------|---------|
| `order_id` | String (UUID) | Não | Identificador único do pedido | `e5f6a7b8-...` |
| `customer_id` | String | Não | ID do cliente | `CUST_04567` |
| `product_id` | String | Não | ID do produto | `PROD_0890` |
| `quantity` | Integer | Não | Quantidade (1 a 20) | `2` |
| `unit_price` | Double | Não | Preço unitário em R$ | `79.90` |
| `total_amount` | Double | Não | Valor total | `159.80` |
| `order_date` | Timestamp | Não | Data do pedido (corresponde à partição) | `2023-11-15` |
| `payment_method` | String | Sim | Forma de pagamento | `boleto` |
| `shipping_city` | String | Sim | Cidade de entrega | `Salvador` |
| `shipping_state` | String | Sim | UF de entrega | `BA` |
| `status` | String | Não | Status do pedido | `pending` |
| `partner_source` | String | Sim | Parceiro de origem | `parceiro_a` |

## Padrão de Particionamento

O dataset usa o padrão Hive de particionamento (`coluna=valor/`), que é nativamente reconhecido pelo Spark:

```python
# Leitura automática de partições pelo Spark
df = spark.read.csv("data/vendas_diarias/", header=True, inferSchema=True)

# Leitura de uma partição específica (um dia)
df_dia = spark.read.csv(
    "data/vendas_diarias/date=2023-11-15/data.csv",
    header=True,
    inferSchema=True
)
```

## Simulação de Processamento Diário

Este dataset simula o cenário onde:
1. Todo dia, novos dados de vendas chegam em uma pasta particionada
2. A DAG do Airflow é executada diariamente às 6h
3. O pipeline processa apenas os dados do dia anterior (`{{ ds }}`)
4. Cada partição é independente e pode ser reprocessada

## Como Regenerar

```bash
python gerar_datasets.py --aula 4
```

## Uso no Lab

Estes dados são utilizados nos exercícios da Aula 4 para:
- Criar primeira DAG com PythonOperator (ETL extrair → transformar → carregar)
- Implementar dependências entre tasks
- Usar XComs para passar contagem de registros entre tasks
- Usar template variables do Airflow (`{{ ds }}`, `{{ ds_nodash }}`) para acessar partições por data
- Configurar `schedule_interval` para execução diária
- Implementar retry e error handling básico
- Criar BashOperator para notificações
