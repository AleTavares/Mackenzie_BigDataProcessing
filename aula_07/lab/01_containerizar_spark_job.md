# Exercício 1 — Containerizar Spark Job como Script Python com CLI

## Duração Estimada

⏱️ ~20 minutos

## Contexto

> **Marina Silva (CTO):** "Carlos, em duas semanas temos a apresentação para os investidores. O Roberto quer ver o pipeline rodando sozinho, sem ninguém tocando. Não podemos depender de notebooks — precisamos de um script que aceite parâmetros, rode via `spark-submit` e funcione igual em qualquer máquina."

> **Carlos Mendes (Engenheiro de Dados Sênior):** "Entendido, Marina. O primeiro passo é transformar nosso notebook em um script Python standalone. Vamos usar `argparse` para receber a data de referência e os caminhos via linha de comando. Assim o Airflow pode chamar o script com os parâmetros corretos para cada dia."

> **Marina Silva (CTO):** "Exatamente. E precisa ser idempotente — se falhar no meio e rodarmos de novo, o resultado tem que ser o mesmo. Nada de duplicar dados."

## Objetivos

Ao final deste exercício, você será capaz de:

- Entender por que scripts CLI são superiores a notebooks em produção
- Criar um script PySpark com `argparse` para receber parâmetros via CLI
- Implementar pipeline Bronze → Silver → Gold em um script standalone
- Adicionar tratamento de erros com `try/except` e `sys.exit(1)`
- Testar o script localmente com `python pipeline_vendas.py`
- Testar via `spark-submit` no container Docker
- Verificar idempotência executando o script múltiplas vezes

## Pré-requisitos

- Ambiente Docker rodando (Spark + Jupyter)
- Aulas 1-6 concluídas (PySpark, Medallion, Airflow, Quality Checks)
- Dataset `datasets/aula_07/producao/` disponível com dados de 7 dias
- Terminal com acesso ao container Spark

## O que vamos construir?

Um script Python de produção que implementa o pipeline completo:

```
┌─────────────────────────────────────────────────────────────────────┐
│  pipeline_vendas.py                                                  │
│                                                                      │
│  Argumentos CLI:                                                     │
│    --data-ref 2023-12-01        (obrigatório: data a processar)     │
│    --input-path /data/producao  (opcional: onde estão os dados)     │
│    --output-path /data/datalake (opcional: onde salvar)             │
│    --log-level INFO             (opcional: verbosidade dos logs)    │
│                                                                      │
│  Pipeline:                                                           │
│    incoming/2023-12-01/vendas.parquet                                │
│         ↓ [Bronze: ingestão + metadados]                            │
│    datalake/bronze/vendas/_data_ref=2023-12-01/                     │
│         ↓ [Silver: limpeza + dedup + validação]                     │
│    datalake/silver/vendas/data_ref=2023-12-01/                      │
│         ↓ [Gold: join produtos + agregações]                        │
│    datalake/gold/metricas_vendas/data_ref=2023-12-01/               │
│                                                                      │
│  Propriedades:                                                       │
│    ✅ Idempotente (overwrite por partição)                          │
│    ✅ Parametrizado (CLI args)                                      │
│    ✅ Tratamento de erros (sys.exit(1))                             │
│    ✅ Submissível via spark-submit                                  │
└─────────────────────────────────────────────────────────────────────┘
```

**Por que scripts CLI em vez de notebooks?**

| Aspecto | Notebook | Script CLI |
|---------|----------|-----------|
| Execução | Manual (clicar "Run All") | Automática (`spark-submit`) |
| Parâmetros | Hardcoded nas células | Via `--data-ref`, `--input-path` |
| Orquestração | Não integrável com Airflow | SparkSubmitOperator direto |
| Reprodutibilidade | Depende da ordem de execução | Sempre mesma sequência |
| CI/CD | Não testável em pipeline | `python -m pytest` + linting |
| Debugging | Difícil em produção | Logs estruturados + stack trace |
| Reprocessamento | Editar célula e rodar de novo | `--data-ref 2023-12-01` (backfill) |

> **Carlos:** "Notebooks são ótimos para exploração e prototipagem. Mas em produção, precisamos de algo que rode sem intervenção humana, aceite parâmetros do orquestrador, e falhe de forma controlada."

---

## Passo 1: Entender a Estrutura do Script

**Descrição:** Antes de escrever código, vamos entender a anatomia de um script PySpark de produção. Um bom script tem 5 seções bem definidas:

```python
# Anatomia de um script PySpark de produção:

# 1. IMPORTS E CONFIGURAÇÃO
#    - Bibliotecas necessárias
#    - Configuração de logging

# 2. PARSING DE ARGUMENTOS (argparse)
#    - Definir --data-ref, --input-path, --output-path, --log-level
#    - Validação dos argumentos

# 3. FUNÇÕES DO PIPELINE
#    - etapa_bronze(): ingestão raw
#    - etapa_silver(): limpeza e validação
#    - etapa_gold(): agregações de negócio

# 4. FUNÇÃO MAIN
#    - Orquestra as etapas em sequência
#    - try/except para tratamento de erros
#    - sys.exit(1) em caso de falha

# 5. ENTRYPOINT
#    - if __name__ == "__main__": main()
```

**Conceitos-chave:**

| Conceito | O que é | Por que importa |
|----------|---------|-----------------|
| `argparse` | Biblioteca padrão Python para CLI | Permite parametrizar sem editar código |
| `sys.exit(1)` | Código de saída não-zero | Airflow detecta falha e pode fazer retry |
| `if __name__ == "__main__"` | Guard de execução | Permite importar funções sem executar o pipeline |
| `partitionOverwriteMode=dynamic` | Config do Spark | Garante idempotência no overwrite |

---

## Passo 2: Criar o Diretório para Spark Jobs

**Descrição:** Vamos criar a estrutura de diretórios para nossos scripts de produção. A convenção é separar os jobs Spark em uma pasta dedicada.

**Comando:**

```bash
mkdir -p aula_07/code/spark_jobs
```

**Resultado esperado:**
```
(nenhuma saída — diretório criado)
```

**Estrutura resultante:**
```
aula_07/
├── code/
│   └── spark_jobs/          ← nossos scripts de produção
│       └── pipeline_vendas.py
├── lab/
│   └── 01_containerizar_spark_job.md  ← este exercício
├── slides/
└── data/
```

---

## Passo 3: Criar o Script — Imports e Configuração de Logging

**Descrição:** Começamos com os imports necessários e uma função para configurar logging. Em produção, logs bem formatados são essenciais para debugging.

**Comando:** Crie o arquivo `aula_07/code/spark_jobs/pipeline_vendas.py`:

```bash
cat > aula_07/code/spark_jobs/pipeline_vendas.py << 'EOF'
"""
Pipeline de Vendas — DataFlow Analytics
=========================================
Script de produção para processamento de vendas diárias.
Implementa pipeline Bronze → Silver → Gold com escrita idempotente.

Uso:
    python pipeline_vendas.py --data-ref 2024-01-15
    python pipeline_vendas.py --data-ref 2024-01-15 --input-path /data/incoming --output-path /datalake

Via spark-submit:
    spark-submit --master spark://spark-master:7077 pipeline_vendas.py --data-ref 2024-01-15

Autor: Carlos Mendes (Engenheiro de Dados Sênior - DataFlow Analytics)
Versão: 1.0.0
"""

import argparse
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as spark_sum,
    count, avg, when, isnan, isnull
)


# ============================================================
# 1. CONFIGURAÇÃO DE LOGGING
# ============================================================
def configurar_logging(log_level: str) -> logging.Logger:
    """Configura logging com formato padronizado para produção."""
    logger = logging.getLogger("pipeline_vendas")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Formato com timestamp, nível e mensagem
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
EOF
```

**O que cada import faz:**

| Import | Propósito |
|--------|-----------|
| `argparse` | Parsing de argumentos CLI (--data-ref, etc.) |
| `sys` | `sys.exit(1)` para sinalizar falha ao orquestrador |
| `logging` | Logs estruturados com timestamp e nível |
| `datetime` | Validar formato da data de referência |
| `pyspark.sql` | SparkSession e funções de transformação |

> **Carlos:** "O `logging` do Python é muito mais poderoso que `print()`. Ele tem níveis (DEBUG, INFO, WARNING, ERROR), timestamps automáticos e pode ser direcionado para arquivos ou sistemas de monitoramento."

---

## Passo 4: Adicionar o Parsing de Argumentos CLI

**Descrição:** O `argparse` é a biblioteca padrão do Python para receber parâmetros via linha de comando. Definimos argumentos obrigatórios e opcionais, com validação automática.

**Adicione ao arquivo** (continuação):

```python
# ============================================================
# 2. PARSING DE ARGUMENTOS CLI
# ============================================================
def parse_args() -> argparse.Namespace:
    """
    Parseia argumentos de linha de comando.

    Argumentos obrigatórios:
        --data-ref: Data de referência para processamento (YYYY-MM-DD)

    Argumentos opcionais:
        --input-path: Diretório base dos dados de entrada
        --output-path: Diretório base para saída (datalake)
        --log-level: Nível de log (DEBUG, INFO, WARNING, ERROR)
    """
    parser = argparse.ArgumentParser(
        description="Pipeline de vendas DataFlow Analytics — Bronze → Silver → Gold",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python pipeline_vendas.py --data-ref 2023-12-01
  python pipeline_vendas.py --data-ref 2023-12-01 --input-path /data/producao
  spark-submit --master spark://spark-master:7077 pipeline_vendas.py --data-ref 2023-12-01
        """
    )

    parser.add_argument(
        "--data-ref",
        type=str,
        required=True,
        help="Data de referência para processamento (formato: YYYY-MM-DD)"
    )

    parser.add_argument(
        "--input-path",
        type=str,
        default="data/aula_07/producao",
        help="Diretório base dos dados de entrada (default: data/aula_07/producao)"
    )

    parser.add_argument(
        "--output-path",
        type=str,
        default="data/aula_07/datalake",
        help="Diretório base para saída do datalake (default: data/aula_07/datalake)"
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de logging (default: INFO)"
    )

    args = parser.parse_args()

    # Validação do formato da data
    try:
        datetime.strptime(args.data_ref, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--data-ref deve estar no formato YYYY-MM-DD. Recebido: '{args.data_ref}'")

    return args
```

**Como o argparse funciona:**

```
$ python pipeline_vendas.py --data-ref 2023-12-01 --log-level DEBUG

argparse mapeia para:
  args.data_ref   = "2023-12-01"
  args.input_path = "data/aula_07/producao"   (default)
  args.output_path = "data/aula_07/datalake"  (default)
  args.log_level  = "DEBUG"

$ python pipeline_vendas.py
  → ERRO: --data-ref is required

$ python pipeline_vendas.py --data-ref "abc"
  → ERRO: --data-ref deve estar no formato YYYY-MM-DD
```

**Vantagens do argparse:**

| Característica | Benefício |
|----------------|-----------|
| `required=True` | Falha com mensagem clara se esquecer |
| `default=` | Valores padrão sensatos |
| `choices=` | Restringe a valores válidos |
| `help=` | Gera `--help` automaticamente |
| `parser.error()` | Mensagem de erro customizada + exit(2) |

---

## Passo 5: Implementar a Etapa Bronze (Ingestão)

**Descrição:** A camada Bronze ingere dados brutos e adiciona metadados de rastreabilidade. Não faz transformações — mantém fidelidade total ao dado original.

**Adicione ao arquivo:**

```python
# ============================================================
# 3. FUNÇÕES DO PIPELINE
# ============================================================
def criar_spark_session(data_ref: str) -> SparkSession:
    """Cria SparkSession configurada para produção."""
    spark = SparkSession.builder \
        .appName(f"DataFlow-PipelineVendas-{data_ref}") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    # Reduz verbosidade dos logs do Spark
    spark.sparkContext.setLogLevel("WARN")

    return spark


def etapa_bronze(spark: SparkSession, input_path: str, output_path: str,
                 data_ref: str, logger: logging.Logger) -> int:
    """
    Camada Bronze: Ingestão raw com metadados de rastreabilidade.

    - Lê dados brutos do dia (incoming/YYYY-MM-DD/vendas.parquet)
    - Adiciona metadados: data_ref, timestamp de ingestão, source
    - Persiste sem transformação (fidelidade total ao dado original)

    Returns:
        Número de registros ingeridos
    """
    logger.info(f"[BRONZE] Iniciando ingestão para data_ref={data_ref}")

    # Ler dados brutos do dia
    caminho_entrada = f"{input_path}/incoming/{data_ref}/vendas.parquet"
    logger.info(f"[BRONZE] Lendo: {caminho_entrada}")

    df_raw = spark.read.parquet(caminho_entrada)
    contagem = df_raw.count()
    logger.info(f"[BRONZE] Registros lidos: {contagem:,}")

    # Adicionar metadados de rastreabilidade
    df_bronze = df_raw \
        .withColumn("_data_ref", lit(data_ref)) \
        .withColumn("_ingestion_ts", current_timestamp()) \
        .withColumn("_source", lit("pipeline_vendas_v1"))

    # Persistir na camada Bronze (overwrite por partição = idempotente)
    caminho_bronze = f"{output_path}/bronze/vendas"
    df_bronze.write \
        .mode("overwrite") \
        .partitionBy("_data_ref") \
        .parquet(caminho_bronze)

    logger.info(f"[BRONZE] Escrita concluída em: {caminho_bronze}")
    logger.info(f"[BRONZE] ✅ Concluído — {contagem:,} registros ingeridos")

    return contagem
```

**Pontos importantes:**

1. **Metadados de rastreabilidade** (`_data_ref`, `_ingestion_ts`, `_source`): Permitem saber quando e de onde cada registro foi ingerido
2. **`partitionBy("_data_ref")`**: Cada dia fica em seu próprio diretório
3. **`mode("overwrite")`** + `partitionOverwriteMode=dynamic`: Só sobrescreve a partição do dia, preservando outros dias

---

## Passo 6: Implementar a Etapa Silver (Limpeza)

**Descrição:** A camada Silver aplica limpeza, deduplicação e validação. Dados inválidos são removidos.

**Adicione ao arquivo:**

```python
def etapa_silver(spark: SparkSession, output_path: str,
                 data_ref: str, logger: logging.Logger) -> int:
    """
    Camada Silver: Limpeza e normalização dos dados.

    - Lê da camada Bronze (partição do dia)
    - Remove registros com campos obrigatórios nulos
    - Remove duplicatas por order_id
    - Filtra registros com valores inválidos (quantity <= 0, total_amount < 0)
    - Persiste dados limpos e validados

    Returns:
        Número de registros válidos na Silver
    """
    logger.info(f"[SILVER] Iniciando limpeza para data_ref={data_ref}")

    # Ler da camada Bronze (partição do dia)
    caminho_bronze = f"{output_path}/bronze/vendas/_data_ref={data_ref}"
    df_bronze = spark.read.parquet(caminho_bronze)
    contagem_entrada = df_bronze.count()
    logger.info(f"[SILVER] Registros da Bronze: {contagem_entrada:,}")

    # Limpeza: remover nulls em campos obrigatórios
    df_limpo = df_bronze.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("total_amount").isNotNull() &
        col("order_date").isNotNull()
    )

    # Remover duplicatas por order_id (mantém primeiro)
    df_dedup = df_limpo.dropDuplicates(["order_id"])

    # Filtrar valores inválidos
    df_valido = df_dedup.filter(
        (col("quantity") > 0) &
        (col("total_amount") >= 0)
    )

    contagem_saida = df_valido.count()
    registros_removidos = contagem_entrada - contagem_saida
    logger.info(f"[SILVER] Registros removidos: {registros_removidos:,}")
    logger.info(f"[SILVER] Registros válidos: {contagem_saida:,}")

    # Adicionar coluna de partição para escrita idempotente
    df_silver = df_valido.withColumn("data_ref", lit(data_ref))

    # Persistir na camada Silver (overwrite por partição = idempotente)
    caminho_silver = f"{output_path}/silver/vendas"
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("data_ref") \
        .parquet(caminho_silver)

    logger.info(f"[SILVER] Escrita concluída em: {caminho_silver}")
    logger.info(f"[SILVER] ✅ Concluído — {contagem_saida:,} registros válidos")

    return contagem_saida
```

**O que a Silver faz (resumo):**

```
Bronze (raw)          Silver (limpo)
─────────────        ──────────────
28.500 registros  →  ~27.800 registros

Removidos:
  - Nulls em campos obrigatórios (~2%)
  - Duplicatas por order_id (~0.5%)
  - quantity <= 0 ou total_amount < 0 (~0.5%)
```

---

## Passo 7: Implementar a Etapa Gold (Agregações)

**Descrição:** A camada Gold produz dados prontos para consumo de negócio. Aqui fazemos joins com tabelas de referência e calculamos métricas agregadas.

**Adicione ao arquivo:**

```python
def etapa_gold(spark: SparkSession, input_path: str, output_path: str,
               data_ref: str, logger: logging.Logger) -> int:
    """
    Camada Gold: Agregações e métricas de negócio.

    - Lê da camada Silver (partição do dia)
    - Enriquece com dados de produtos (join)
    - Calcula métricas: faturamento por estado, categoria, método de pagamento
    - Persiste agregações prontas para consumo

    Returns:
        Número de registros na Gold (agregados)
    """
    logger.info(f"[GOLD] Iniciando agregações para data_ref={data_ref}")

    # Ler da camada Silver (partição do dia)
    caminho_silver = f"{output_path}/silver/vendas/data_ref={data_ref}"
    df_silver = spark.read.parquet(caminho_silver)

    # Ler dados de referência para enriquecimento
    df_produtos = spark.read.parquet(f"{input_path}/produtos.parquet")

    # Join com produtos para enriquecer com categoria
    df_enriquecido = df_silver.join(
        df_produtos.select("product_id", "category", "subcategory"),
        on="product_id",
        how="left"
    )

    # Agregação: métricas por estado e categoria
    df_metricas = df_enriquecido.groupBy(
        "shipping_state", "category"
    ).agg(
        spark_sum("total_amount").alias("faturamento_total"),
        count("order_id").alias("total_pedidos"),
        avg("total_amount").alias("ticket_medio")
    ).withColumn("data_ref", lit(data_ref))

    contagem_gold = df_metricas.count()
    logger.info(f"[GOLD] Métricas geradas: {contagem_gold:,} combinações estado/categoria")

    # Persistir na camada Gold (overwrite por partição = idempotente)
    caminho_gold = f"{output_path}/gold/metricas_vendas"
    df_metricas.write \
        .mode("overwrite") \
        .partitionBy("data_ref") \
        .parquet(caminho_gold)

    logger.info(f"[GOLD] Escrita concluída em: {caminho_gold}")
    logger.info(f"[GOLD] ✅ Concluído — {contagem_gold:,} agregações geradas")

    return contagem_gold
```

**Fluxo da Gold:**

```
Silver (vendas limpas)  +  Produtos (referência)
         │                        │
         └──── LEFT JOIN ─────────┘
                    │
                    ▼
         GROUP BY (estado, categoria)
                    │
                    ▼
         Métricas: faturamento, pedidos, ticket médio
                    │
                    ▼
         Gold (particionada por data_ref)
```

> **Ana (Product Owner):** "Perfeito! Com essas métricas na camada Gold, minha equipe de BI pode montar dashboards que atualizam automaticamente todo dia. Não precisam mais rodar queries pesadas — os dados já chegam agregados."

---

## Passo 8: Implementar a Função Main com Tratamento de Erros

**Descrição:** A função `main()` orquestra todas as etapas e implementa tratamento de erros. Se qualquer etapa falhar, o script encerra com `sys.exit(1)` — que é como o Airflow detecta que o job falhou.

**Adicione ao arquivo:**

```python
# ============================================================
# 4. FUNÇÃO PRINCIPAL
# ============================================================
def main():
    """
    Função principal do pipeline.
    Executa as três etapas (Bronze → Silver → Gold) em sequência.
    Em caso de erro, encerra com código de saída 1.
    """
    # Parsear argumentos
    args = parse_args()

    # Configurar logging
    logger = configurar_logging(args.log_level)

    logger.info("=" * 60)
    logger.info("PIPELINE DE VENDAS — DATAFLOW ANALYTICS")
    logger.info("=" * 60)
    logger.info(f"Data de referência: {args.data_ref}")
    logger.info(f"Input path: {args.input_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Log level: {args.log_level}")
    logger.info("=" * 60)

    spark = None

    try:
        # Criar SparkSession
        spark = criar_spark_session(args.data_ref)
        logger.info(f"SparkSession criada — versão {spark.version}")

        # Etapa 1: Bronze (Ingestão)
        registros_bronze = etapa_bronze(
            spark, args.input_path, args.output_path, args.data_ref, logger
        )

        # Etapa 2: Silver (Limpeza)
        registros_silver = etapa_silver(
            spark, args.output_path, args.data_ref, logger
        )

        # Etapa 3: Gold (Agregações)
        registros_gold = etapa_gold(
            spark, args.input_path, args.output_path, args.data_ref, logger
        )

        # Resumo final
        logger.info("=" * 60)
        logger.info("RESUMO DO PROCESSAMENTO")
        logger.info("=" * 60)
        logger.info(f"Data processada: {args.data_ref}")
        logger.info(f"Bronze (ingestão):  {registros_bronze:,} registros")
        logger.info(f"Silver (limpeza):   {registros_silver:,} registros")
        logger.info(f"Gold (agregações):  {registros_gold:,} métricas")
        logger.info("=" * 60)
        logger.info("✅ Pipeline concluído com SUCESSO")

    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        logger.error(f"Verifique se os dados existem para data_ref={args.data_ref}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Erro inesperado no pipeline: {e}")
        logger.error(f"Tipo do erro: {type(e).__name__}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada")


# ============================================================
# 5. ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    main()
```

**Por que `sys.exit(1)` é crucial:**

```
┌─────────────────────────────────────────────────────────────┐
│  CÓDIGO DE SAÍDA E ORQUESTRAÇÃO                              │
│                                                              │
│  sys.exit(0) → Sucesso   → Airflow marca task como SUCCESS  │
│  sys.exit(1) → Falha     → Airflow marca task como FAILED   │
│                           → Pode fazer retry automático      │
│                           → Pode disparar on_failure_callback│
│                                                              │
│  Sem sys.exit(1):                                            │
│  - Script falha com exceção → exit code depende do Python   │
│  - Comportamento inconsistente entre versões                 │
│  - Airflow pode não detectar a falha corretamente            │
└─────────────────────────────────────────────────────────────┘
```

**Padrão try/except/finally:**

| Bloco | Responsabilidade |
|-------|-----------------|
| `try` | Executa o pipeline normalmente |
| `except FileNotFoundError` | Captura erro específico (dados não existem para a data) |
| `except Exception` | Captura qualquer outro erro inesperado |
| `finally` | **Sempre** fecha a SparkSession (libera recursos) |

> **Marina:** "O `finally` é fundamental. Se o script falhar no meio, o `spark.stop()` no finally garante que a SparkSession é encerrada e os recursos do cluster são liberados. Sem isso, connections ficam penduradas."

---

## Passo 9: Criar o Arquivo Completo (Versão Final)

**Descrição:** Agora vamos criar o arquivo completo de uma única vez. Este é o script final que usaremos em produção.

**Comando:**

```bash
cat > aula_07/code/spark_jobs/pipeline_vendas.py << 'PYEOF'
"""
Pipeline de Vendas — DataFlow Analytics
=========================================
Script de produção para processamento de vendas diárias.
Implementa pipeline Bronze → Silver → Gold com escrita idempotente.

Uso:
    python pipeline_vendas.py --data-ref 2024-01-15
    python pipeline_vendas.py --data-ref 2024-01-15 --input-path /data/incoming --output-path /datalake

Via spark-submit:
    spark-submit --master spark://spark-master:7077 pipeline_vendas.py --data-ref 2024-01-15

Autor: Carlos Mendes (Engenheiro de Dados Sênior - DataFlow Analytics)
Versão: 1.0.0
"""

import argparse
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as spark_sum,
    count, avg, when, isnan, isnull
)


# ============================================================
# 1. CONFIGURAÇÃO DE LOGGING
# ============================================================
def configurar_logging(log_level: str) -> logging.Logger:
    """Configura logging com formato padronizado para produção."""
    logger = logging.getLogger("pipeline_vendas")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# ============================================================
# 2. PARSING DE ARGUMENTOS CLI
# ============================================================
def parse_args() -> argparse.Namespace:
    """Parseia argumentos de linha de comando."""
    parser = argparse.ArgumentParser(
        description="Pipeline de vendas DataFlow Analytics — Bronze → Silver → Gold",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python pipeline_vendas.py --data-ref 2023-12-01
  python pipeline_vendas.py --data-ref 2023-12-01 --input-path /data/producao
  spark-submit --master spark://spark-master:7077 pipeline_vendas.py --data-ref 2023-12-01
        """
    )

    parser.add_argument(
        "--data-ref", type=str, required=True,
        help="Data de referência para processamento (formato: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--input-path", type=str, default="data/aula_07/producao",
        help="Diretório base dos dados de entrada (default: data/aula_07/producao)"
    )
    parser.add_argument(
        "--output-path", type=str, default="data/aula_07/datalake",
        help="Diretório base para saída do datalake (default: data/aula_07/datalake)"
    )
    parser.add_argument(
        "--log-level", type=str, default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nível de logging (default: INFO)"
    )

    args = parser.parse_args()

    # Validação do formato da data
    try:
        datetime.strptime(args.data_ref, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--data-ref deve estar no formato YYYY-MM-DD. Recebido: '{args.data_ref}'")

    return args


# ============================================================
# 3. FUNÇÕES DO PIPELINE
# ============================================================
def criar_spark_session(data_ref: str) -> SparkSession:
    """Cria SparkSession configurada para produção."""
    spark = SparkSession.builder \
        .appName(f"DataFlow-PipelineVendas-{data_ref}") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def etapa_bronze(spark, input_path, output_path, data_ref, logger):
    """Camada Bronze: Ingestão raw com metadados de rastreabilidade."""
    logger.info(f"[BRONZE] Iniciando ingestão para data_ref={data_ref}")

    caminho_entrada = f"{input_path}/incoming/{data_ref}/vendas.parquet"
    logger.info(f"[BRONZE] Lendo: {caminho_entrada}")

    df_raw = spark.read.parquet(caminho_entrada)
    contagem = df_raw.count()
    logger.info(f"[BRONZE] Registros lidos: {contagem:,}")

    df_bronze = df_raw \
        .withColumn("_data_ref", lit(data_ref)) \
        .withColumn("_ingestion_ts", current_timestamp()) \
        .withColumn("_source", lit("pipeline_vendas_v1"))

    caminho_bronze = f"{output_path}/bronze/vendas"
    df_bronze.write \
        .mode("overwrite") \
        .partitionBy("_data_ref") \
        .parquet(caminho_bronze)

    logger.info(f"[BRONZE] ✅ Concluído — {contagem:,} registros ingeridos")
    return contagem


def etapa_silver(spark, output_path, data_ref, logger):
    """Camada Silver: Limpeza e normalização dos dados."""
    logger.info(f"[SILVER] Iniciando limpeza para data_ref={data_ref}")

    caminho_bronze = f"{output_path}/bronze/vendas/_data_ref={data_ref}"
    df_bronze = spark.read.parquet(caminho_bronze)
    contagem_entrada = df_bronze.count()
    logger.info(f"[SILVER] Registros da Bronze: {contagem_entrada:,}")

    df_limpo = df_bronze.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("total_amount").isNotNull() &
        col("order_date").isNotNull()
    )

    df_dedup = df_limpo.dropDuplicates(["order_id"])

    df_valido = df_dedup.filter(
        (col("quantity") > 0) &
        (col("total_amount") >= 0)
    )

    contagem_saida = df_valido.count()
    registros_removidos = contagem_entrada - contagem_saida
    logger.info(f"[SILVER] Registros removidos: {registros_removidos:,}")

    df_silver = df_valido.withColumn("data_ref", lit(data_ref))

    caminho_silver = f"{output_path}/silver/vendas"
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("data_ref") \
        .parquet(caminho_silver)

    logger.info(f"[SILVER] ✅ Concluído — {contagem_saida:,} registros válidos")
    return contagem_saida


def etapa_gold(spark, input_path, output_path, data_ref, logger):
    """Camada Gold: Agregações e métricas de negócio."""
    logger.info(f"[GOLD] Iniciando agregações para data_ref={data_ref}")

    caminho_silver = f"{output_path}/silver/vendas/data_ref={data_ref}"
    df_silver = spark.read.parquet(caminho_silver)

    df_produtos = spark.read.parquet(f"{input_path}/produtos.parquet")

    df_enriquecido = df_silver.join(
        df_produtos.select("product_id", "category", "subcategory"),
        on="product_id",
        how="left"
    )

    df_metricas = df_enriquecido.groupBy(
        "shipping_state", "category"
    ).agg(
        spark_sum("total_amount").alias("faturamento_total"),
        count("order_id").alias("total_pedidos"),
        avg("total_amount").alias("ticket_medio")
    ).withColumn("data_ref", lit(data_ref))

    contagem_gold = df_metricas.count()

    caminho_gold = f"{output_path}/gold/metricas_vendas"
    df_metricas.write \
        .mode("overwrite") \
        .partitionBy("data_ref") \
        .parquet(caminho_gold)

    logger.info(f"[GOLD] ✅ Concluído — {contagem_gold:,} agregações geradas")
    return contagem_gold


# ============================================================
# 4. FUNÇÃO PRINCIPAL
# ============================================================
def main():
    """Função principal — orquestra Bronze → Silver → Gold."""
    args = parse_args()
    logger = configurar_logging(args.log_level)

    logger.info("=" * 60)
    logger.info("PIPELINE DE VENDAS — DATAFLOW ANALYTICS")
    logger.info("=" * 60)
    logger.info(f"Data de referência: {args.data_ref}")
    logger.info(f"Input path: {args.input_path}")
    logger.info(f"Output path: {args.output_path}")
    logger.info(f"Log level: {args.log_level}")
    logger.info("=" * 60)

    spark = None

    try:
        spark = criar_spark_session(args.data_ref)
        logger.info(f"SparkSession criada — versão {spark.version}")

        registros_bronze = etapa_bronze(
            spark, args.input_path, args.output_path, args.data_ref, logger
        )
        registros_silver = etapa_silver(
            spark, args.output_path, args.data_ref, logger
        )
        registros_gold = etapa_gold(
            spark, args.input_path, args.output_path, args.data_ref, logger
        )

        logger.info("=" * 60)
        logger.info("RESUMO DO PROCESSAMENTO")
        logger.info("=" * 60)
        logger.info(f"Data processada: {args.data_ref}")
        logger.info(f"Bronze (ingestão):  {registros_bronze:,} registros")
        logger.info(f"Silver (limpeza):   {registros_silver:,} registros")
        logger.info(f"Gold (agregações):  {registros_gold:,} métricas")
        logger.info("=" * 60)
        logger.info("✅ Pipeline concluído com SUCESSO")

    except FileNotFoundError as e:
        logger.error(f"Arquivo não encontrado: {e}")
        logger.error(f"Verifique se os dados existem para data_ref={args.data_ref}")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Erro inesperado no pipeline: {e}")
        logger.error(f"Tipo do erro: {type(e).__name__}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada")


# ============================================================
# 5. ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    main()
PYEOF
```

**Resultado esperado:**
```
(nenhuma saída — arquivo criado com sucesso)
```

**Verificação rápida — sintaxe Python:**
```bash
python -c "import ast; ast.parse(open('aula_07/code/spark_jobs/pipeline_vendas.py').read()); print('✅ Sintaxe válida!')"
```

**Resultado esperado:**
```
✅ Sintaxe válida!
```

---

## Passo 10: Testar o Help do Script

**Descrição:** Antes de executar o pipeline completo, vamos verificar que o `argparse` está funcionando corretamente testando o `--help`.

**Comando:**

```bash
python aula_07/code/spark_jobs/pipeline_vendas.py --help
```

**Resultado esperado:**

```
usage: pipeline_vendas.py [-h] --data-ref DATA_REF [--input-path INPUT_PATH]
                          [--output-path OUTPUT_PATH]
                          [--log-level {DEBUG,INFO,WARNING,ERROR}]

Pipeline de vendas DataFlow Analytics — Bronze → Silver → Gold

options:
  -h, --help            show this help message and exit
  --data-ref DATA_REF   Data de referência para processamento (formato: YYYY-MM-DD)
  --input-path INPUT_PATH
                        Diretório base dos dados de entrada (default: data/aula_07/producao)
  --output-path OUTPUT_PATH
                        Diretório base para saída do datalake (default: data/aula_07/datalake)
  --log-level {DEBUG,INFO,WARNING,ERROR}
                        Nível de logging (default: INFO)

Exemplos de uso:
  python pipeline_vendas.py --data-ref 2023-12-01
  python pipeline_vendas.py --data-ref 2023-12-01 --input-path /data/producao
  spark-submit --master spark://spark-master:7077 pipeline_vendas.py --data-ref 2023-12-01
```

**Testando validação de erros:**

```bash
# Sem argumentos → erro claro
python aula_07/code/spark_jobs/pipeline_vendas.py
```

**Resultado esperado:**
```
usage: pipeline_vendas.py [-h] --data-ref DATA_REF ...
pipeline_vendas.py: error: the following arguments are required: --data-ref
```

```bash
# Data inválida → erro de validação
python aula_07/code/spark_jobs/pipeline_vendas.py --data-ref "abc"
```

**Resultado esperado:**
```
usage: pipeline_vendas.py [-h] --data-ref DATA_REF ...
pipeline_vendas.py: error: --data-ref deve estar no formato YYYY-MM-DD. Recebido: 'abc'
```

> **Carlos:** "Repare como o argparse dá mensagens de erro claras e o `--help` é gerado automaticamente. Isso é o que diferencia um script amador de um script profissional."

---

## Passo 11: Testar o Pipeline Localmente

**Descrição:** Agora vamos executar o pipeline completo localmente processando o dia `2023-12-01`. O script vai ler os dados de `datasets/aula_07/producao/incoming/2023-12-01/`, processar Bronze → Silver → Gold, e salvar o resultado.

**Comando:**

```bash
cd /opt/spark/work-dir  # ou o diretório raiz do projeto no container

python aula_07/code/spark_jobs/pipeline_vendas.py \
    --data-ref 2023-12-01 \
    --input-path datasets/aula_07/producao \
    --output-path data/aula_07/datalake \
    --log-level INFO
```

**Resultado esperado:**

```
[2024-01-15 10:30:00] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:30:00] INFO | pipeline_vendas | PIPELINE DE VENDAS — DATAFLOW ANALYTICS
[2024-01-15 10:30:00] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:30:00] INFO | pipeline_vendas | Data de referência: 2023-12-01
[2024-01-15 10:30:00] INFO | pipeline_vendas | Input path: datasets/aula_07/producao
[2024-01-15 10:30:00] INFO | pipeline_vendas | Output path: data/aula_07/datalake
[2024-01-15 10:30:00] INFO | pipeline_vendas | Log level: INFO
[2024-01-15 10:30:00] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:30:02] INFO | pipeline_vendas | SparkSession criada — versão 3.5.x
[2024-01-15 10:30:02] INFO | pipeline_vendas | [BRONZE] Iniciando ingestão para data_ref=2023-12-01
[2024-01-15 10:30:02] INFO | pipeline_vendas | [BRONZE] Lendo: datasets/aula_07/producao/incoming/2023-12-01/vendas.parquet
[2024-01-15 10:30:03] INFO | pipeline_vendas | [BRONZE] Registros lidos: 28,500
[2024-01-15 10:30:04] INFO | pipeline_vendas | [BRONZE] ✅ Concluído — 28,500 registros ingeridos
[2024-01-15 10:30:04] INFO | pipeline_vendas | [SILVER] Iniciando limpeza para data_ref=2023-12-01
[2024-01-15 10:30:05] INFO | pipeline_vendas | [SILVER] Registros da Bronze: 28,500
[2024-01-15 10:30:05] INFO | pipeline_vendas | [SILVER] Registros removidos: ~500
[2024-01-15 10:30:06] INFO | pipeline_vendas | [SILVER] ✅ Concluído — ~28,000 registros válidos
[2024-01-15 10:30:06] INFO | pipeline_vendas | [GOLD] Iniciando agregações para data_ref=2023-12-01
[2024-01-15 10:30:08] INFO | pipeline_vendas | [GOLD] ✅ Concluído — ~200 agregações geradas
[2024-01-15 10:30:08] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:30:08] INFO | pipeline_vendas | RESUMO DO PROCESSAMENTO
[2024-01-15 10:30:08] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:30:08] INFO | pipeline_vendas | Data processada: 2023-12-01
[2024-01-15 10:30:08] INFO | pipeline_vendas | Bronze (ingestão):  28,500 registros
[2024-01-15 10:30:08] INFO | pipeline_vendas | Silver (limpeza):   ~28,000 registros
[2024-01-15 10:30:08] INFO | pipeline_vendas | Gold (agregações):  ~200 métricas
[2024-01-15 10:30:08] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:30:08] INFO | pipeline_vendas | ✅ Pipeline concluído com SUCESSO
[2024-01-15 10:30:08] INFO | pipeline_vendas | SparkSession encerrada
```

> **💡 Nota:** Os números exatos podem variar dependendo dos dados gerados. O importante é que o pipeline complete sem erros.

**Verificar o código de saída:**
```bash
echo $?
```

**Resultado esperado:**
```
0
```

(Zero = sucesso. Qualquer outro valor = falha.)

---

## Passo 12: Testar via spark-submit no Docker

**Descrição:** Em produção, scripts PySpark não são executados diretamente com `python` — são submetidos ao cluster via `spark-submit`. Isso permite configurar resources (memória, cores) e conectar ao cluster distribuído.

**Comando:**

```bash
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 1g \
    --driver-memory 1g \
    /opt/spark-jobs/pipeline_vendas.py \
    --data-ref 2023-12-01 \
    --input-path /data/producao \
    --output-path /data/datalake
```

**Resultado esperado:**

O output será similar ao Passo 11, mas com logs adicionais do Spark (submissão ao cluster, alocação de executors):

```
... (logs do Spark sobre conexão com cluster)
[2024-01-15 10:35:00] INFO | pipeline_vendas | ============================================================
[2024-01-15 10:35:00] INFO | pipeline_vendas | PIPELINE DE VENDAS — DATAFLOW ANALYTICS
...
[2024-01-15 10:35:10] INFO | pipeline_vendas | ✅ Pipeline concluído com SUCESSO
```

**Diferenças entre `python` e `spark-submit`:**

| Aspecto | `python script.py` | `spark-submit script.py` |
|---------|--------------------|-----------------------|
| SparkSession | Usa `.master("local[*]")` ou o builder | Conecta ao cluster via `--master` |
| Recursos | Limitado à máquina local | Distribuído entre executors |
| Deploy mode | Sempre local | `client` ou `cluster` |
| Configuração | Só via código | Via CLI `--conf`, `--executor-memory` |
| Produção | ❌ Não recomendado | ✅ Padrão da indústria |

> **Carlos:** "O `spark-submit` é o ponto de entrada oficial para jobs Spark em produção. Quando o Airflow usar o SparkSubmitOperator, ele vai executar exatamente este comando."

---

## Passo 13: Verificar a Idempotência

**Descrição:** Um pipeline idempotente produz o mesmo resultado independente de quantas vezes é executado. Vamos verificar executando o pipeline duas vezes para a mesma data e comparando os resultados.

**Comando — Primeira execução:**

```bash
python aula_07/code/spark_jobs/pipeline_vendas.py \
    --data-ref 2023-12-01 \
    --input-path datasets/aula_07/producao \
    --output-path data/aula_07/datalake
```

**Comando — Segunda execução (mesma data):**

```bash
python aula_07/code/spark_jobs/pipeline_vendas.py \
    --data-ref 2023-12-01 \
    --input-path datasets/aula_07/producao \
    --output-path data/aula_07/datalake
```

**Verificar que os dados não duplicaram:**

```python
# Execute no PySpark shell ou Jupyter
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Verificacao-Idempotencia") \
    .getOrCreate()

# Contar registros na Silver após 2 execuções
df_silver = spark.read.parquet("data/aula_07/datalake/silver/vendas/data_ref=2023-12-01")
print(f"Registros na Silver: {df_silver.count():,}")

# Contar registros na Gold após 2 execuções
df_gold = spark.read.parquet("data/aula_07/datalake/gold/metricas_vendas/data_ref=2023-12-01")
print(f"Registros na Gold: {df_gold.count():,}")

# Se fosse mode="append", teria o DOBRO de registros!
# Com mode="overwrite" + partitionBy, o número é sempre o mesmo ✅
```

**Resultado esperado:**

```
Registros na Silver: ~28,000  (igual à primeira execução)
Registros na Gold: ~200       (igual à primeira execução)
```

**O que garante a idempotência:**

```
1. spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
   → Só sobrescreve partições presentes no DataFrame

2. .mode("overwrite").partitionBy("data_ref")
   → Sobrescreve a partição inteira do dia

3. Resultado: executar 1x ou 100x = mesmo número de registros
```

> **Marina:** "Se o pipeline falhar às 3h da manhã e o retry automático executar às 3:05h, com idempotência não precisamos nos preocupar — o resultado será exatamente o mesmo. Sem idempotência, teríamos dados duplicados toda vez que um retry acontecesse."

---

## Passo 14: Testar Processamento de Outro Dia

**Descrição:** Vamos confirmar que o script funciona com diferentes datas, processando um segundo dia sem afetar o primeiro.

**Comando:**

```bash
python aula_07/code/spark_jobs/pipeline_vendas.py \
    --data-ref 2023-12-02 \
    --input-path datasets/aula_07/producao \
    --output-path data/aula_07/datalake
```

**Verificar que ambos os dias existem:**

```bash
ls data/aula_07/datalake/silver/vendas/
```

**Resultado esperado:**

```
data_ref=2023-12-01/
data_ref=2023-12-02/
```

**Confirmar que o dia 01 não foi afetado:**

```python
df_dia_01 = spark.read.parquet("data/aula_07/datalake/silver/vendas/data_ref=2023-12-01")
df_dia_02 = spark.read.parquet("data/aula_07/datalake/silver/vendas/data_ref=2023-12-02")

print(f"Dia 01: {df_dia_01.count():,} registros (intacto)")
print(f"Dia 02: {df_dia_02.count():,} registros (novo)")
```

> **Carlos:** "Repare que processar `2023-12-02` não alterou os dados de `2023-12-01`. É isso que o `partitionOverwriteMode=dynamic` garante — cada dia é independente."

---

## Passo 15: Verificar Estrutura Final do Datalake

**Descrição:** Após processar dois dias, vamos verificar a estrutura completa do datalake gerado.

**Comando:**

```bash
find data/aula_07/datalake -type d | sort
```

**Resultado esperado:**

```
data/aula_07/datalake/
data/aula_07/datalake/bronze/
data/aula_07/datalake/bronze/vendas/
data/aula_07/datalake/bronze/vendas/_data_ref=2023-12-01/
data/aula_07/datalake/bronze/vendas/_data_ref=2023-12-02/
data/aula_07/datalake/silver/
data/aula_07/datalake/silver/vendas/
data/aula_07/datalake/silver/vendas/data_ref=2023-12-01/
data/aula_07/datalake/silver/vendas/data_ref=2023-12-02/
data/aula_07/datalake/gold/
data/aula_07/datalake/gold/metricas_vendas/
data/aula_07/datalake/gold/metricas_vendas/data_ref=2023-12-01/
data/aula_07/datalake/gold/metricas_vendas/data_ref=2023-12-02/
```

**Arquitetura visual:**

```
datalake/
├── bronze/vendas/                     ← Dados raw com metadados
│   ├── _data_ref=2023-12-01/
│   └── _data_ref=2023-12-02/
├── silver/vendas/                     ← Dados limpos e validados
│   ├── data_ref=2023-12-01/
│   └── data_ref=2023-12-02/
└── gold/metricas_vendas/              ← Métricas de negócio
    ├── data_ref=2023-12-01/
    └── data_ref=2023-12-02/
```

---

## Resumo

Neste exercício você aprendeu a:

| # | Conceito | O que fez |
|---|----------|-----------|
| 1 | Scripts CLI vs Notebooks | Entendeu por que produção requer scripts parametrizados |
| 2 | `argparse` | Criou interface CLI com `--data-ref`, `--input-path`, `--output-path` |
| 3 | Pipeline Bronze → Silver → Gold | Implementou 3 camadas em funções separadas |
| 4 | Tratamento de erros | Usou `try/except/finally` com `sys.exit(1)` |
| 5 | `spark-submit` | Executou o script via submissão ao cluster Spark |
| 6 | Idempotência | Verificou que 2 execuções = mesmo resultado |
| 7 | `partitionOverwriteMode=dynamic` | Configurou overwrite cirúrgico por partição |

**Próximo exercício:** Vamos adicionar **logging estruturado** (formato JSON) ao nosso script para permitir monitoramento e alertas automatizados em produção.

> **Marina:** "Excelente! Agora temos um script de produção que qualquer pessoa pode executar com um único comando. O Airflow vai poder chamá-lo via SparkSubmitOperator passando a data como parâmetro. Esse é o padrão que usamos na indústria."
