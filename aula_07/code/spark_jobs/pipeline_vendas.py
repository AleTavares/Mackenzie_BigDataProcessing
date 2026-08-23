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
