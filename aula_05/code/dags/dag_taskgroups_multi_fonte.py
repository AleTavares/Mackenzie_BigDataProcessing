"""
DAG: Pipeline Multi-Fonte com TaskGroups — DataFlow Analytics
=============================================================
Descrição: Pipeline organizado em 3 TaskGroups representando
           as camadas da arquitetura Medallion (Bronze/Silver/Gold).
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: Ingestão (3 fontes) → Transformação → Gold (agregações)

Cenário de Negócio:
    - DataFlow recebe dados de 3 parceiros (A, B, C)
    - Cada parceiro envia em formato diferente
    - Pipeline deve ingerir, transformar e agregar
    - TaskGroups organizam visualmente as 8 tasks em 3 blocos
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta


# ============================================================
# 1. DEFAULT ARGS
# ============================================================
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ============================================================
# 2. FUNÇÕES DE NEGÓCIO
# ============================================================
def ingerir_fonte(fonte: str, formato: str, **context):
    """Simula ingestão de dados de um parceiro específico."""
    ds = context["ds"]
    print(f"{'=' * 50}")
    print(f"📥 INGESTÃO — {fonte.upper()}")
    print(f"{'=' * 50}")
    print(f"  Fonte: {fonte}")
    print(f"  Formato: {formato}")
    print(f"  Data referência: {ds}")
    print(f"  Destino: datalake/bronze/{fonte}/{ds}/")
    print(f"  ✅ {fonte} ingerido com sucesso!")
    context["ti"].xcom_push(key="registros", value=50000)


def limpar_dados(**context):
    """Remove nulls e valores inválidos."""
    print("🧹 Limpando dados: removendo nulls e valores negativos...")
    print("   Registros removidos: 2.847 (2.3%)")
    print("   ✅ Dados limpos!")


def normalizar_schema(**context):
    """Normaliza schemas de diferentes fontes para formato único."""
    print("🔄 Normalizando schemas dos 3 parceiros...")
    print("   Colunas padronizadas: order_id, customer_id, amount, date")
    print("   ✅ Schema unificado!")


def deduplicar(**context):
    """Remove registros duplicados."""
    print("🔍 Deduplicando registros...")
    print("   Duplicatas encontradas: 1.523 (1.0%)")
    print("   ✅ Dados deduplicados!")


def agregar_vendas(**context):
    """Agrega vendas por estado e categoria para camada Gold."""
    ds = context["ds"]
    print(f"📊 Agregando vendas para {ds}...")
    print("   Dimensões: estado, categoria, payment_method")
    print("   Destino: datalake/gold/vendas_agregadas/")
    print("   ✅ Camada Gold atualizada!")


def gerar_relatorio(**context):
    """Gera relatório executivo diário."""
    ds = context["ds"]
    print(f"📋 Gerando relatório executivo de {ds}...")
    print("   Faturamento total: R$ 1.247.890,00")
    print("   Top estado: SP (34%)")
    print("   ✅ Relatório disponível em /reports/")


# ============================================================
# 3. DAG COM TASKGROUPS
# ============================================================
with DAG(
    dag_id="dataflow_taskgroups_multi_fonte_v1",
    default_args=default_args,
    description="Pipeline multi-fonte com TaskGroups (Bronze/Silver/Gold)",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "taskgroups", "medallion", "aula05"],
) as dag:

    # ========================================================
    # GRUPO 1: INGESTÃO (Camada Bronze)
    # ========================================================
    with TaskGroup(
        group_id="ingestao",
        tooltip="Ingestão de dados dos 3 parceiros → Bronze"
    ) as grupo_ingestao:

        parceiro_a = PythonOperator(
            task_id="parceiro_a",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": "parceiro_a", "formato": "CSV"},
        )

        parceiro_b = PythonOperator(
            task_id="parceiro_b",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": "parceiro_b", "formato": "JSON"},
        )

        parceiro_c = PythonOperator(
            task_id="parceiro_c",
            python_callable=ingerir_fonte,
            op_kwargs={"fonte": "parceiro_c", "formato": "Parquet"},
        )

        # Tasks dentro do grupo são PARALELAS (sem dependência entre si)
        # Os 3 parceiros são ingeridos simultaneamente!

    # ========================================================
    # GRUPO 2: TRANSFORMAÇÃO (Camada Silver)
    # ========================================================
    with TaskGroup(
        group_id="transformacao",
        tooltip="Limpeza, normalização e deduplicação → Silver"
    ) as grupo_transformacao:

        limpar = PythonOperator(
            task_id="limpar",
            python_callable=limpar_dados,
        )

        normalizar = PythonOperator(
            task_id="normalizar",
            python_callable=normalizar_schema,
        )

        dedup = PythonOperator(
            task_id="deduplicar",
            python_callable=deduplicar,
        )

        # Dentro do grupo: dependências sequenciais
        limpar >> normalizar >> dedup

    # ========================================================
    # GRUPO 3: GOLD (Camada Gold)
    # ========================================================
    with TaskGroup(
        group_id="gold",
        tooltip="Agregações de negócio e relatórios → Gold"
    ) as grupo_gold:

        agregar = PythonOperator(
            task_id="agregar_vendas",
            python_callable=agregar_vendas,
        )

        relatorio = PythonOperator(
            task_id="gerar_relatorio",
            python_callable=gerar_relatorio,
        )

        agregar >> relatorio

    # ========================================================
    # DEPENDÊNCIAS ENTRE GRUPOS
    # ========================================================
    # Toda a ingestão deve terminar antes da transformação começar
    # Toda a transformação deve terminar antes do gold começar
    grupo_ingestao >> grupo_transformacao >> grupo_gold
