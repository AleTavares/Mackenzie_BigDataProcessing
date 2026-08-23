"""
DAG: Pipeline de Processamento Adaptativo da DataFlow Analytics
================================================================
Descrição: Conta registros de vendas do dia e decide automaticamente
           se processa com Spark (alto volume) ou Python (baixo volume).
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: BranchPythonOperator com convergência via trigger_rule
Schedule: Todo dia às 7h (após as extrações)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import random


default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ============================================================
# FUNÇÕES — Lógica de negócio
# ============================================================
def contar_registros(**context):
    """Conta o volume de registros de vendas do dia."""
    execution_date = context["ds"]
    print(f"{'=' * 50}")
    print(f"📊 CONTAGEM DE REGISTROS")
    print(f"{'=' * 50}")
    print(f"Data de referência: {execution_date}")
    print(f"Verificando volume de vendas do dia...")

    # Simula volume variado para testar ambos os caminhos
    volume = random.choice([1_500_000, 500_000, 2_000_000, 300_000])

    print(f"✅ Volume detectado: {volume:,} registros")
    print(f"   Limiar de decisão: 1.000.000 registros")

    if volume > 1_000_000:
        print(f"   → Volume ALTO: processamento distribuído recomendado")
    else:
        print(f"   → Volume NORMAL: processamento leve suficiente")

    context["ti"].xcom_push(key="volume_registros", value=volume)
    return volume


def decidir_processamento(**context):
    """
    FUNÇÃO DE BRANCHING — Retorna o task_id do caminho escolhido.
    - volume > 1M → "processar_spark"
    - volume ≤ 1M → "processar_python"
    """
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"{'=' * 50}")
    print(f"🔀 DECISÃO DE PROCESSAMENTO")
    print(f"{'=' * 50}")
    print(f"Volume detectado: {volume:,} registros")
    print(f"Limiar: 1.000.000 registros")

    if volume > 1_000_000:
        decisao = "processar_spark"
        print(f"\n📌 DECISÃO: Processar com SPARK (distribuído)")
        print(f"   Motivo: Volume ({volume:,}) excede 1M")
    else:
        decisao = "processar_python"
        print(f"\n📌 DECISÃO: Processar com PYTHON (leve)")
        print(f"   Motivo: Volume ({volume:,}) está dentro do limiar")

    print(f"   → Task selecionada: '{decisao}'")
    return decisao


def processar_spark(**context):
    """Processamento distribuído com Apache Spark."""
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"{'=' * 50}")
    print(f"⚡ PROCESSAMENTO DISTRIBUÍDO (SPARK)")
    print(f"{'=' * 50}")
    print(f"Volume: {volume:,} registros")
    print(f"Inicializando SparkSession...")
    print(f"  - Executors: 4")
    print(f"  - Memory per executor: 2GB")
    print(f"  - Cores per executor: 2")
    print(f"Processando em paralelo...")

    registros_processados = int(volume * 0.97)
    print(f"✅ Spark concluído: {registros_processados:,} registros processados")
    print(f"   Rejeitados: {volume - registros_processados:,}")

    return {
        "metodo": "spark",
        "registros_processados": registros_processados,
        "registros_rejeitados": volume - registros_processados,
    }


def processar_python(**context):
    """Processamento leve com Python/pandas."""
    ti = context["ti"]
    volume = ti.xcom_pull(task_ids="contar_registros", key="volume_registros")

    print(f"{'=' * 50}")
    print(f"🐍 PROCESSAMENTO LEVE (PYTHON)")
    print(f"{'=' * 50}")
    print(f"Volume: {volume:,} registros")
    print(f"Carregando com pandas (single-node, sem overhead)...")
    print(f"Processando localmente...")

    registros_processados = int(volume * 0.98)
    print(f"✅ Python concluído: {registros_processados:,} registros processados")
    print(f"   Rejeitados: {volume - registros_processados:,}")

    return {
        "metodo": "python",
        "registros_processados": registros_processados,
        "registros_rejeitados": volume - registros_processados,
    }


def gerar_relatorio(**context):
    """Gera relatório consolidado (convergência dos dois caminhos)."""
    ti = context["ti"]
    resultado_spark = ti.xcom_pull(task_ids="processar_spark")
    resultado_python = ti.xcom_pull(task_ids="processar_python")
    resultado = resultado_spark if resultado_spark else resultado_python

    print(f"{'=' * 50}")
    print(f"📋 GERAÇÃO DE RELATÓRIO")
    print(f"{'=' * 50}")
    print(f"Método utilizado: {resultado['metodo'].upper()}")
    print(f"Registros processados: {resultado['registros_processados']:,}")
    print(f"Registros rejeitados: {resultado['registros_rejeitados']:,}")
    print(f"")
    print(f"Gerando relatório diário...")
    print(f"✅ Relatório gerado com sucesso!")
    print(f"   Salvo em: relatorios/diario_{context['ds']}.pdf")

    return {
        "relatorio_gerado": True,
        "metodo_utilizado": resultado["metodo"],
        "total_processado": resultado["registros_processados"],
    }


# ============================================================
# DAG — Pipeline de Processamento Adaptativo
# ============================================================
with DAG(
    dag_id="dataflow_branching_processamento_v1",
    default_args=default_args,
    description="Pipeline adaptativo: Spark ou Python baseado no volume",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "branching", "adaptativo", "aula05"],
) as dag:

    contar = PythonOperator(
        task_id="contar_registros",
        python_callable=contar_registros,
    )

    decidir = BranchPythonOperator(
        task_id="decidir_processamento",
        python_callable=decidir_processamento,
    )

    spark = PythonOperator(
        task_id="processar_spark",
        python_callable=processar_spark,
    )

    python_proc = PythonOperator(
        task_id="processar_python",
        python_callable=processar_python,
    )

    relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
        trigger_rule="none_failed_min_one_success",
    )

    # Dependências
    contar >> decidir >> [spark, python_proc] >> relatorio
