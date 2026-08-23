"""
DAG: Pipeline de Vendas em Producao — DataFlow Analytics
=========================================================
Aula 07 — Pipeline End-to-End (Spark + Airflow + Docker + QA)

Fluxo: FileSensor → SparkSubmit → Quality Checks → Notificacao

Schedule: Diario as 06:00 UTC
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


# =============================================================
# CALLBACKS
# =============================================================
def alerta_falha(context):
    """Callback de falha — loga informacoes da task que falhou."""
    task = context["task_instance"]
    print(f"[ALERTA] Task '{task.task_id}' falhou!")
    print(f"  DAG: {context['dag'].dag_id}")
    print(f"  Data: {context['ds']}")
    print(f"  Erro: {context.get('exception', 'N/A')}")


# =============================================================
# DEFAULT ARGS
# =============================================================
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alerta_falha,
}


# =============================================================
# FUNCOES
# =============================================================
def quality_checks(**context):
    """
    Executa quality checks na camada Gold apos processamento.
    Falha se dados nao passam nos criterios minimos.
    """
    data_ref = context["ds"]
    print(f"[QUALITY] Verificando dados para data_ref={data_ref}")

    # Em producao, conectaria ao Spark e verificaria os dados
    # Aqui simulamos os checks
    checks = {
        "nao_vazio": True,
        "sem_valores_negativos": True,
        "campos_obrigatorios_preenchidos": True,
        "volume_minimo_100_registros": True,
    }

    failed = [k for k, v in checks.items() if not v]

    print(f"  Checks executados: {len(checks)}")
    print(f"  Passed: {len(checks) - len(failed)}")
    print(f"  Failed: {len(failed)}")

    if failed:
        raise Exception(f"Quality gate FAILED: {failed}")

    print("[QUALITY] Todos os checks passaram!")
    return {"status": "PASSED", "checks": len(checks)}


def notificar_sucesso(**context):
    """Notificacao de sucesso com resumo."""
    data_ref = context["ds"]
    print("=" * 50)
    print("PIPELINE CONCLUIDO COM SUCESSO")
    print("=" * 50)
    print(f"  Data: {data_ref}")
    print(f"  Fluxo: incoming -> Bronze -> Silver -> Gold")
    print(f"  Quality Gate: PASSED")
    print("=" * 50)


# =============================================================
# DAG
# =============================================================
with DAG(
    dag_id="dataflow_pipeline_vendas_producao",
    default_args=default_args,
    description="Pipeline E2E: Sensor -> Spark -> QA -> Notificacao",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "producao", "spark", "pipeline", "aula07"],
) as dag:

    # Task 1: Aguardar arquivo do dia
    sensor_arquivo = FileSensor(
        task_id="aguardar_arquivo",
        filepath="/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.csv",
        poke_interval=30,
        timeout=300,
        mode="poke",
        soft_fail=True,  # Nao falha a DAG se timeout (lab)
    )

    # Task 2: Executar pipeline Spark
    spark_pipeline = SparkSubmitOperator(
        task_id="executar_spark_pipeline",
        application="/opt/spark/jobs/pipeline_vendas.py",
        conn_id="spark_default",
        application_args=["--data-ref", "{{ ds }}"],
        name="pipeline_vendas_{{ ds_nodash }}",
        verbose=True,
    )

    # Task 3: Quality checks
    task_quality = PythonOperator(
        task_id="quality_checks",
        python_callable=quality_checks,
    )

    # Task 4: Notificacao
    task_notificar = PythonOperator(
        task_id="notificar_sucesso",
        python_callable=notificar_sucesso,
    )

    # Task 5: Log final (BashOperator com template)
    task_log = BashOperator(
        task_id="log_execucao",
        bash_command='echo "[{{ ds }}] Pipeline concluido as $(date +%H:%M:%S). Proxima execucao: {{ next_ds }}"',
    )

    # Dependencias
    sensor_arquivo >> spark_pipeline >> task_quality >> task_notificar >> task_log
