"""
DAG: Pipeline Diário de Vendas da DataFlow Analytics
=====================================================
Aula 04 — Introdução ao Apache Airflow
Demonstra: PythonOperator, BashOperator, XComs, templates Jinja, retries.

Schedule: Todo dia às 6h (0 6 * * *)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# =============================================================
# DEFAULT ARGS
# =============================================================
default_args = {
    "owner": "dataflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


# =============================================================
# FUNCOES (callables)
# =============================================================
def extrair_dados(**context):
    """Extrai vendas do dia. Retorna metadados via XCom."""
    execution_date = context["ds"]
    print(f"[EXTRACAO] Data: {execution_date}")
    print("Conectando a fonte de dados...")

    # Simula extracao
    total = 1500
    print(f"Registros extraidos: {total}")

    # Push XCom (return automatico)
    return {"total": total, "fonte": "shopbrasil_api", "data_ref": execution_date}


def transformar_dados(**context):
    """Transforma e valida dados. Consome XCom da extracao."""
    ti = context["ti"]

    # Pull XCom da task anterior
    info = ti.xcom_pull(task_ids="extrair_dados")
    total = info["total"]

    print(f"[TRANSFORMACAO] Recebidos: {total} registros")
    print("Removendo duplicatas...")
    print("Validando campos obrigatorios...")

    # 95% passam na validacao
    validos = int(total * 0.95)
    rejeitados = total - validos

    print(f"Validos: {validos} | Rejeitados: {rejeitados}")

    # Push XCom explicito
    ti.xcom_push(key="registros_validos", value=validos)
    ti.xcom_push(key="registros_rejeitados", value=rejeitados)


def carregar_dados(**context):
    """Carrega dados no data lake. Consome XCom da transformacao."""
    ti = context["ti"]

    validos = ti.xcom_pull(task_ids="transformar_dados", key="registros_validos")
    data_ref = context["ds"]

    print(f"[CARGA] Salvando {validos} registros")
    print(f"Destino: datalake/silver/vendas/data={data_ref}/")
    print(f"Formato: Parquet particionado")
    print("Carga concluida!")

    return {"registros_salvos": validos, "particao": f"data={data_ref}"}


def gerar_relatorio(**context):
    """Gera relatorio de execucao consolidando metricas."""
    ti = context["ti"]

    info_extracao = ti.xcom_pull(task_ids="extrair_dados")
    validos = ti.xcom_pull(task_ids="transformar_dados", key="registros_validos")
    rejeitados = ti.xcom_pull(task_ids="transformar_dados", key="registros_rejeitados")

    print("=" * 50)
    print("RELATORIO DE EXECUCAO")
    print("=" * 50)
    print(f"Data: {context['ds']}")
    print(f"Fonte: {info_extracao['fonte']}")
    print(f"Extraidos: {info_extracao['total']}")
    print(f"Validos: {validos}")
    print(f"Rejeitados: {rejeitados}")
    print(f"Taxa aprovacao: {validos / info_extracao['total'] * 100:.1f}%")
    print("=" * 50)


# =============================================================
# DAG
# =============================================================
with DAG(
    dag_id="dataflow_vendas_diarias",
    default_args=default_args,
    description="Pipeline ETL diario de vendas — DataFlow Analytics",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "vendas", "etl", "aula04"],
) as dag:

    # --- Task 1: Extracao (PythonOperator) ---
    task_extrair = PythonOperator(
        task_id="extrair_dados",
        python_callable=extrair_dados,
    )

    # --- Task 2: Transformacao (PythonOperator) ---
    task_transformar = PythonOperator(
        task_id="transformar_dados",
        python_callable=transformar_dados,
    )

    # --- Task 3: Carga (PythonOperator) ---
    task_carregar = PythonOperator(
        task_id="carregar_dados",
        python_callable=carregar_dados,
    )

    # --- Task 4: Relatorio (PythonOperator) ---
    task_relatorio = PythonOperator(
        task_id="gerar_relatorio",
        python_callable=gerar_relatorio,
    )

    # --- Task 5: Notificacao (BashOperator com template Jinja) ---
    task_notificar = BashOperator(
        task_id="notificar_conclusao",
        bash_command='echo "[NOTIFICACAO] Pipeline de vendas concluido para {{ ds }}. Proximo: {{ next_ds }}"',
    )

    # --- Dependencias ---
    task_extrair >> task_transformar >> task_carregar >> task_relatorio >> task_notificar
