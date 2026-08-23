"""
DAG: Sensor de Arquivo de Vendas da DataFlow Analytics
======================================================
Descrição: Espera o arquivo de vendas do parceiro chegar no diretório
           de incoming antes de iniciar o processamento.
Autor: Carlos Mendes (Engenheiro de Dados Sênior)
Padrão: FileSensor → Processamento → Notificação
Schedule: Todo dia às 6h (horário que o parceiro começa a entregar)

Cenário de Negócio:
    - Parceiro A entrega dados entre 6h e 6h30
    - Pipeline deve esperar o arquivo chegar
    - Se não chegar em 10 minutos → timeout (investigar!)
    - Sensor verifica a cada 30 segundos
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os


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
# 2. FUNÇÕES — Lógica de negócio
# ============================================================
def processar_vendas(**context):
    """
    Processa o arquivo de vendas após o sensor confirmar sua existência.
    Em produção, aqui teria leitura com PySpark e escrita no data lake.
    """
    execution_date = context["ds"]
    ds_nodash = context["ds_nodash"]
    filepath = f"/opt/airflow/data/incoming/vendas_{ds_nodash}.csv"

    print(f"{'=' * 50}")
    print(f"📦 PROCESSAMENTO DE VENDAS")
    print(f"{'=' * 50}")
    print(f"Data de referência: {execution_date}")
    print(f"Arquivo detectado: {filepath}")

    if os.path.exists(filepath):
        tamanho = os.path.getsize(filepath)
        print(f"Tamanho do arquivo: {tamanho:,} bytes")
    else:
        print(f"⚠️ Arquivo não encontrado!")
        return

    print(f"")
    print(f"Etapas de processamento:")
    print(f"  1. Leitura do CSV...")
    print(f"  2. Validação de schema...")
    print(f"  3. Limpeza de dados...")
    print(f"  4. Escrita na camada Bronze...")
    print(f"")
    print(f"✅ Processamento concluído!")
    print(f"   Destino: datalake/bronze/vendas/{execution_date}/")

    context["ti"].xcom_push(key="arquivo_processado", value=filepath)
    context["ti"].xcom_push(key="tamanho_bytes", value=tamanho)


# ============================================================
# 3. DAG — Pipeline com FileSensor
# ============================================================
with DAG(
    dag_id="dataflow_sensor_arquivo_v1",
    default_args=default_args,
    description="Espera arquivo de vendas chegar antes de processar",
    schedule_interval="0 6 * * *",  # Todo dia às 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "sensor", "arquivo", "aula05"],
) as dag:

    # ========================================================
    # 4. TASKS
    # ========================================================

    # Task 1: SENSOR — Espera o arquivo de vendas do dia chegar
    # O {{ ds_nodash }} é um template Jinja que o Airflow substitui
    # pela data de execução no formato YYYYMMDD (ex: 20240115)
    esperar_arquivo = FileSensor(
        task_id="esperar_arquivo_vendas",
        filepath="/opt/airflow/data/incoming/vendas_{{ ds_nodash }}.csv",
        poke_interval=30,       # Verifica a cada 30 segundos
        timeout=600,            # Timeout: 10 minutos (600 segundos)
        mode="poke",            # Mantém worker slot ocupado (espera curta)
        soft_fail=False,        # Timeout = FAILED (arquivo é obrigatório)
        fs_conn_id="fs_default",
    )

    # Task 2: Processar o arquivo após sensor confirmar existência
    processar = PythonOperator(
        task_id="processar_vendas",
        python_callable=processar_vendas,
    )

    # Task 3: Notificar equipe sobre conclusão
    notificar = BashOperator(
        task_id="notificar_conclusao",
        bash_command=(
            'echo "✅ [{{ ds }}] Pipeline de vendas concluído com sucesso! '
            'Arquivo vendas_{{ ds_nodash }}.csv processado e carregado '
            'no data lake."'
        ),
    )

    # ========================================================
    # 5. DEPENDÊNCIAS
    # ========================================================
    esperar_arquivo >> processar >> notificar
