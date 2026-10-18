from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from gcp_check import assert_gcp_access

with DAG(
    dag_id="granular_preprocessing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["granular", "preflight"],
) as dag:
    PythonOperator(task_id="check_gcp_access", python_callable=assert_gcp_access)
