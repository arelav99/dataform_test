from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="granular_postprocessing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["granular", "dummy"],
) as dag:
    EmptyOperator(task_id="postprocessing")
