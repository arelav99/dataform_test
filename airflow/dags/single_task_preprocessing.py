from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="single_task_preprocessing",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["single-task", "dummy"],
) as dag:
    EmptyOperator(task_id="preprocessing")
