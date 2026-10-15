from datetime import datetime

from airflow.sdk import DAG
from dataform_common import run_dataform_tag

with DAG(
    dag_id="coarse_silver",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["coarse", "dataform"],
) as dag:
    run_dataform_tag("silver")
