from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG


def trigger(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
    )


with DAG(
    dag_id="coarse_orchestrator",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["coarse", "orchestrator"],
) as dag:
    preprocessing = trigger("run_preprocessing", "coarse_preprocessing")
    raw_bronze = trigger("run_raw_bronze", "coarse_raw_bronze")
    silver = trigger("run_silver", "coarse_silver")
    golden = trigger("run_golden", "coarse_golden")
    postprocessing = trigger("run_postprocessing", "coarse_postprocessing")

    preprocessing >> raw_bronze >> silver >> golden >> postprocessing
