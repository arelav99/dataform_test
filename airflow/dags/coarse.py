from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from dataform_config import DEFINITIONS_DIR
from dataform_graph import layers_in_order, load_actions

LAYERS = layers_in_order(load_actions(DEFINITIONS_DIR))


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
    dag_id="coarse",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["coarse", "dataform"],
) as dag:
    preprocessing = trigger("run_preprocessing", "coarse_preprocessing")
    layer_runs = [trigger(f"run_{layer}", f"coarse_{layer}") for layer in LAYERS]
    postprocessing = trigger("run_postprocessing", "coarse_postprocessing")

    # Layers run one after another, in the order they depend on each other.
    steps = [preprocessing, *layer_runs, postprocessing]
    for before, after in zip(steps, steps[1:]):
        before >> after
