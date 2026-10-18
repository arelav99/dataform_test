from datetime import datetime

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG
from dataform_config import (
    DEFINITIONS_DIR,
    GIT_COMMITISH,
    PROJECT_ID,
    REGION,
    REPOSITORY_ID,
    SERVICE_ACCOUNT,
)
from dataform_graph import load_actions

ACTIONS = load_actions(DEFINITIONS_DIR)


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
    dag_id="granular",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["granular", "dataform"],
) as dag:
    preprocessing = trigger("run_preprocessing", "granular_preprocessing")
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
            # The GCP project the tables are written to. Set here so the project ID stays out of the repo.
            "code_compilation_config": {"default_database": PROJECT_ID},
        },
    )
    postprocessing = trigger("run_postprocessing", "granular_postprocessing")

    # One invocation per action, built from the Dataform definitions. Airflow enforces the
    # ref() order between them; each invocation runs only its own target.
    tasks = {}
    for action in ACTIONS.values():
        tasks[action.name] = DataformCreateWorkflowInvocationOperator(
            task_id=f"run_{action.name}",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('compile')['name'] }}",
                "invocation_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "included_targets": [
                        {"database": PROJECT_ID, "schema": action.schema, "name": action.name}
                    ],
                },
            },
        )

    preprocessing >> compile_dataform
    for action in ACTIONS.values():
        compile_dataform >> tasks[action.name] >> postprocessing
        for parent in action.upstream:
            tasks[parent] >> tasks[action.name]
