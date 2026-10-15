import os
from datetime import datetime

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG
from dataform_graph import load_actions

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["DATAFORM_REGION"]
REPOSITORY_ID = os.environ["DATAFORM_REPOSITORY_ID"]
GIT_COMMITISH = os.environ.get("DATAFORM_GIT_COMMITISH", "granular")
DEFINITIONS_DIR = os.environ["DATAFORM_DEFINITIONS_DIR"]

ACTIONS = load_actions(DEFINITIONS_DIR)

with DAG(
    dag_id="granular_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["granular", "dataform"],
) as dag:
    preprocessing = EmptyOperator(task_id="preprocessing")
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": GIT_COMMITISH},
    )
    postprocessing = EmptyOperator(task_id="postprocessing")

    preprocessing >> compile_dataform

    tasks = {}
    for action in ACTIONS.values():
        tasks[action.name] = DataformCreateWorkflowInvocationOperator(
            task_id=f"run_{action.name}",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
                "invocation_config": {"included_targets": [{"schema": action.schema, "name": action.name}]},
            },
        )

    for action in ACTIONS.values():
        compile_dataform >> tasks[action.name] >> postprocessing
        for parent in action.upstream:
            tasks[parent] >> tasks[action.name]
