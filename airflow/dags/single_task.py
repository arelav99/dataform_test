import json
import os
from datetime import datetime

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["DATAFORM_REGION"]
REPOSITORY_ID = os.environ["DATAFORM_REPOSITORY_ID"]
GIT_COMMITISH = os.environ.get("DATAFORM_GIT_COMMITISH", "single-task")
# The repository enforces strict act-as, so each invocation must name the service account it runs as.
# Use the Airflow key's own account, read from the file GOOGLE_APPLICATION_CREDENTIALS points to.
with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) as key_file:
    SERVICE_ACCOUNT = json.load(key_file)["client_email"]


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
    dag_id="single_task",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["single-task", "dataform"],
) as dag:
    preprocessing = trigger("run_preprocessing", "single_task_preprocessing")
    compile_dataform = DataformCreateCompilationResultOperator(
        task_id="compile",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        # default_database is the GCP project the tables are written to. Set here so the project ID stays out of the repo.
        compilation_result={
            "git_commitish": GIT_COMMITISH,
            "code_compilation_config": {"default_database": PROJECT_ID},
        },
    )
    # No included_tags or included_targets: Dataform runs every action, in ref() order.
    run_dataform = DataformCreateWorkflowInvocationOperator(
        task_id="run_dataform",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile')['name'] }}",
            "invocation_config": {"service_account": SERVICE_ACCOUNT},
        },
    )
    postprocessing = trigger("run_postprocessing", "single_task_postprocessing")

    preprocessing >> compile_dataform >> run_dataform >> postprocessing
