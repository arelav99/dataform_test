import os
from datetime import datetime

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["DATAFORM_REGION"]
REPOSITORY_ID = os.environ["DATAFORM_REPOSITORY_ID"]
GIT_COMMITISH = os.environ.get("DATAFORM_GIT_COMMITISH", "granular")

# One task per Dataform action: name -> (schema, upstream actions)
ACTIONS = {
    "bronze_customers": ("bronze", []),
    "bronze_orders": ("bronze", []),
    "silver_customers": ("silver", ["bronze_customers"]),
    "silver_orders": ("silver", ["bronze_orders"]),
    "golden_customer_orders": ("golden", ["silver_customers", "silver_orders"]),
}

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
    for name, (schema, upstream) in ACTIONS.items():
        tasks[name] = DataformCreateWorkflowInvocationOperator(
            task_id=f"run_{name}",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('compile_dataform')['name'] }}",
                "invocation_config": {"included_targets": [{"schema": schema, "name": name}]},
            },
        )
        compile_dataform >> tasks[name] >> postprocessing
        for parent in upstream:
            tasks[parent] >> tasks[name]
