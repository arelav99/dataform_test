from datetime import datetime

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.sdk import DAG
from dataform_config import (
    DEFINITIONS_DIR,
    GIT_COMMITISH,
    PROJECT_ID,
    REGION,
    REPOSITORY_ID,
    SERVICE_ACCOUNT,
)
from dataform_graph import layers_in_order, load_actions

ACTIONS = load_actions(DEFINITIONS_DIR)

# One DAG per layer (tag), named coarse_<layer>. Each one compiles the branch once and runs
# every action carrying that tag as its own task, wired by ref() order within the layer.
# Dependencies on other layers are handled by the main coarse DAG, which runs layers in order.
for layer in layers_in_order(ACTIONS):
    dag_id = f"coarse_{layer}"
    layer_actions = [action for action in ACTIONS.values() if layer in action.tags]
    with DAG(
        dag_id=dag_id,
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["coarse", "dataform"],
    ) as dag:
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

        tasks = {}
        for action in layer_actions:
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

        for action in layer_actions:
            compile_dataform >> tasks[action.name]
            for parent in action.upstream:
                if parent in tasks:
                    tasks[parent] >> tasks[action.name]
    globals()[dag_id] = dag
