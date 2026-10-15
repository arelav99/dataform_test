import os

from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["DATAFORM_REGION"]
REPOSITORY_ID = os.environ["DATAFORM_REPOSITORY_ID"]
GIT_COMMITISH = os.environ.get("DATAFORM_GIT_COMMITISH", "coarse")


def run_dataform_tag(tag: str) -> None:
    """Compile the git branch and run every Dataform action with the given tag."""
    compile_task = DataformCreateCompilationResultOperator(
        task_id="compile",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={"git_commitish": GIT_COMMITISH},
    )
    run_task = DataformCreateWorkflowInvocationOperator(
        task_id=f"run_{tag}",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile')['name'] }}",
            "invocation_config": {"included_tags": [tag]},
        },
    )
    compile_task >> run_task
