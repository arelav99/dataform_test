import os
from pathlib import Path

from airflow.exceptions import AirflowException

EXPECTED_ENV = ("GCP_PROJECT_ID", "DATAFORM_REGION", "DATAFORM_REPOSITORY_ID")
PLACEHOLDER = "REPLACE_WITH"


def assert_gcp_access() -> None:
    """Preflight check for the Dataform connection. Run it before any task that calls GCP.

    Fails with a readable message when config or credentials are missing.
    Otherwise it makes one real API call to confirm the repository is reachable.
    """
    problems = []
    for key in EXPECTED_ENV:
        value = os.environ.get(key, "")
        if not value or PLACEHOLDER in value:
            problems.append(f"{key} is missing or still a placeholder in airflow/.env")

    creds_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_file:
        problems.append("GOOGLE_APPLICATION_CREDENTIALS is not set")
    elif not Path(creds_file).is_file():
        problems.append(
            f"service account key missing at {creds_file}, see the GCP setup section in README.md"
        )

    if problems:
        raise AirflowException("GCP setup incomplete:\n- " + "\n- ".join(problems))

    from airflow.providers.google.cloud.hooks.dataform import DataformHook

    client = DataformHook().get_dataform_client()
    name = (
        f"projects/{os.environ['GCP_PROJECT_ID']}"
        f"/locations/{os.environ['DATAFORM_REGION']}"
        f"/repositories/{os.environ['DATAFORM_REPOSITORY_ID']}"
    )
    client.get_repository(request={"name": name})
