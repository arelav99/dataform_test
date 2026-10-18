import os
from pathlib import Path

from airflow.exceptions import AirflowException
from google.api_core import exceptions as gcp_exceptions
from google.auth.exceptions import DefaultCredentialsError

EXPECTED_ENV = ("GCP_PROJECT_ID", "DATAFORM_REGION", "DATAFORM_REPOSITORY_ID")
PLACEHOLDER = "REPLACE_WITH"


def assert_gcp_access() -> None:
    """Preflight check for the GCP connection. Run it before any task that calls Dataform.

    Checks, in order: config values, credential file presence, credential validity,
    and finally a real API call to confirm the Dataform repository is reachable.
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

    name = (
        f"projects/{os.environ['GCP_PROJECT_ID']}"
        f"/locations/{os.environ['DATAFORM_REGION']}"
        f"/repositories/{os.environ['DATAFORM_REPOSITORY_ID']}"
    )
    try:
        client = DataformHook().get_dataform_client()
        client.get_repository(request={"name": name})
    except gcp_exceptions.NotFound:
        raise AirflowException(
            f"Dataform repository {name} not found. Check GCP_PROJECT_ID, DATAFORM_REGION, "
            "and DATAFORM_REPOSITORY_ID in airflow/.env."
        ) from None
    except gcp_exceptions.Unauthenticated:
        raise AirflowException(
            f"GCP rejected the credentials at {creds_file}. Check the key is active and belongs to a valid service account."
        ) from None
    except gcp_exceptions.Forbidden:
        raise AirflowException(
            "The service account cannot read the Dataform repository. "
            "Grant it the Dataform Editor role (roles/dataform.editor)."
        ) from None
    except (DefaultCredentialsError, ValueError) as err:
        raise AirflowException(f"GCP credentials at {creds_file} are invalid: {err}") from None
