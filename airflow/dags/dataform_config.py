import json
import os

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
REGION = os.environ["DATAFORM_REGION"]
REPOSITORY_ID = os.environ["DATAFORM_REPOSITORY_ID"]
DEFINITIONS_DIR = os.environ["DATAFORM_DEFINITIONS_DIR"]
GIT_COMMITISH = os.environ.get("DATAFORM_GIT_COMMITISH", "coarse")

# The repository enforces strict act-as, so each invocation must name the service account it runs as.
# Use the Airflow key's own account, read from the file GOOGLE_APPLICATION_CREDENTIALS points to.
with open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) as key_file:
    SERVICE_ACCOUNT = json.load(key_file)["client_email"]
