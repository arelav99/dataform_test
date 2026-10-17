# Local Airflow

Local Airflow 3.3.2 for testing project structures. It runs with Docker Compose using LocalExecutor and PostgreSQL. The DAGs trigger Dataform in GCP.

## Services

| Service | Role |
|---|---|
| `postgres` | Metadata database |
| `airflow-init` | One-off: migrates the DB, creates the admin user, fixes folder ownership |
| `airflow-apiserver` | Web UI and REST API (http://localhost:8080) |
| `airflow-scheduler` | Schedules DAG runs and executes tasks |
| `airflow-dag-processor` | Parses DAG files from `dags/` |
| `airflow-triggerer` | Runs deferrable operators |

Optional, under the `debug` profile: `airflow-cli` for running `airflow` commands inside the stack.

## Requirements

- Docker with Compose v2.
- On Windows: Docker Desktop with WSL integration enabled for your distro (Settings → Resources → WSL integration).
- At least 4 GB RAM and 2 CPUs for Docker.
- A GCP project with the Dataform, BigQuery, and Developer Connect APIs enabled.

## Setup

The `.env` file is created from `.env.example`. Fill in the GCP values:

```bash
cp .env.example .env   # skip if .env already exists
```

| Variable | Meaning |
|---|---|
| `GCP_PROJECT_ID` | GCP project that holds the datasets and the Dataform repository |
| `DATAFORM_REGION` | Region of the Dataform repository, for example `us-central1` |
| `DATAFORM_REPOSITORY_ID` | ID of the Dataform repository |
| `FERNET_KEY` | Generated key for encrypting connection secrets. Do not change after first start. |

`FERNET_KEY` can be regenerated with:

```bash
python3 -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

### GCP setup (once per project)

1. **Dataform repository.** In the console (BigQuery → Dataform), create a repository in `DATAFORM_REGION` and connect it to this repository's git remote. Set the default branch to `main`. Connecting a remote requires the Developer Connect API or an HTTPS/SSH secret.
2. **Raw tables.** Run `../gcp/seed_raw_tables.sql` in the BigQuery console. Use the location set in `defaultLocation` (`US` by default). The Dataform declarations read these tables.
3. **Airflow service account.** Create a service account and grant it the **Dataform Editor** role (`roles/dataform.editor`). Create a JSON key and save it as `secrets/gcp-sa.json`. The `secrets/` folder is gitignored.
4. **Dataform runtime permissions.** By default, Dataform runs BigQuery jobs as its service agent `service-PROJECT_NUMBER@gcp-sa-dataform.iam.gserviceaccount.com`. Grant that agent `roles/bigquery.dataEditor` and `roles/bigquery.jobUser` on the project.

The DAGs read `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/gcp-sa.json` and the default connection `google-cloud-platform://`, which is set in `docker-compose.yaml`.

## Run

From this `airflow/` directory:

```bash
# First start: initialize the database and create the admin user
docker compose up airflow-init

# Start everything in the background
docker compose up -d

# Check status
docker compose ps
```

Open http://localhost:8080 and log in with `airflow` / `airflow`.

DAGs in `dags/` are picked up automatically. DAGs are unpaused on creation (`DAGS_ARE_PAUSED_AT_CREATION=false`). `smoke_test` is a sample DAG that confirms the Airflow setup works without GCP.

Dataform compiles the git branch named in each DAG (`DATAFORM_GIT_COMMITISH`, default set per branch). Push your changes to that branch before you trigger a run.

## Common commands

```bash
docker compose logs -f airflow-scheduler   # follow scheduler logs
docker compose restart airflow-scheduler   # after changing config
docker compose run --rm airflow-cli airflow dags list
docker compose down                        # stop, keep metadata DB
docker compose down -v                     # stop and delete metadata DB
```

## Adding dependencies

Put packages in a custom image instead of `_PIP_ADDITIONAL_REQUIREMENTS`. Create a `Dockerfile` next to `docker-compose.yaml`:

```dockerfile
FROM apache/airflow:3.3.2
RUN pip install --no-cache-dir <package>
```

Then in `docker-compose.yaml`, comment out `image:` and uncomment `build: .` in `x-airflow-common`, and run `docker compose build`.

## Troubleshooting

- **Port 8080 in use:** set `AIRFLOW_PORT` in `.env`, for example `8081`.
- **Permission errors in `logs/` or `dags/`:** check that `AIRFLOW_UID` in `.env` matches `id -u`.
- **Scheduler unhealthy right after start:** the first start can take a minute while the DB initializes.
- **`DefaultCredentialsError` in a Dataform task:** `secrets/gcp-sa.json` is missing or not mounted. Check it exists on the host and restart the stack.
- **`PERMISSION_DENIED` from Dataform:** check the service account has Dataform Editor, and the Dataform service agent has the BigQuery roles from step 4.
- **Import error mentioning `GCP_PROJECT_ID`, `DATAFORM_REGION`, or `DATAFORM_REPOSITORY_ID`:** these are missing from `.env`. Add them and restart.

## Pipeline structure: `single-task`

One Dataform run. Airflow does not split the workflow by layer or by table. Dataform runs every action, in the order of the `ref()` dependencies.

Dataform (`definitions/`):

- `raw/`: declarations of the raw source tables. Not executed; the other tables read them.
- `bronze/`, `silver/`, `golden/`: the same tables as the other branches. No tags. Dependencies are only `ref()` calls.

Airflow (`dags/`):

```
single_task
  └─ run_preprocessing  → single_task_preprocessing   (dummy DAG)
  └─ compile            → compile the git branch
  └─ run_dataform       → one invocation, the whole workflow
  └─ run_postprocessing → single_task_postprocessing  (dummy DAG)
```

`single_task` is the only DAG that talks to Dataform. It triggers the preprocessing DAG and waits, compiles and runs the whole workflow, then triggers the postprocessing DAG and waits. Trigger it from the UI or with `docker compose run --rm airflow-cli airflow dags trigger single_task`.

It compiles the git branch named by `DATAFORM_GIT_COMMITISH` (default `single-task`). Push this branch before a run.
