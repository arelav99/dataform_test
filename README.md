# dataform_test

A small Dataform project on BigQuery, orchestrated by a local Airflow instance. This `main` branch holds only this overview. Each approach lives on its own branch with its own DAGs, definitions, and setup steps. Start with the branch that matches what you need.

## The three approaches

Each branch uses the same Dataform project and differs in how Airflow hands the work to Dataform. In `single-task`, one Dataform invocation runs the whole workflow, and Airflow only adds a preflight check and placeholder pre- and postprocessing DAGs. In `granular`, Airflow reads the Dataform definitions and creates one task per action, with each `ref()` turned into a task dependency, so every table is its own task. In `coarse`, Airflow creates one DAG per layer (tag), with the main DAG running the layers in dependency order, and inside each layer every table is its own task.

## Comparison

Measured times are from one run per branch on the same five-table sample data, so treat them as a rough guide. Infrastructure and UI rows describe what each branch needs to keep working.

| Criterion | `single-task` | `granular` | `coarse` |
|---|---|---|---|
| How Dataform is called | 1 invocation for the whole workflow | 1 invocation per action | 1 invocation per action  |
| Compile calls per run | 1 | 1 | 1 per layer (3 here) |
| Measured wall time (sample) | 62 s | 77 s | 127 s |
| Parsing the definitions in Airflow | None | Regex over `definitions/` at DAG parse | Regex over `definitions/` at DAG parse, plus layer ordering |
| Infrastructure to keep in sync | DAG files only; the definitions stay in Dataform | Definitions must be visible to the DAG processor | Definitions must be visible to the DAG processor |
| Dataform semantics preserved | Full: Dataform resolves the graph itself | Partial: the regex graph can miss JavaScript refs and config dependencies | Partial: same as `granular` |
| Airflow UI: what you see | One Dataform task; table detail only in Dataform's logs | Every table is a vertex in one DAG | Layers in the main DAG, every table a vertex inside its layer DAG |
| Overhead per run | Lowest | One invocation and one poll per table | Same as `granular`, plus one compile and one trigger per layer |
| Deploying Airflow on GCP (Cloud Composer) | Only the DAG files need to be in the bucket | DAG files plus `definitions/` (or a generated graph file) | DAG files plus `definitions/` (or a generated graph file) |

## Keeping the SQLX files in sync

`granular` and `coarse` build their DAGs from the SQLX files in `definitions/`. Whatever Airflow reads has to match what is committed in Dataform, in any form: the local mount, a generated graph file, or a copy in the Composer bucket on GCP. If they drift, the DAGs show tasks that no longer match the Dataform graph. `single-task` is not affected, because Airflow never reads the SQLX files.

The most preferable way to keep them in sync is a GitHub Actions workflow that runs on every push. It regenerates the graph or copies the definitions, commits or uploads the result, and fails the build when they don't match. That keeps the definitions and the DAGs in step automatically, with no manual copying, and the same job can sync to the Composer bucket once Airflow runs on GCP.
