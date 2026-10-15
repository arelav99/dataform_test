from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="smoke_test",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smoke"],
)
def smoke_test():
    @task
    def say_hello() -> str:
        return "Airflow is running"

    say_hello()


smoke_test()
