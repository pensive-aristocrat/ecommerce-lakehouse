"""Daily lakehouse pipeline: sources -> Bronze -> Silver -> Gold -> quality checks.

Every task runs a command with the PROJECT's own .venv, so Airflow and the
pipeline never share Python packages.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = os.environ.get("LAKEHOUSE_PROJECT", "/mnt/m/Ecommerce/ecommerce-lakehouse")
PY = f"{PROJECT}/.venv/bin/python"
DBT = f"{PROJECT}/.venv/bin/dbt"

default_args = {
    "retries": 2,                         # try a failed task 2 more times
    "retry_delay": timedelta(minutes=1),  # wait 1 minute between tries
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Rebuild Bronze, Silver and Gold from the sources, then validate.",
    schedule="0 2 * * *",                 # every day at 02:00
    start_date=datetime(2026, 11, 1),
    catchup=False,                        # do not run the days missed in the past
    max_active_runs=1,                    # never two runs at the same time
    default_args=default_args,
    tags=["lakehouse", "portfolio"],
) as dag:

    start_sources = BashOperator(
        task_id="start_sources",
        bash_command=(
            f"cd {PROJECT} && ./scripts/sources_up.sh && "
            "until docker exec src-oracle healthcheck.sh; do sleep 10; done"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    reset_lakehouse = BashOperator(
        task_id="reset_lakehouse",
        bash_command=(
            f"cd {PROJECT} && ./scripts/lakehouse_up.sh && "
            "until curl -s localhost:8080/v1/info | grep -q '\"starting\":false'; do sleep 5; done"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    bronze = BashOperator(
        task_id="bronze_ingest",
        bash_command=f"cd {PROJECT} && {PY} -m spark_jobs.bronze.ingest_sources",
    )

    stop_oracle = BashOperator(
        task_id="stop_oracle",
        bash_command="docker stop src-oracle",
    )

    silver = BashOperator(
        task_id="silver_build",
        bash_command=f"cd {PROJECT} && {PY} -m spark_jobs.silver.build_silver",
    )

    gold = BashOperator(
        task_id="gold_dbt_build",
        bash_command=f"cd {PROJECT}/dbt && {DBT} build --profiles-dir .",
    )

    validate = BashOperator(
        task_id="validate_gold",
        bash_command=f"cd {PROJECT} && {PY} scripts/validate_gold.py",
        retries=0,  # a failed quality check is a real answer; retrying will not change it
    )

    start_sources >> reset_lakehouse >> bronze >> [silver, stop_oracle]
    silver >> gold >> validate