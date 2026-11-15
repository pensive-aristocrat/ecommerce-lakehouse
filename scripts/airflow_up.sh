#!/usr/bin/env bash
# Start Airflow (all components, SQLite database) with the UI on port 8085.
set -euo pipefail

PROJECT="$(cd "$(dirname "$0")/.." && pwd)"
export AIRFLOW_HOME="$HOME/airflow"                     # settings + SQLite, inside Linux
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__API__PORT=8085                          # 8080 is taken by Trino
export AIRFLOW__API__BASE_URL=http://localhost:8085
export LAKEHOUSE_PROJECT="$PROJECT"                     # read by the DAG
export PATH="$HOME/airflow-venv/bin:$PATH"              # Airflow's own environment

airflow standalone