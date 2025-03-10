from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

dag = DAG(
    "dbt_run",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="DBT_PROFILES_DIR=/opt/dbt dbt run --project-dir /opt/dbt",
    dag=dag,
)

dbt_run
