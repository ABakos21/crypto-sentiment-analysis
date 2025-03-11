from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default arguments for tasks
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "depends_on_past": False,
}

# Define the DAG
dag = DAG(
    dag_id="dbt_run",
    default_args=default_args,
    description="Run dbt models using BashOperator",
    schedule_interval="@daily",
    catchup=False,
)

# Task to run dbt
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="DBT_PROFILES_DIR=/opt/dbt dbt run --project-dir /opt/dbt",
    dag=dag,
    env={"DBT_PROFILES_DIR": "/opt/dbt"}
)


# Define DAG structure
dbt_run
