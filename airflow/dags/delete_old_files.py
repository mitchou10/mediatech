from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "DELETE_OLD_FILES",
    default_args=default_args,
    schedule="0 7 * * 0",
    catchup=False,
    max_active_runs=1,
    description="Delete some files older than $RETENTION_DAYS from various directories",
    tags=["periodically", "delete old files"],
) as dag:
    delete_old_files = BashOperator(
        task_id="delete_old_files",
        bash_command="{{ 'cd /tmp/mediatech && bash scripts/delete_old_files.sh' }}",
    )

    delete_old_files
