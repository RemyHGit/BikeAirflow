from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1), 
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# DAG definition
with DAG(
    dag_id="refresh_bike_db",
    default_args=default_args,
    description="Un DAG qui refresh la database des vélos chaque minute",
    schedule_interval="*/1 * * * *",  # every minute
    catchup=False,                     
) as dag:

    refresh_bike = BashOperator(
        task_id="refresh_bike_db",
        bash_command=r"""
          set -euxo pipefail
          cd "$AIRFLOW_HOME/plugins"
          python gather_bike_data.py
        """,         
        do_xcom_push=False,
    )

    refresh_bike 