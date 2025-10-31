from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


# Configuration de base du DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),  # date de démarrage du DAG
    "retries": 1,
    "retry_delay": timedelta(seconds=30),  # délai avant une nouvelle tentative
}

# Définition du DAG
with DAG(
    dag_id="refresh_bike_db",
    default_args=default_args,
    description="Un DAG qui refresh la database des vélos chaque minute",
    schedule_interval="*/1 * * * *",  # chaque minute
    catchup=False,                     # ne pas exécuter les tâches manquées
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