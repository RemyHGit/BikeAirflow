from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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

    # Tâche 1 : Exécution d’une commande simple
    refresh_bike = BashOperator(
        task_id="refresh_bike_db",
        bash_command="python $AIRFLOW_HOME/plugins/test2.py"
        
    )

    # Tu peux ajouter d'autres tâches ici (PythonOperator, etc.)
    refresh_bike




