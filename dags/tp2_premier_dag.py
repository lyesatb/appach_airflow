"""
TP 2 — Créer un premier DAG Airflow

Workflow simple en 3 étapes :
  extraire_donnees >> transformer_donnees >> charger_resultat
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def extraire():
    """Simule la récupération de données brutes."""
    donnees = ["alice", "bob", "charlie"]
    print(f"Données extraites : {donnees}")
    return donnees


def transformer(**context):
    """Transforme les données extraites (mise en majuscules)."""
    donnees = context["ti"].xcom_pull(task_ids="extraire_donnees")
    transformees = [nom.upper() for nom in donnees]
    print(f"Données transformées : {transformees}")
    return transformees


def charger(**context):
    """Affiche le résultat final (simulation d'un chargement)."""
    resultat = context["ti"].xcom_pull(task_ids="transformer_donnees")
    print(f"Résultat chargé avec succès : {resultat}")
    print("Pipeline terminé.")


default_args = {
    "owner": "lyesatb",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="tp2_premier_dag",
    default_args=default_args,
    description="Premier DAG Airflow — extraction, transformation et chargement",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp2", "formation"],
) as dag:

    tache_extraire = PythonOperator(
        task_id="extraire_donnees",
        python_callable=extraire,
    )

    tache_transformer = PythonOperator(
        task_id="transformer_donnees",
        python_callable=transformer,
    )

    tache_charger = PythonOperator(
        task_id="charger_resultat",
        python_callable=charger,
    )

    tache_extraire >> tache_transformer >> tache_charger
