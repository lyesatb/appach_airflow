"""
TP 3 — Pipeline complet API → transformation → PostgreSQL

Workflow :
  recuperer_meteo_api
      >> transformer_donnees_meteo
      >> charger_postgresql
      >> journaliser_ingestion
      >> exporter_preuve_chargement

DAG paramétrable via Params Airflow (villes, connexion PostgreSQL).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from meteo.api_open_meteo import recuperer_reponses_brutes
from meteo.postgres_loader import (
    charger_meteo_courant,
    exporter_preuve_chargement,
    initialiser_schema,
    journaliser_ingestion,
)
from meteo.transformation import transformer_reponses_brutes

VILLES_PAR_DEFAUT = [
    {"ville": "Paris", "latitude": 48.86, "longitude": 2.35},
    {"ville": "Lyon", "latitude": 45.76, "longitude": 4.84},
    {"ville": "Marseille", "latitude": 43.30, "longitude": 5.37},
]

CONNEXION_POSTGRES_DEFAUT = "postgresql://airflow:airflow@postgres:5432/airflow"
CHEMIN_PREUVE = Path("/opt/airflow/livrables/tp3/preuve_chargement.json")


def _obtenir_villes(context: dict) -> list[dict]:
    """Villes depuis les params du DAG ou la config du trigger (dag_run.conf)."""
    villes = context["params"]["villes"]
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    return conf.get("villes", villes)


def _obtenir_connexion_postgres(context: dict) -> str:
    """Connexion PostgreSQL paramétrable."""
    connexion = context["params"]["connexion_postgres"]
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    return conf.get("connexion_postgres", connexion)


def tache_recuperer_meteo_api(**context) -> list[dict]:
    """Tâche 1 — Récupération Open-Meteo (données brutes)."""
    villes = _obtenir_villes(context)
    reponses = recuperer_reponses_brutes(villes)
    print(f"API : {len(reponses)} villes récupérées → {[v['ville'] for v in reponses]}")
    return reponses


def tache_transformer_donnees_meteo(**context) -> list[dict]:
    """Tâche 2 — Transformation vers structure table cible."""
    reponses_brutes = context["ti"].xcom_pull(task_ids="recuperer_meteo_api")
    donnees = transformer_reponses_brutes(reponses_brutes)
    for ligne in donnees:
        print(f"  {ligne['ville']} : {ligne['temperature_celsius']}°C")
    return donnees


def tache_charger_postgresql(**context) -> int:
    """Tâche 3 — Chargement dans PostgreSQL."""
    connexion = _obtenir_connexion_postgres(context)
    donnees = context["ti"].xcom_pull(task_ids="transformer_donnees_meteo")

    initialiser_schema(connexion)
    nb_lignes = charger_meteo_courant(connexion, donnees)
    print(f"PostgreSQL : {nb_lignes} ligne(s) chargée(s) dans tp3.meteo_courant")
    return nb_lignes


def tache_journaliser_ingestion(**context) -> None:
    """Tâche 4 — Écriture dans la table de suivi d'ingestion."""
    connexion = _obtenir_connexion_postgres(context)
    villes = [v["ville"] for v in _obtenir_villes(context)]
    nb_lignes = context["ti"].xcom_pull(task_ids="charger_postgresql")
    dag_run_id = context["dag_run"].run_id

    journaliser_ingestion(connexion, dag_run_id, nb_lignes, villes, statut="success")
    print(f"Suivi : ingestion journalisée pour run {dag_run_id}")


def tache_exporter_preuve_chargement(**context) -> None:
    """Tâche 5 — Export de la preuve de chargement (livrable TP)."""
    connexion = _obtenir_connexion_postgres(context)
    preuve = exporter_preuve_chargement(connexion)

    CHEMIN_PREUVE.parent.mkdir(parents=True, exist_ok=True)
    with CHEMIN_PREUVE.open("w", encoding="utf-8") as fichier:
        json.dump(preuve, fichier, ensure_ascii=False, indent=2)

    print(f"Preuve exportée : {CHEMIN_PREUVE}")


default_args = {
    "owner": "lyesatb",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tp3_pipeline_meteo_postgresql",
    default_args=default_args,
    description="TP3 — Pipeline Open-Meteo → transformation → PostgreSQL",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp3", "meteo", "postgresql"],
    params={
        "villes": Param(
            default=VILLES_PAR_DEFAUT,
            type="array",
            description="Villes à ingérer (nom + latitude + longitude)",
        ),
        "connexion_postgres": Param(
            default=CONNEXION_POSTGRES_DEFAUT,
            type="string",
            description="URL de connexion PostgreSQL",
        ),
    },
) as dag:

    recuperer_meteo_api = PythonOperator(
        task_id="recuperer_meteo_api",
        python_callable=tache_recuperer_meteo_api,
    )

    transformer_donnees_meteo = PythonOperator(
        task_id="transformer_donnees_meteo",
        python_callable=tache_transformer_donnees_meteo,
    )

    charger_postgresql = PythonOperator(
        task_id="charger_postgresql",
        python_callable=tache_charger_postgresql,
    )

    journaliser_ingestion_task = PythonOperator(
        task_id="journaliser_ingestion",
        python_callable=tache_journaliser_ingestion,
    )

    exporter_preuve_chargement_task = PythonOperator(
        task_id="exporter_preuve_chargement",
        python_callable=tache_exporter_preuve_chargement,
    )

    (
        recuperer_meteo_api
        >> transformer_donnees_meteo
        >> charger_postgresql
        >> journaliser_ingestion_task
        >> exporter_preuve_chargement_task
    )
