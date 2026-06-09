"""
TP 2 — Préparer une ingestion API météo (Open-Meteo)

Workflow :
  recuperer_meteo_api >> preparer_donnees_meteo >> exporter_apercu_donnees

La récupération API et la transformation sont séparées (modules distincts).
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from meteo.api_open_meteo import recuperer_reponses_brutes
from meteo.transformation import transformer_reponses_brutes

CHEMIN_APERCU = Path("/opt/airflow/livrables/tp2/apercu_donnees.json")
CHEMIN_BRUT_APERCU = Path("/opt/airflow/livrables/tp2/donnees_brutes_apercu.json")


def tache_recuperer_meteo_api() -> list[dict]:
    """Tâche 1 — Appelle Open-Meteo et retourne les JSON bruts via XCom."""
    reponses_brutes = recuperer_reponses_brutes()
    print(f"Réponses API récupérées pour {len(reponses_brutes)} villes.")
    for reponse in reponses_brutes:
        print(f"  - {reponse['ville']} : OK")
    return reponses_brutes


def tache_preparer_donnees_meteo(**context) -> list[dict]:
    """Tâche 2 — Transforme les données brutes en structure exploitable."""
    reponses_brutes = context["ti"].xcom_pull(task_ids="recuperer_meteo_api")
    donnees_preparees = transformer_reponses_brutes(reponses_brutes)
    print(f"Données préparées : {len(donnees_preparees)} enregistrements.")
    for ligne in donnees_preparees:
        print(
            f"  - {ligne['ville']} : {ligne['temperature_celsius']}°C, "
            f"humidité {ligne['humidite_pct']}%"
        )
    return donnees_preparees


def tache_exporter_apercu_donnees(**context) -> None:
    """Tâche 3 — Exporte un aperçu des données brutes et préparées (livrable TP)."""
    reponses_brutes = context["ti"].xcom_pull(task_ids="recuperer_meteo_api")
    donnees_preparees = context["ti"].xcom_pull(task_ids="preparer_donnees_meteo")

    CHEMIN_APERCU.parent.mkdir(parents=True, exist_ok=True)
    CHEMIN_BRUT_APERCU.parent.mkdir(parents=True, exist_ok=True)

    with CHEMIN_APERCU.open("w", encoding="utf-8") as fichier:
        json.dump(donnees_preparees, fichier, ensure_ascii=False, indent=2)

    with CHEMIN_BRUT_APERCU.open("w", encoding="utf-8") as fichier:
        json.dump(reponses_brutes[0], fichier, ensure_ascii=False, indent=2)

    print(f"Aperçu données préparées : {CHEMIN_APERCU}")
    print(f"Aperçu données brutes (Paris) : {CHEMIN_BRUT_APERCU}")


default_args = {
    "owner": "lyesatb",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="tp2_ingestion_meteo",
    default_args=default_args,
    description="TP2 — Ingestion météo Open-Meteo (API + transformation)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["tp2", "meteo", "api"],
) as dag:

    recuperer_meteo_api = PythonOperator(
        task_id="recuperer_meteo_api",
        python_callable=tache_recuperer_meteo_api,
    )

    preparer_donnees_meteo = PythonOperator(
        task_id="preparer_donnees_meteo",
        python_callable=tache_preparer_donnees_meteo,
    )

    exporter_apercu_donnees = PythonOperator(
        task_id="exporter_apercu_donnees",
        python_callable=tache_exporter_apercu_donnees,
    )

    recuperer_meteo_api >> preparer_donnees_meteo >> exporter_apercu_donnees
