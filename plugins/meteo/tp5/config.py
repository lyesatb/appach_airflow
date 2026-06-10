"""Configuration centralisée du pipeline TP5."""

from pathlib import Path

from meteo.utils import normaliser_villes as _normaliser_villes

VILLES_PAR_DEFAUT = [
    {"ville": "Paris", "latitude": 48.86, "longitude": 2.35},
    {"ville": "Lyon", "latitude": 45.76, "longitude": 4.84},
    {"ville": "Marseille", "latitude": 43.30, "longitude": 5.37},
]

CONNEXION_POSTGRES_DEFAUT = "postgresql://airflow:airflow@postgres:5432/airflow"
CONNEXION_AIRFLOW_ID = "postgres_default"

# Variables Airflow documentées (optionnelles — Params utilisés par défaut)
VAR_VILLES = "tp5_villes_defaut"
VAR_CONNEXION = "tp5_connexion_postgres"

CHEMIN_ARCHIVES = Path("/opt/airflow/livrables/tp5/archives")
CHEMIN_PREUVES = Path("/opt/airflow/livrables/tp5/preuves")
CHEMIN_SCHEMA_SQL = Path("/opt/airflow/sql/tp5/schema.sql")

# Contrôles qualité
TEMPERATURE_MIN = -50.0
TEMPERATURE_MAX = 60.0
HUMIDITE_MIN = 0
HUMIDITE_MAX = 100
VENT_MIN = 0.0


def normaliser_villes(villes) -> list[dict]:
    """Normalise le paramètre villes depuis Airflow."""
    return _normaliser_villes(villes, VILLES_PAR_DEFAUT)
