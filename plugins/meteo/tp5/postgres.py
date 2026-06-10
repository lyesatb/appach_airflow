"""Chargement PostgreSQL idempotent (TP5)."""

from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from meteo.tp5.config import CHEMIN_SCHEMA_SQL
from meteo.tp5.logging_utils import log_evenement


def _chemin_schema() -> Path:
    if CHEMIN_SCHEMA_SQL.exists():
        return CHEMIN_SCHEMA_SQL
    fallback = Path(__file__).resolve().parents[3] / "sql" / "tp5" / "schema.sql"
    if fallback.exists():
        return fallback
    raise FileNotFoundError("sql/tp5/schema.sql introuvable")


def initialiser_schema(connexion_postgres: str) -> None:
    """Crée le schéma tp5 et les tables."""
    sql = _chemin_schema().read_text(encoding="utf-8")
    with psycopg2.connect(connexion_postgres) as connexion:
        connexion.autocommit = True
        with connexion.cursor() as curseur:
            curseur.execute(sql)


def charger_meteo_idempotent(connexion_postgres: str, donnees: list[dict]) -> int:
    """
    Charge les données en mode idempotent (UPSERT sur ville + date_releve).

    Une relance du DAG ne crée pas de doublons.
    """
    if not donnees:
        return 0

    requete = """
        INSERT INTO tp5.meteo_courant (
            ville, latitude, longitude, date_releve,
            temperature_celsius, humidite_pct, vitesse_vent_kmh,
            code_meteo, source_api, date_ingestion, dag_run_id
        ) VALUES %s
        ON CONFLICT (ville, date_releve) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            temperature_celsius = EXCLUDED.temperature_celsius,
            humidite_pct = EXCLUDED.humidite_pct,
            vitesse_vent_kmh = EXCLUDED.vitesse_vent_kmh,
            code_meteo = EXCLUDED.code_meteo,
            source_api = EXCLUDED.source_api,
            date_ingestion = EXCLUDED.date_ingestion,
            dag_run_id = EXCLUDED.dag_run_id
    """

    lignes = [
        (
            ligne["ville"],
            ligne["latitude"],
            ligne["longitude"],
            datetime.fromisoformat(ligne["date_releve"]),
            ligne["temperature_celsius"],
            ligne["humidite_pct"],
            ligne["vitesse_vent_kmh"],
            ligne["code_meteo"],
            ligne["source_api"],
            datetime.fromisoformat(ligne["date_ingestion"]),
            ligne.get("dag_run_id"),
        )
        for ligne in donnees
    ]

    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            execute_values(curseur, requete, lignes)
        connexion.commit()

    log_evenement("INFO", "Chargement PostgreSQL idempotent", nb_lignes=len(lignes))
    return len(lignes)


def compter_lignes_meteo(connexion_postgres: str) -> dict:
    """Compte les lignes pour preuve d'idempotence."""
    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            curseur.execute("SELECT COUNT(*) FROM tp5.meteo_courant")
            total = curseur.fetchone()[0]
            curseur.execute(
                """
                SELECT ville, date_releve, COUNT(*) AS nb
                FROM tp5.meteo_courant
                GROUP BY ville, date_releve
                HAVING COUNT(*) > 1
                """
            )
            doublons = curseur.fetchall()
    return {"total_lignes": total, "doublons_detectes": len(doublons)}
