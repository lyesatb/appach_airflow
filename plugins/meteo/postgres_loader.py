"""
Chargement PostgreSQL pour le pipeline météo (TP 3).
"""

from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

def _chemin_schema_sql() -> Path:
    for candidat in (
        Path("/opt/airflow/sql/tp3/schema.sql"),
        Path(__file__).resolve().parents[2] / "sql" / "tp3" / "schema.sql",
    ):
        if candidat.exists():
            return candidat
    raise FileNotFoundError("Fichier sql/tp3/schema.sql introuvable")


def initialiser_schema(connexion_postgres: str) -> None:
    """Crée le schéma et les tables si nécessaire."""
    sql = _chemin_schema_sql().read_text(encoding="utf-8")
    with psycopg2.connect(connexion_postgres) as connexion:
        connexion.autocommit = True
        with connexion.cursor() as curseur:
            curseur.execute(sql)


def charger_meteo_courant(connexion_postgres: str, donnees: list[dict]) -> int:
    """Insère ou met à jour les mesures météo dans tp3.meteo_courant."""
    if not donnees:
        return 0

    requete = """
        INSERT INTO tp3.meteo_courant (
            ville, latitude, longitude, date_releve,
            temperature_celsius, humidite_pct, vitesse_vent_kmh,
            code_meteo, source_api, date_ingestion
        ) VALUES %s
        ON CONFLICT (ville, date_releve) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            temperature_celsius = EXCLUDED.temperature_celsius,
            humidite_pct = EXCLUDED.humidite_pct,
            vitesse_vent_kmh = EXCLUDED.vitesse_vent_kmh,
            code_meteo = EXCLUDED.code_meteo,
            source_api = EXCLUDED.source_api,
            date_ingestion = EXCLUDED.date_ingestion
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
        )
        for ligne in donnees
    ]

    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            execute_values(curseur, requete, lignes)
        connexion.commit()

    return len(lignes)


def journaliser_ingestion(
    connexion_postgres: str,
    dag_run_id: str,
    nb_lignes: int,
    villes: list[str],
    statut: str = "success",
) -> None:
    """Écrit une ligne dans la table de suivi d'ingestion."""
    requete = """
        INSERT INTO tp3.suivi_ingestion (
            dag_run_id, nb_lignes_chargees, villes, statut
        ) VALUES (%s, %s, %s, %s)
    """
    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            curseur.execute(requete, (dag_run_id, nb_lignes, villes, statut))
        connexion.commit()


def exporter_preuve_chargement(connexion_postgres: str) -> dict:
    """Retourne un aperçu des données chargées (preuve du TP)."""
    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            curseur.execute(
                """
                SELECT ville, date_releve, temperature_celsius, humidite_pct
                FROM tp3.meteo_courant
                ORDER BY date_releve DESC, ville
                LIMIT 10
                """
            )
            meteo = [
                {
                    "ville": ligne[0],
                    "date_releve": ligne[1].isoformat() if ligne[1] else None,
                    "temperature_celsius": float(ligne[2]),
                    "humidite_pct": ligne[3],
                }
                for ligne in curseur.fetchall()
            ]

            curseur.execute(
                """
                SELECT dag_run_id, nb_lignes_chargees, villes, statut, date_ingestion
                FROM tp3.suivi_ingestion
                ORDER BY date_ingestion DESC
                LIMIT 5
                """
            )
            suivi = [
                {
                    "dag_run_id": ligne[0],
                    "nb_lignes_chargees": ligne[1],
                    "villes": ligne[2],
                    "statut": ligne[3],
                    "date_ingestion": ligne[4].isoformat() if ligne[4] else None,
                }
                for ligne in curseur.fetchall()
            ]

    return {"meteo_courant": meteo, "suivi_ingestion": suivi}
