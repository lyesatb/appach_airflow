"""Traçabilité et journalisation des exécutions (TP5)."""

import json
from pathlib import Path

import psycopg2

from meteo.tp5.config import CHEMIN_PREUVES
from meteo.tp5.logging_utils import log_evenement


def enregistrer_anomalies(
    connexion_postgres: str,
    dag_run_id: str,
    anomalies: list[dict],
) -> None:
    """Trace les anomalies qualité sans charger les données finales."""
    if not anomalies:
        return

    requete = """
        INSERT INTO tp5.anomalies_qualite (
            dag_run_id, ville, champ, valeur, regle
        ) VALUES (%s, %s, %s, %s, %s)
    """
    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            for anomalie in anomalies:
                curseur.execute(
                    requete,
                    (
                        dag_run_id,
                        anomalie.get("ville"),
                        anomalie.get("champ"),
                        str(anomalie.get("valeur")),
                        anomalie.get("regle"),
                    ),
                )
        connexion.commit()

    log_evenement(
        "ERROR",
        "Anomalies qualité enregistrées",
        dag_run_id=dag_run_id,
        nb=len(anomalies),
    )


def journaliser_execution(
    connexion_postgres: str,
    dag_run_id: str,
    statut: str,
    nb_lignes: int,
    villes: list[str],
    chemin_archive: str | None,
    nb_anomalies: int = 0,
    mode_execution: str = "nominal",
) -> None:
    """Écrit une ligne dans tp5.suivi_ingestion."""
    requete = """
        INSERT INTO tp5.suivi_ingestion (
            dag_run_id, statut, nb_lignes_chargees, nb_anomalies,
            villes, chemin_archive, mode_execution
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            curseur.execute(
                requete,
                (
                    dag_run_id,
                    statut,
                    nb_lignes,
                    nb_anomalies,
                    villes,
                    chemin_archive,
                    mode_execution,
                ),
            )
        connexion.commit()

    log_evenement(
        "INFO",
        "Exécution journalisée",
        dag_run_id=dag_run_id,
        statut=statut,
        nb_lignes=nb_lignes,
    )


def exporter_etat_postgresql(connexion_postgres: str) -> dict:
    """Exporte l'état des tables pour preuve de rendu."""
    with psycopg2.connect(connexion_postgres) as connexion:
        with connexion.cursor() as curseur:
            curseur.execute(
                """
                SELECT ville, date_releve, temperature_celsius, dag_run_id
                FROM tp5.meteo_courant
                ORDER BY date_releve DESC, ville
                LIMIT 15
                """
            )
            meteo = [
                {
                    "ville": row[0],
                    "date_releve": row[1].isoformat() if row[1] else None,
                    "temperature_celsius": float(row[2]),
                    "dag_run_id": row[3],
                }
                for row in curseur.fetchall()
            ]

            curseur.execute(
                """
                SELECT dag_run_id, statut, nb_lignes_chargees, nb_anomalies,
                       mode_execution, date_ingestion
                FROM tp5.suivi_ingestion
                ORDER BY date_ingestion DESC
                LIMIT 10
                """
            )
            suivi = [
                {
                    "dag_run_id": row[0],
                    "statut": row[1],
                    "nb_lignes_chargees": row[2],
                    "nb_anomalies": row[3],
                    "mode_execution": row[4],
                    "date_ingestion": row[5].isoformat() if row[5] else None,
                }
                for row in curseur.fetchall()
            ]

            curseur.execute(
                """
                SELECT dag_run_id, ville, champ, valeur, regle, date_detection
                FROM tp5.anomalies_qualite
                ORDER BY date_detection DESC
                LIMIT 10
                """
            )
            anomalies = [
                {
                    "dag_run_id": row[0],
                    "ville": row[1],
                    "champ": row[2],
                    "valeur": row[3],
                    "regle": row[4],
                    "date_detection": row[5].isoformat() if row[5] else None,
                }
                for row in curseur.fetchall()
            ]

            curseur.execute("SELECT COUNT(*) FROM tp5.meteo_courant")
            total_meteo = curseur.fetchone()[0]

    return {
        "meteo_courant": meteo,
        "suivi_ingestion": suivi,
        "anomalies_qualite": anomalies,
        "total_lignes_meteo_courant": total_meteo,
    }


def exporter_preuve_fichier(
    connexion_postgres: str,
    nom_fichier: str,
    extra: dict | None = None,
) -> str:
    """Exporte un JSON de preuve dans livrables/tp5/preuves/."""
    CHEMIN_PREUVES.mkdir(parents=True, exist_ok=True)
    contenu = exporter_etat_postgresql(connexion_postgres)
    if extra:
        contenu.update(extra)

    chemin = CHEMIN_PREUVES / nom_fichier
    with chemin.open("w", encoding="utf-8") as fichier:
        json.dump(contenu, fichier, ensure_ascii=False, indent=2)

    log_evenement("INFO", "Preuve exportée", chemin=str(chemin))
    return str(chemin)
