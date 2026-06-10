"""Archivage des données brutes (TP5)."""

import json
from pathlib import Path

from meteo.tp5.config import CHEMIN_ARCHIVES
from meteo.tp5.logging_utils import log_evenement


def archiver_donnees_brutes(
    reponses_brutes: list[dict],
    dag_run_id: str,
    repertoire: Path | None = None,
) -> str:
    """
    Archive le JSON brut par exécution DAG (traçabilité / rejeu).

    Returns:
        Chemin du fichier archive créé.
    """
    base = repertoire or CHEMIN_ARCHIVES
    dossier_run = base / dag_run_id.replace(":", "_")
    dossier_run.mkdir(parents=True, exist_ok=True)

    chemin_fichier = dossier_run / "donnees_brutes.json"
    with chemin_fichier.open("w", encoding="utf-8") as fichier:
        json.dump(reponses_brutes, fichier, ensure_ascii=False, indent=2)

    log_evenement("INFO", "Archive créée", chemin=str(chemin_fichier))
    return str(chemin_fichier)
