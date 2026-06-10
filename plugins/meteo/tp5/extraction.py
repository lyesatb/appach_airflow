"""Extraction des données Open-Meteo (TP5)."""

from meteo.api_open_meteo import recuperer_reponses_brutes
from meteo.tp5.logging_utils import log_evenement


def extraire_donnees_meteo(villes: list[dict]) -> list[dict]:
    """Appelle l'API Open-Meteo pour chaque ville configurée."""
    log_evenement("INFO", "Début extraction API", nb_villes=len(villes))
    reponses = recuperer_reponses_brutes(villes)
    log_evenement(
        "INFO",
        "Extraction terminée",
        villes=[r["ville"] for r in reponses],
    )
    return reponses
