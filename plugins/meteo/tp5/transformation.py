"""Transformation des données météo (TP5)."""

from meteo.tp5.logging_utils import log_evenement
from meteo.transformation import transformer_reponses_brutes


def transformer_donnees(
    reponses_brutes: list[dict],
    simuler_anomalie: bool = False,
) -> list[dict]:
    """
    Transforme les réponses API en enregistrements normalisés.

    Si simuler_anomalie=True, injecte une température invalide (cas test qualité).
    """
    donnees = transformer_reponses_brutes(reponses_brutes)

    if simuler_anomalie and donnees:
        donnees = [dict(ligne) for ligne in donnees]
        ville_cible = donnees[0]["ville"]
        donnees[0]["temperature_celsius"] = 999.0
        log_evenement(
            "WARNING",
            "Simulation anomalie qualité activée",
            ville=ville_cible,
            temperature_injectee=999.0,
        )

    log_evenement("INFO", "Transformation terminée", nb_lignes=len(donnees))
    return donnees
