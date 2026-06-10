"""Contrôles qualité des données météo (TP5)."""

from meteo.tp5.config import (
    HUMIDITE_MAX,
    HUMIDITE_MIN,
    TEMPERATURE_MAX,
    TEMPERATURE_MIN,
    VENT_MIN,
)
from meteo.tp5.logging_utils import log_evenement


def _verifier_ligne(ligne: dict) -> list[dict]:
    """Retourne la liste des anomalies détectées pour une ligne."""
    anomalies = []

    temp = ligne.get("temperature_celsius")
    if temp is None or not (TEMPERATURE_MIN <= float(temp) <= TEMPERATURE_MAX):
        anomalies.append(
            {
                "ville": ligne.get("ville"),
                "champ": "temperature_celsius",
                "valeur": temp,
                "regle": f"doit être entre {TEMPERATURE_MIN} et {TEMPERATURE_MAX}",
            }
        )

    humidite = ligne.get("humidite_pct")
    if humidite is None or not (HUMIDITE_MIN <= int(humidite) <= HUMIDITE_MAX):
        anomalies.append(
            {
                "ville": ligne.get("ville"),
                "champ": "humidite_pct",
                "valeur": humidite,
                "regle": f"doit être entre {HUMIDITE_MIN} et {HUMIDITE_MAX}",
            }
        )

    vent = ligne.get("vitesse_vent_kmh")
    if vent is None or float(vent) < VENT_MIN:
        anomalies.append(
            {
                "ville": ligne.get("ville"),
                "champ": "vitesse_vent_kmh",
                "valeur": vent,
                "regle": f"doit être >= {VENT_MIN}",
            }
        )

    if not ligne.get("ville"):
        anomalies.append(
            {
                "ville": None,
                "champ": "ville",
                "valeur": ligne.get("ville"),
                "regle": "obligatoire",
            }
        )

    return anomalies


def controler_qualite(donnees: list[dict]) -> dict:
    """
    Contrôle qualité sur l'ensemble des enregistrements.

    Returns:
        dict avec valide (bool), anomalies (list), donnees, nb_anomalies
    """
    toutes_anomalies = []
    for ligne in donnees:
        toutes_anomalies.extend(_verifier_ligne(ligne))

    valide = len(toutes_anomalies) == 0
    niveau = "INFO" if valide else "ERROR"
    log_evenement(
        niveau,
        "Contrôle qualité terminé",
        valide=valide,
        nb_anomalies=len(toutes_anomalies),
    )

    return {
        "valide": valide,
        "anomalies": toutes_anomalies,
        "donnees": donnees,
        "nb_anomalies": len(toutes_anomalies),
    }
