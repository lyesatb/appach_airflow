"""
Transformation des réponses Open-Meteo vers une structure prête pour le pipeline.

On ne conserve que les champs utiles au métier (voir livrables/tp2/CHAMPS_SELECTIONNES.md).
"""

from datetime import datetime, timezone


def transformer_reponse_ville(enregistrement_brut: dict, date_ingestion: str | None = None) -> dict:
    """
    Transforme une réponse API brute en enregistrement normalisé.

    Args:
        enregistrement_brut: dict avec ville, latitude, longitude, reponse_api_brute
        date_ingestion: horodatage ISO de l'ingestion (UTC)
    """
    reponse = enregistrement_brut["reponse_api_brute"]
    courant = reponse["current"]

    return {
        "ville": enregistrement_brut["ville"],
        "latitude": enregistrement_brut["latitude"],
        "longitude": enregistrement_brut["longitude"],
        "date_releve": courant["time"],
        "temperature_celsius": courant["temperature_2m"],
        "humidite_pct": courant["relative_humidity_2m"],
        "vitesse_vent_kmh": courant["wind_speed_10m"],
        "code_meteo": courant["weather_code"],
        "source_api": "open-meteo",
        "date_ingestion": date_ingestion or datetime.now(timezone.utc).isoformat(),
    }


def transformer_reponses_brutes(reponses_brutes: list[dict]) -> list[dict]:
    """Transforme la liste complète des réponses API en données prêtes pour la table cible."""
    date_ingestion = datetime.now(timezone.utc).isoformat()
    return [
        transformer_reponse_ville(enregistrement, date_ingestion)
        for enregistrement in reponses_brutes
    ]
