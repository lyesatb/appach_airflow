"""
Récupération des données brutes depuis l'API Open-Meteo.

Ce module ne transforme pas les données : il retourne la réponse JSON telle quelle.
"""

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request

import certifi

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Au moins 3 villes — coordonnées fixes pour éviter un second appel géocodage
VILLES_A_INGERER = [
    {"ville": "Paris", "latitude": 48.86, "longitude": 2.35},
    {"ville": "Lyon", "latitude": 45.76, "longitude": 4.84},
    {"ville": "Marseille", "latitude": 43.30, "longitude": 5.37},
]

CHAMPS_COURANTS_API = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
]


def construire_url_meteo(latitude: float, longitude: float) -> str:
    """Construit l'URL d'appel Open-Meteo pour une ville."""
    params = urllib.parse.urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(CHAMPS_COURANTS_API),
            "timezone": "Europe/Paris",
        }
    )
    return f"{API_BASE_URL}?{params}"


def _ouvrir_url(request: urllib.request.Request):
    """
    Ouvre une URL HTTPS en gérant les erreurs SSL fréquentes dans Docker
    (certificats manquants ou proxy d'entreprise).
    """
    contexte_verifie = ssl.create_default_context(cafile=certifi.where())
    try:
        return urllib.request.urlopen(request, timeout=30, context=contexte_verifie)
    except urllib.error.URLError as erreur:
        if "CERTIFICATE_VERIFY_FAILED" not in str(erreur):
            raise
        # Repli pour environnement Docker / réseau entreprise
        contexte_sans_verif = ssl._create_unverified_context()
        return urllib.request.urlopen(request, timeout=30, context=contexte_sans_verif)


def appeler_api_meteo(latitude: float, longitude: float) -> dict:
    """Appelle l'API et retourne le JSON brut (sans transformation métier)."""
    url = construire_url_meteo(latitude, longitude)
    request = urllib.request.Request(url, headers={"User-Agent": "appach-tp2/1.0"})

    with _ouvrir_url(request) as response:
        return json.loads(response.read().decode("utf-8"))


def recuperer_reponses_brutes() -> list[dict]:
    """
    Récupère les réponses JSON brutes pour toutes les villes configurées.

    Chaque élément contient le nom de la ville, ses coordonnées et la réponse API complète.
    """
    reponses = []

    for ville in VILLES_A_INGERER:
        reponse_api = appeler_api_meteo(ville["latitude"], ville["longitude"])
        reponses.append(
            {
                "ville": ville["ville"],
                "latitude": ville["latitude"],
                "longitude": ville["longitude"],
                "reponse_api_brute": reponse_api,
            }
        )

    return reponses
