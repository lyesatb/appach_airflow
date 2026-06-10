"""Utilitaires partagés météo."""

import json


def normaliser_villes(villes, villes_defaut: list[dict]) -> list[dict]:
    """
    Normalise le paramètre villes (list, dict unique, JSON string).

    Corrige le cas Airflow où un dict est passé au lieu d'une liste.
    """
    if villes is None:
        return list(villes_defaut)

    if isinstance(villes, str):
        try:
            villes = json.loads(villes)
        except json.JSONDecodeError:
            return list(villes_defaut)

    if isinstance(villes, dict):
        if "ville" in villes and "latitude" in villes:
            return [villes]
        valeurs = [v for v in villes.values() if isinstance(v, dict)]
        if valeurs:
            return valeurs
        return list(villes_defaut)

    if isinstance(villes, list):
        if villes and isinstance(villes[0], dict):
            return villes
        return list(villes_defaut)

    return list(villes_defaut)
