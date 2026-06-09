# TP 2 — Champs sélectionnés

## Données API brutes vs données préparées

| Type | Fichier | Contenu |
|------|---------|---------|
| **Brut API** | `donnees_brutes_apercu.json` | Réponse Open-Meteo complète (timezone, elevation, unités, etc.) |
| **Préparé pipeline** | `apercu_donnees.json` | Enregistrements normalisés, prêts pour une table cible |

## Champs conservés (table cible `meteo_courant`)

| Champ | Source API | Justification métier |
|-------|------------|----------------------|
| `ville` | Config pipeline | Identifiant lisible pour le reporting |
| `latitude` / `longitude` | Config pipeline | Géolocalisation stable de la mesure |
| `date_releve` | `current.time` | Horodatage de la mesure météo |
| `temperature_celsius` | `current.temperature_2m` | Indicateur principal pour suivi / alertes |
| `humidite_pct` | `current.relative_humidity_2m` | Confort, risque pluie |
| `vitesse_vent_kmh` | `current.wind_speed_10m` | Conditions extérieures, sécurité |
| `code_meteo` | `current.weather_code` | Catégorisation WMO (soleil, pluie, etc.) |
| `source_api` | Constante | Traçabilité de la provenance |
| `date_ingestion` | Généré pipeline | Audit : quand la donnée a été collectée |

## Champs API non conservés (volontairement)

| Champ API | Raison de l'exclusion |
|-----------|----------------------|
| `elevation` | Peu utile pour notre cas d'usage ville |
| `timezone`, `timezone_abbreviation` | Redondant (Europe/Paris fixe) |
| `current_units` | Métadonnée technique, pas une mesure |
| Prévisions horaires / journalières | Hors scope : on ingère uniquement l'état **courant** |

## Cohérence de la table cible

Tous les enregistrements partagent le **même schéma** :

```
ville, latitude, longitude, date_releve,
temperature_celsius, humidite_pct, vitesse_vent_kmh,
code_meteo, source_api, date_ingestion
```
