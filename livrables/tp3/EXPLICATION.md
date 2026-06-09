# TP 3 — Pipeline complet API → PostgreSQL

## Workflow

```
recuperer_meteo_api
    >> transformer_donnees_meteo
    >> charger_postgresql
    >> journaliser_ingestion
    >> exporter_preuve_chargement
```

## Paramètres du DAG

Le DAG `tp3_pipeline_meteo_postgresql` est **paramétrable** :

| Paramètre | Description | Défaut |
|-----------|-------------|--------|
| `villes` | Liste `{ville, latitude, longitude}` | Paris, Lyon, Marseille |
| `connexion_postgres` | URL PostgreSQL | `postgresql://airflow:airflow@postgres:5432/airflow` |

### Exemple de trigger avec config personnalisée

```json
{
  "villes": [
    {"ville": "Paris", "latitude": 48.86, "longitude": 2.35},
    {"ville": "Lille", "latitude": 50.63, "longitude": 3.07}
  ]
}
```

Dans l'UI Airflow : **Trigger DAG w/ config** → coller le JSON ci-dessus.

## Tables PostgreSQL

| Table | Rôle |
|-------|------|
| `tp3.meteo_courant` | Données météo transformées |
| `tp3.suivi_ingestion` | Traçabilité de chaque exécution |

Script SQL : `sql/tp3/schema.sql`

## Séparation des responsabilités

| Phase | Tâche | Module |
|-------|-------|--------|
| Récupération | `recuperer_meteo_api` | `plugins/meteo/api_open_meteo.py` |
| Transformation | `transformer_donnees_meteo` | `plugins/meteo/transformation.py` |
| Chargement | `charger_postgresql` | `plugins/meteo/postgres_loader.py` |
| Suivi | `journaliser_ingestion` | `plugins/meteo/postgres_loader.py` |
