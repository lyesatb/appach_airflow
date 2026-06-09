# TP 3 — Preuve de chargement PostgreSQL

## Exécution

DAG : `tp3_pipeline_meteo_postgresql` — toutes les tâches en **success**.

## Données dans `tp3.meteo_courant`

Voir le fichier `preuve_chargement.json` (extrait automatique après chaque run).

## Table de suivi `tp3.suivi_ingestion`

Chaque exécution du DAG écrit une ligne avec :
- `dag_run_id` — identifiant du run Airflow
- `nb_lignes_chargees` — nombre de villes chargées
- `villes` — liste des villes ingérées
- `statut` — `success` ou `failed`
- `date_ingestion` — horodatage

## Vérification manuelle (psql)

```powershell
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM tp3.meteo_courant ORDER BY date_releve DESC LIMIT 5;"
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM tp3.suivi_ingestion ORDER BY date_ingestion DESC LIMIT 5;"
```
