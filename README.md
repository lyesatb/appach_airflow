# TP 5 — Industrialisation d'un pipeline Airflow Open-Meteo

## Description du pipeline

Pipeline Airflow complet qui :
1. **Extrait** la météo Open-Meteo pour plusieurs villes configurables
2. **Archive** les données brutes (JSON) par exécution
3. **Transforme** vers un schéma normalisé
4. **Contrôle la qualité** (température, humidité, vent)
5. **Décide** via branchement conditionnel : charger ou tracer l'anomalie
6. **Charge** PostgreSQL uniquement si qualité OK (mode idempotent)
7. **Journalise** chaque exécution dans une table de suivi
8. **Exporte** les preuves JSON

**DAG :** `tp5_pipeline_industrialise`

---

## Schéma du workflow

Voir `SCHEMA_PIPELINE.md` et le diagramme ci-dessous :

```
extraire → archiver → transformer → controler_qualite → decider_branchement
                              ├─ OK → charger_postgresql ─┐
                              └─ KO → tracer_anomalie ────┤
                                                          journaliser → exporter_preuves
```

---

## Variables Airflow

| Variable | Usage | Valeur par défaut |
|----------|-------|-------------------|
| `tp5_villes_defaut` | *(documentée, optionnelle)* | Paris, Lyon, Marseille |
| `tp5_connexion_postgres` | *(documentée, optionnelle)* | URL PostgreSQL |

> **Implémentation :** le DAG utilise des **Params** Airflow (`villes`, `connexion_postgres`, `simuler_anomalie_qualite`) surchargeables via **Trigger DAG w/ config**.

---

## Connexions Airflow

| Connexion | ID | Usage |
|-----------|-----|-------|
| PostgreSQL | `postgres_default` | Documentée — le DAG utilise l'URL param `connexion_postgres` |

Valeur par défaut : `postgresql://airflow:airflow@postgres:5432/airflow`

---

## Tâches du DAG

| Tâche | Rôle | Module |
|-------|------|--------|
| `extraire_donnees_meteo` | Appels API Open-Meteo | `plugins/meteo/tp5/extraction.py` |
| `archiver_donnees_brutes` | Archive JSON brut | `plugins/meteo/tp5/archivage.py` |
| `transformer_donnees` | Normalisation | `plugins/meteo/tp5/transformation.py` |
| `controler_qualite_donnees` | Contrôles qualité | `plugins/meteo/tp5/qualite.py` |
| `decider_branchement` | Branchement conditionnel | DAG (BranchPythonOperator) |
| `charger_postgresql` | UPSERT idempotent | `plugins/meteo/tp5/postgres.py` |
| `tracer_anomalie_qualite` | Trace sans charger | `plugins/meteo/tp5/tracabilite.py` |
| `journaliser_execution` | Suivi ingestion | `plugins/meteo/tp5/tracabilite.py` |
| `exporter_preuves` | Export JSON preuves | `plugins/meteo/tp5/tracabilite.py` |

---

## Stratégie de robustesse

- **Retries :** 2 tentatives, délai 1 minute
- **Timeout :** 5 minutes sur l'extraction API
- **Gestion SSL :** repli automatique (réseau entreprise / Docker)
- **Branchement :** pas de chargement si qualité KO
- **Logs applicatifs :** format `[TP5][NIVEAU] message | {contexte}`

---

## Stratégie d'idempotence

- Contrainte `UNIQUE (ville, date_releve)` sur `tp5.meteo_courant`
- `INSERT ... ON CONFLICT DO UPDATE` (UPSERT)
- Relance du DAG → **pas de doublons**, mise à jour des valeurs

---

## Contrôles qualité

| Champ | Règle |
|-------|-------|
| `temperature_celsius` | Entre -50 et 60 |
| `humidite_pct` | Entre 0 et 100 |
| `vitesse_vent_kmh` | >= 0 |
| `ville` | Obligatoire |

---

## Règle de branchement conditionnel

```python
if resultat_qualite["valide"]:
    → charger_postgresql
else:
    → tracer_anomalie_qualite  # pas de chargement final
```

Les deux branches convergent vers `journaliser_execution` (`trigger_rule=none_failed_min_one_success`).

---

## Logs produits

- Logs Airflow standard (par tâche)
- Logs applicatifs `[TP5]` dans chaque module
- Tables PostgreSQL : `tp5.suivi_ingestion`, `tp5.anomalies_qualite`

---

## Tables PostgreSQL

| Table | Rôle |
|-------|------|
| `tp5.meteo_courant` | Données météo validées et chargées |
| `tp5.suivi_ingestion` | Traçabilité de chaque exécution |
| `tp5.anomalies_qualite` | Détail des anomalies détectées |

Script : `sql/tp5/schema.sql`

---

## Cas à démontrer

### Cas nominal
```powershell
docker compose exec airflow-scheduler airflow dags trigger tp5_pipeline_industrialise
```
→ Toutes les tâches vertes, branche `charger_postgresql` exécutée.

### Cas anomalie qualité
Dans l'UI : **Trigger DAG w/ config** :
```json
{"simuler_anomalie_qualite": true}
```
→ `tracer_anomalie_qualite` exécutée, `charger_postgresql` skipped, anomalie tracée.

### Cas relance (idempotence)
Relancer 2 fois le cas nominal → `total_lignes_meteo_courant` stable, `doublons_detectes: 0`.

---

## Preuves d'exécution

Placer les captures dans `livrables/tp5/preuves/` :

| Fichier | Contenu |
|---------|---------|
| `cas_nominal_graph.png` | Graph Airflow — run success |
| `cas_nominal_logs.png` | Logs tâche `extraire_donnees_meteo` |
| `cas_anomalie_graph.png` | Graph — branche anomalie |
| `cas_anomalie_postgres.png` | Table `tp5.anomalies_qualite` |
| `cas_relance_idempotence.png` | Preuve doublons = 0 |
| `cas_postgresql_tables.png` | Contenu tables tp5 |

JSON auto-générés : `preuve_nominal_*.json`, `preuve_anomalie_*.json`

---

## Limites

- Pas de connexion Airflow `postgres_default` câblée nativement (URL en Param)
- Simulation anomalie via paramètre (pas de fichier externe)
- Archive locale (pas S3/MinIO)
- Une seule source API (Open-Meteo)

---

## Lancement

```powershell
docker compose up -d
docker compose exec airflow-scheduler airflow dags trigger tp5_pipeline_industrialise
```

Interface : http://localhost:8080 (admin / admin)
