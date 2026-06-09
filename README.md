# Appach — Projet Airflow

Dépôt regroupant les travaux pratiques Airflow. **Chaque TP a son dossier et son DAG.**

| TP | Sujet | DAG Airflow | Dossier livrables |
|----|-------|-------------|-------------------|
| **TP 1** | Premier DAG (3 tâches ETL) | `tp1_premier_dag` | `livrables/tp1/` |
| **TP 2** | Ingestion API météo Open-Meteo | `tp2_ingestion_meteo` | `livrables/tp2/` |
| **TP 3** | Pipeline API → PostgreSQL | `tp3_pipeline_meteo_postgresql` | `livrables/tp3/` |

## Structure du projet

```
appach/
├── dags/
│   ├── tp1_premier_dag.py           # TP 1
│   ├── tp2_ingestion_meteo_dag.py   # TP 2
│   └── tp3_pipeline_meteo_postgresql_dag.py  # TP 3
├── plugins/meteo/                   # Code métier TP 2 & TP 3
│   ├── api_open_meteo.py            #   → appels API
│   ├── transformation.py            #   → transformation
│   └── postgres_loader.py           #   → chargement PostgreSQL (TP 3)
├── sql/tp3/schema.sql               # Script SQL TP 3
├── livrables/
│   ├── tp1/                         # Livrables TP 1
│   │   ├── EXPLICATION.md
│   │   ├── preuve_execution.md
│   │   └── preuve_execution.png
│   ├── tp2/                         # Livrables TP 2
│   └── tp3/                         # Livrables TP 3
├── docker-compose.yaml
└── README.md
```

## Démarrage (commun aux deux TPs)

**Prérequis :** Docker Desktop démarré.

```powershell
mkdir logs
docker compose up -d
# Interface : http://localhost:8080  (admin / admin)
```

---

## TP 1 — Premier DAG Airflow

**Objectif :** Créer un DAG simple avec 3 tâches et des dépendances explicites.

**Workflow :**
```
extraire_donnees >> transformer_donnees >> charger_resultat
```

**Fichiers :**
- DAG : `dags/tp1_premier_dag.py`
- Livrables : `livrables/tp1/`

**Lancer :**
```powershell
docker compose exec airflow-scheduler airflow dags trigger tp1_premier_dag
```

---

## TP 2 — Ingestion API météo

**Objectif :** Récupérer la météo de Paris, Lyon et Marseille via [Open-Meteo](https://open-meteo.com/), séparer récupération API et transformation.

**Workflow :**
```
recuperer_meteo_api >> preparer_donnees_meteo >> exporter_apercu_donnees
```

| Tâche | Rôle | Module |
|-------|------|--------|
| `recuperer_meteo_api` | JSON brut depuis l'API | `plugins/meteo/api_open_meteo.py` |
| `preparer_donnees_meteo` | Structure table cible | `plugins/meteo/transformation.py` |
| `exporter_apercu_donnees` | Export JSON livrables | DAG |

**Fichiers :**
- DAG : `dags/tp2_ingestion_meteo_dag.py`
- Livrables : `livrables/tp2/`

**Lancer :**
```powershell
docker compose exec airflow-scheduler airflow dags trigger tp2_ingestion_meteo
```

---

## TP 3 — Pipeline API → PostgreSQL

**Objectif :** Pipeline complet Open-Meteo → transformation → PostgreSQL + table de suivi. DAG **paramétrable**.

**Workflow :**
```
recuperer_meteo_api >> transformer_donnees_meteo >> charger_postgresql
    >> journaliser_ingestion >> exporter_preuve_chargement
```

**Fichiers :**
- DAG : `dags/tp3_pipeline_meteo_postgresql_dag.py`
- SQL : `sql/tp3/schema.sql`
- Livrables : `livrables/tp3/`

**Lancer :**
```powershell
docker compose exec airflow-scheduler airflow dags trigger tp3_pipeline_meteo_postgresql
```

---

## Arrêt

```powershell
docker compose down
```
