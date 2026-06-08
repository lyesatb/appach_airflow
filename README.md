# TP 2 — Créer un premier DAG Airflow

Projet pour le TP « Créer un premier DAG Airflow » — workflow ETL en 3 tâches.

## Structure du projet

```
appach/
├── dags/
│   └── tp2_premier_dag.py    # Fichier DAG (livrable)
├── livrables/
│   ├── EXPLICATION_TP2.md    # Explication du fonctionnement
│   ├── preuve_execution.md   # Preuve d'exécution (logs + API)
│   └── preuve_execution.png  # Capture d'écran Airflow UI
├── docker-compose.yaml       # Environnement Airflow
└── README.md
```

## Démarrage rapide

**Prérequis :** Docker Desktop démarré.

```powershell
# 1. Créer le dossier logs
mkdir logs

# 2. Lancer Airflow
docker compose up -d

# 3. Attendre ~1 min puis ouvrir l'interface
# http://localhost:8080
# Identifiants : admin / admin
```

## Exécution du DAG

1. Ouvrir http://localhost:8080 et se connecter (`admin` / `admin`)
2. Activer le DAG `tp2_premier_dag` (interrupteur à gauche)
3. Cliquer sur **Trigger DAG** (icône ▶) pour lancer manuellement
4. Ouvrir le DAG → onglet **Graph** pour voir les 3 tâches enchaînées
5. Cliquer sur une tâche → **Log** pour consulter les logs

### Lancer via la ligne de commande

```powershell
docker compose exec airflow-scheduler airflow dags trigger tp2_premier_dag
```

## Livrables TP

| Livrable | Fichier |
|----------|---------|
| Fichier DAG Python | `dags/tp2_premier_dag.py` |
| Preuve d'exécution | `livrables/preuve_execution.png` + `preuve_execution.md` |
| Explication | `livrables/EXPLICATION_TP2.md` |

## Arrêt de l'environnement

```powershell
docker compose down
```
