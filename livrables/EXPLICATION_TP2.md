# Explication du fonctionnement — TP 2

## Vue d'ensemble

Le DAG `tp2_premier_dag` modélise un mini-pipeline **ETL** (Extract, Transform, Load) en 3 étapes séparées, comme demandé dans le TP.

```
extraire_donnees  →  transformer_donnees  →  charger_resultat
```

## Rôle de chaque tâche

### 1. `extraire_donnees`
- **Rôle :** Simule la récupération de données brutes (ici une liste de prénoms).
- **Action :** Retourne `['alice', 'bob', 'charlie']` via XCom pour la tâche suivante.

### 2. `transformer_donnees`
- **Rôle :** Applique une transformation sur les données extraites.
- **Action :** Récupère le résultat de `extraire_donnees` (XCom), met chaque prénom en majuscules, retourne `['ALICE', 'BOB', 'CHARLIE']`.

### 3. `charger_resultat`
- **Rôle :** Simule le chargement du résultat final (vers une base, un fichier, etc.).
- **Action :** Récupère les données transformées et affiche le résultat final dans les logs.

## Dépendances

La ligne suivante dans le code définit explicitement l'ordre d'exécution :

```python
tache_extraire >> tache_transformer >> tache_charger
```

Airflow n'exécutera `transformer_donnees` qu'après le succès de `extraire_donnees`, et `charger_resultat` qu'après `transformer_donnees`.

## Concepts Airflow utilisés

| Concept | Utilisation dans ce TP |
|---------|------------------------|
| **DAG** | Conteneur du workflow `tp2_premier_dag` |
| **Task** | Chaque étape (extraire, transformer, charger) |
| **Operator** | `PythonOperator` pour exécuter du code Python |
| **XCom** | Passage de données entre tâches |
| **Trigger manuel** | `schedule=None` + lancement via l'UI ou CLI |
