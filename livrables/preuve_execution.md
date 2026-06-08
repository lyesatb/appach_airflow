# Preuve d'exécution — TP 2

## État des exécutions (API Airflow)

Deux exécutions manuelles du DAG `tp2_premier_dag` ont réussi (`state: success`).

```json
{
  "dag_runs": [
    {
      "dag_run_id": "manual__2026-06-08T12:07:23+00:00",
      "state": "success",
      "run_type": "manual"
    },
    {
      "dag_run_id": "manual__2026-06-08T12:10:11+00:00",
      "state": "success",
      "run_type": "manual"
    }
  ]
}
```

## État des 3 tâches (dernière exécution)

| Tâche               | État    |
|---------------------|---------|
| extraire_donnees    | success |
| transformer_donnees | success |
| charger_resultat    | success |

## Extraits des logs

**extraire_donnees :**
```
Données extraites : ['alice', 'bob', 'charlie']
```

**transformer_donnees :**
```
Données transformées : ['ALICE', 'BOB', 'CHARLIE']
```

**charger_resultat :**
```
Résultat chargé avec succès : ['ALICE', 'BOB', 'CHARLIE']
Pipeline terminé.
```

## Capture d'écran

Voir le fichier `preuve_execution.png` : vue Graph du DAG avec les 3 tâches en **success** (vert).
