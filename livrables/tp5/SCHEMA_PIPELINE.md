# TP 5 — Schéma logique du pipeline

```
┌─────────────────────────┐
│ extraire_donnees_meteo  │  ← API Open-Meteo (villes paramétrables)
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│ archiver_donnees_brutes │  ← JSON brut archivé par dag_run_id
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│   transformer_donnees   │  ← Normalisation (+ simulation anomalie optionnelle)
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│ controler_qualite_donnees│ ← Température, humidité, vent, ville obligatoire
└───────────┬─────────────┘
            ▼
┌─────────────────────────┐
│   decider_branchement   │  ← BranchPythonOperator
└─────┬─────────────┬─────┘
      │ OK          │ KO
      ▼             ▼
┌─────────────┐ ┌──────────────────────┐
│charger_     │ │tracer_anomalie_qualite│ → tp5.anomalies_qualite
│postgresql   │ └──────────┬───────────┘
└──────┬──────┘            │
       │                   │
       └─────────┬─────────┘
                 ▼
      ┌──────────────────────┐
      │ journaliser_execution │ → tp5.suivi_ingestion
      └──────────┬───────────┘
                 ▼
      ┌──────────────────────┐
      │  exporter_preuves    │ → livrables/tp5/preuves/*.json
      └──────────────────────┘
```

## Règle de branchement

| Condition | Branche suivante | Chargement final |
|-----------|------------------|------------------|
| `valide == True` | `charger_postgresql` | Oui (UPSERT idempotent) |
| `valide == False` | `tracer_anomalie_qualite` | **Non** |

## Idempotence

- Contrainte `UNIQUE (ville, date_releve)` sur `tp5.meteo_courant`
- `ON CONFLICT DO UPDATE` → relance sans doublon
