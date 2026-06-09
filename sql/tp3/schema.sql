-- TP 3 — Schéma PostgreSQL météo + suivi d'ingestion

CREATE SCHEMA IF NOT EXISTS tp3;

CREATE TABLE IF NOT EXISTS tp3.meteo_courant (
    id SERIAL PRIMARY KEY,
    ville VARCHAR(100) NOT NULL,
    latitude NUMERIC(9, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    date_releve TIMESTAMP NOT NULL,
    temperature_celsius NUMERIC(5, 2) NOT NULL,
    humidite_pct INTEGER NOT NULL,
    vitesse_vent_kmh NUMERIC(5, 2) NOT NULL,
    code_meteo INTEGER NOT NULL,
    source_api VARCHAR(50) NOT NULL DEFAULT 'open-meteo',
    date_ingestion TIMESTAMPTZ NOT NULL,
    UNIQUE (ville, date_releve)
);

CREATE TABLE IF NOT EXISTS tp3.suivi_ingestion (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255) NOT NULL,
    nb_lignes_chargees INTEGER NOT NULL,
    villes TEXT[] NOT NULL,
    statut VARCHAR(50) NOT NULL,
    date_ingestion TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_meteo_courant_ville
    ON tp3.meteo_courant (ville);

CREATE INDEX IF NOT EXISTS idx_suivi_ingestion_date
    ON tp3.suivi_ingestion (date_ingestion DESC);
