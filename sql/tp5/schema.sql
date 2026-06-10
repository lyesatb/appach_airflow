-- TP 5 — Schéma PostgreSQL industrialisé

CREATE SCHEMA IF NOT EXISTS tp5;

CREATE TABLE IF NOT EXISTS tp5.meteo_courant (
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
    dag_run_id VARCHAR(255),
    UNIQUE (ville, date_releve)
);

CREATE TABLE IF NOT EXISTS tp5.suivi_ingestion (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255) NOT NULL,
    statut VARCHAR(50) NOT NULL,
    nb_lignes_chargees INTEGER NOT NULL DEFAULT 0,
    nb_anomalies INTEGER NOT NULL DEFAULT 0,
    villes TEXT[] NOT NULL,
    chemin_archive TEXT,
    mode_execution VARCHAR(50) NOT NULL DEFAULT 'nominal',
    date_ingestion TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tp5.anomalies_qualite (
    id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255) NOT NULL,
    ville VARCHAR(100),
    champ VARCHAR(100) NOT NULL,
    valeur TEXT,
    regle TEXT NOT NULL,
    date_detection TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tp5_meteo_ville ON tp5.meteo_courant (ville);
CREATE INDEX IF NOT EXISTS idx_tp5_suivi_date ON tp5.suivi_ingestion (date_ingestion DESC);
CREATE INDEX IF NOT EXISTS idx_tp5_anomalies_run ON tp5.anomalies_qualite (dag_run_id);
