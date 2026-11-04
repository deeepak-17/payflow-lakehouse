-- PayFlow warehouse schema (PostgreSQL). Star schema for payments analytics,
-- plus reconciliation breaks and data-quality results for monitoring.
CREATE SCHEMA IF NOT EXISTS payflow;
SET search_path TO payflow;

CREATE TABLE IF NOT EXISTS dim_currency (
    currency        CHAR(3) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_corridor (
    corridor_key    VARCHAR(5) PRIMARY KEY,      -- e.g. 'US-IN'
    src_country     CHAR(2) NOT NULL,
    dst_country     CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        DATE PRIMARY KEY,
    day_of_week     SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id          VARCHAR(40) PRIMARY KEY,
    created_date        DATE NOT NULL REFERENCES dim_date(date_key),
    corridor_key        VARCHAR(5) NOT NULL REFERENCES dim_corridor(corridor_key),
    currency            CHAR(3) NOT NULL REFERENCES dim_currency(currency),
    target_currency     CHAR(3) NOT NULL REFERENCES dim_currency(currency),
    rail                VARCHAR(10) NOT NULL,
    payer_fi            VARCHAR(10),
    payee_fi            VARCHAR(10),
    status              VARCHAR(10) NOT NULL CHECK (status IN ('PENDING','SETTLED','FAILED')),
    amount              NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    amount_usd          NUMERIC(18,2) NOT NULL,
    fx_rate_date        DATE NOT NULL,
    fx_fallback         BOOLEAN NOT NULL,
    created_ts          TIMESTAMPTZ NOT NULL,
    outcome_ts          TIMESTAMPTZ,
    settlement_seconds  BIGINT
);
CREATE INDEX IF NOT EXISTS ix_fact_created_corridor ON fact_payments (created_date, corridor_key);
CREATE INDEX IF NOT EXISTS ix_fact_status ON fact_payments (status) WHERE status <> 'SETTLED';

CREATE TABLE IF NOT EXISTS recon_breaks (
    recon_date      DATE NOT NULL,
    payment_id      VARCHAR(40) NOT NULL,
    recon_status    VARCHAR(25) NOT NULL,
    ledger_amount   NUMERIC(18,2),
    settled_amount  NUMERIC(18,2),
    amount_diff     NUMERIC(18,2),
    PRIMARY KEY (recon_date, payment_id)
);

CREATE TABLE IF NOT EXISTS dq_results (
    run_date        DATE NOT NULL,
    layer           VARCHAR(10) NOT NULL,
    check_name      VARCHAR(60) NOT NULL,
    severity        VARCHAR(10) NOT NULL,
    observed        DOUBLE PRECISION,
    threshold       VARCHAR(30),
    passed          BOOLEAN NOT NULL,
    PRIMARY KEY (run_date, layer, check_name)
);
