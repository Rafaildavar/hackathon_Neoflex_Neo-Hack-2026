CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS stg.raw_moex_data (
    raw_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_endpoint TEXT NOT NULL,
    ticker TEXT,
    request_params JSONB,
    payload JSONB NOT NULL
);

SELECT create_hypertable(
    'stg.raw_moex_data',
    'ingested_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_raw_moex_data_ticker_time
    ON stg.raw_moex_data (ticker, ingested_at DESC);

CREATE TABLE IF NOT EXISTS core.minute_candles (
    bucket TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    interval_size INTERVAL NOT NULL DEFAULT INTERVAL '1 minute',
    PRIMARY KEY (ticker, bucket)
);

SELECT create_hypertable(
    'core.minute_candles',
    'bucket',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_minute_candles_bucket_desc
    ON core.minute_candles (bucket DESC);

CREATE TABLE IF NOT EXISTS mart.dashboard_metrics (
    bucket TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    interval_type TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    price_change_pct NUMERIC,
    sma_7 NUMERIC,
    sma_20 NUMERIC,
    rsi NUMERIC,
    PRIMARY KEY (ticker, interval_type, bucket)
);

SELECT create_hypertable(
    'mart.dashboard_metrics',
    'bucket',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS mart.technical_indicators (
    bucket TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    interval_type TEXT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    price_change_pct NUMERIC,
    volume NUMERIC,
    sma_7 NUMERIC,
    sma_20 NUMERIC,
    sma_50 NUMERIC,
    rsi NUMERIC,
    ema_12 NUMERIC,
    ema_26 NUMERIC,
    macd_line NUMERIC,
    macd_signal NUMERIC,
    macd_histogram NUMERIC,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, interval_type, bucket)
);

SELECT create_hypertable(
    'mart.technical_indicators',
    'bucket',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS mart.daily_metrics (
    trade_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    close NUMERIC,
    price_change_pct NUMERIC,
    volume NUMERIC,
    volatility_pct NUMERIC,
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE IF NOT EXISTS mart.anomaly_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker TEXT NOT NULL,
    anomaly_type TEXT NOT NULL,
    severity TEXT,
    metric_value NUMERIC,
    threshold_value NUMERIC,
    details JSONB
);

CREATE TABLE IF NOT EXISTS auth.users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    telegram_chat_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CHECK (position('@' in email) > 1),
    CHECK (char_length(password_hash) > 20)
);

CREATE INDEX IF NOT EXISTS idx_auth_users_email
    ON auth.users (email);

CREATE UNIQUE INDEX IF NOT EXISTS uq_auth_users_telegram_chat_id
    ON auth.users (telegram_chat_id)
    WHERE telegram_chat_id IS NOT NULL;

