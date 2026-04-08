CREATE TABLE IF NOT EXISTS stg.raw_moex_data (
    id BIGSERIAL PRIMARY KEY,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_endpoint TEXT NOT NULL,
    ticker TEXT NOT NULL,
    event_ts TIMESTAMPTZ,
    payload_json JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_moex_ticker_event
    ON stg.raw_moex_data (ticker, event_ts DESC);
