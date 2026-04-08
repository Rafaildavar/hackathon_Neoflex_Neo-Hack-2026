CREATE TABLE IF NOT EXISTS mart.daily_metrics (
    trade_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    close NUMERIC(18,6),
    volume BIGINT,
    price_change_pct NUMERIC(10,4),
    range_pct NUMERIC(10,4),
    PRIMARY KEY (trade_date, ticker)
);

CREATE TABLE IF NOT EXISTS mart.anomaly_events (
    id BIGSERIAL PRIMARY KEY,
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker TEXT NOT NULL,
    anomaly_type TEXT NOT NULL,
    metric_value NUMERIC(18,6),
    threshold NUMERIC(18,6),
    severity TEXT DEFAULT 'medium',
    is_alert_sent BOOLEAN DEFAULT FALSE
);
