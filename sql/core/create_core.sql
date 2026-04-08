CREATE TABLE IF NOT EXISTS core.daily_candles (
    name TEXT NOT NULL,
    date DATE NOT NULL,
    high NUMERIC(18,6),
    open NUMERIC(18,6),
    close NUMERIC(18,6),
    low NUMERIC(18,6),
    valume BIGINT,
    PRIMARY KEY (name, date)
);

CREATE INDEX IF NOT EXISTS idx_core_daily_ticker_date
    ON core.daily_candles (name, date DESC);

CREATE TABLE IF NOT EXISTS core.intraday_quotes (
    quote_ts TIMESTAMPTZ NOT NULL,
    ticker TEXT NOT NULL,
    last_price NUMERIC(18,6),
    change_pct NUMERIC(10,4),
    volume BIGINT,
    PRIMARY KEY (quote_ts, ticker)
);

CREATE INDEX IF NOT EXISTS idx_core_intraday_ticker_ts
    ON core.intraday_quotes (ticker, quote_ts DESC);
