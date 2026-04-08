CREATE TABLE IF NOT EXISTS core.daily_candles (
    trade_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    open NUMERIC(18,6),
    close NUMERIC(18,6),
    high NUMERIC(18,6),
    low NUMERIC(18,6),
    volume BIGINT,
    price_change_pct NUMERIC(10,4),
    range_pct NUMERIC(10,4),
    PRIMARY KEY (trade_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_core_daily_ticker_date
    ON core.daily_candles (ticker, trade_date DESC);

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
