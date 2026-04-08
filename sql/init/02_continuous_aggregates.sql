DO $$
BEGIN
    IF to_regclass('core.hourly_candles') IS NULL THEN
        EXECUTE $sql$
            CREATE MATERIALIZED VIEW core.hourly_candles
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 hour', bucket) AS bucket,
                ticker,
                FIRST(open, bucket) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, bucket) AS close,
                SUM(volume) AS volume
            FROM core.minute_candles
            GROUP BY 1, 2
            WITH NO DATA
        $sql$;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('core.daily_candles') IS NULL THEN
        EXECUTE $sql$
            CREATE MATERIALIZED VIEW core.daily_candles
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 day', bucket) AS bucket,
                ticker,
                FIRST(open, bucket) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, bucket) AS close,
                SUM(volume) AS volume,
                ((MAX(high) - MIN(low)) / NULLIF(LAST(close, bucket), 0)) * 100 AS volatility
            FROM core.minute_candles
            GROUP BY 1, 2
            WITH NO DATA
        $sql$;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('core.weekly_candles') IS NULL THEN
        EXECUTE $sql$
            CREATE MATERIALIZED VIEW core.weekly_candles
            WITH (timescaledb.continuous) AS
            SELECT
                time_bucket('1 week', bucket) AS bucket,
                ticker,
                FIRST(open, bucket) AS open,
                MAX(high) AS high,
                MIN(low) AS low,
                LAST(close, bucket) AS close,
                SUM(volume) AS volume
            FROM core.minute_candles
            GROUP BY 1, 2
            WITH NO DATA
        $sql$;
    END IF;
END $$;

SELECT add_continuous_aggregate_policy(
    'core.hourly_candles',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy(
    'core.daily_candles',
    start_offset => INTERVAL '90 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy(
    'core.weekly_candles',
    start_offset => INTERVAL '365 days',
    end_offset => INTERVAL '1 week',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

