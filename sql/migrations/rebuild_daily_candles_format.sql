DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'daily_candles'
    ) THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'daily_candles' AND column_name = 'ticker'
    ) THEN
        ALTER TABLE core.daily_candles RENAME COLUMN ticker TO name;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'daily_candles' AND column_name = 'trade_date'
    ) THEN
        ALTER TABLE core.daily_candles RENAME COLUMN trade_date TO date;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'daily_candles' AND column_name = 'volume'
    ) THEN
        ALTER TABLE core.daily_candles RENAME COLUMN volume TO valume;
    END IF;

    ALTER TABLE core.daily_candles DROP COLUMN IF EXISTS price_change_pct;
    ALTER TABLE core.daily_candles DROP COLUMN IF EXISTS range_pct;

    ALTER TABLE core.daily_candles DROP CONSTRAINT IF EXISTS daily_candles_pkey;
    ALTER TABLE core.daily_candles ADD CONSTRAINT daily_candles_pkey PRIMARY KEY (name, date);
END $$;

DROP INDEX IF EXISTS core.idx_core_daily_ticker_date;
CREATE INDEX IF NOT EXISTS idx_core_daily_ticker_date
    ON core.daily_candles (name, date DESC);
