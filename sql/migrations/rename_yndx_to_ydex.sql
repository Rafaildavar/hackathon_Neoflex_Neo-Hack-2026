DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stg' AND table_name = 'raw_moex_data'
    ) THEN
        UPDATE stg.raw_moex_data
        SET ticker = 'YDEX'
        WHERE ticker = 'YNDX';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'daily_candles'
    ) THEN
        DELETE FROM core.daily_candles
        WHERE ticker = 'YNDX'
          AND EXISTS (
              SELECT 1
              FROM core.daily_candles c2
              WHERE c2.trade_date = core.daily_candles.trade_date
                AND c2.ticker = 'YDEX'
          );

        UPDATE core.daily_candles
        SET ticker = 'YDEX'
        WHERE ticker = 'YNDX';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'core' AND table_name = 'intraday_quotes'
    ) THEN
        DELETE FROM core.intraday_quotes
        WHERE ticker = 'YNDX'
          AND EXISTS (
              SELECT 1
              FROM core.intraday_quotes c2
              WHERE c2.quote_ts = core.intraday_quotes.quote_ts
                AND c2.ticker = 'YDEX'
          );

        UPDATE core.intraday_quotes
        SET ticker = 'YDEX'
        WHERE ticker = 'YNDX';
    END IF;
END $$;
