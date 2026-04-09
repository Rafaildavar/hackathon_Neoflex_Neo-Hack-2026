from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv is not None:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


FEATURE_COLUMNS = ("open", "high", "low", "close")
PREDICTION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS mart.lstm_daily_predictions (
    ticker TEXT PRIMARY KEY,
    source_bucket TIMESTAMPTZ NOT NULL,
    predicted_bucket TIMESTAMPTZ NOT NULL,
    predicted_open NUMERIC NOT NULL,
    predicted_high NUMERIC NOT NULL,
    predicted_low NUMERIC NOT NULL,
    predicted_close NUMERIC NOT NULL,
    model_path TEXT NOT NULL,
    model_version TEXT NOT NULL DEFAULT 'lstm_v1',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lstm_daily_predictions_bucket
    ON mart.lstm_daily_predictions (predicted_bucket DESC);
"""


@dataclass
class DbConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DbConfig":
        return cls(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "moex_dwh"),
            user=os.getenv("POSTGRES_USER", "moex"),
            password=os.getenv("POSTGRES_PASSWORD", "moex_pass"),
        )


@dataclass
class RunStats:
    total_tickers: int = 0
    success_tickers: int = 0
    skipped_tickers: int = 0
    errors_tickers: int = 0


def _load_tf():
    try:
        from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
        from tensorflow.keras.layers import LSTM, Dense, Dropout
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.optimizers import Adam
    except ImportError as exc:
        raise RuntimeError(
            "TensorFlow не установлен. Установите зависимости: pip install -r requirements.txt"
        ) from exc

    return Sequential, LSTM, Dropout, Dense, Adam, EarlyStopping, ReduceLROnPlateau


def build_model(timesteps: int):
    Sequential, LSTM, Dropout, Dense, Adam, _, _ = _load_tf()

    model = Sequential(
        [
            LSTM(64, input_shape=(timesteps, 4), return_sequences=True),
            Dropout(0.15),
            LSTM(32, return_sequences=False),
            Dropout(0.15),
            Dense(16, activation="relu"),
            Dense(4),
        ]
    )
    optimizer = Adam(learning_rate=0.0005)
    model.compile(optimizer=optimizer, loss="mse", metrics=["mae"])
    return model


def ensure_prediction_table(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(PREDICTION_TABLE_DDL)


def load_tickers(conn: psycopg2.extensions.connection, ticker: str | None = None) -> list[str]:
    with conn.cursor() as cur:
        if ticker:
            cur.execute("SELECT DISTINCT ticker FROM core.daily_candles WHERE ticker = %s", (ticker.upper(),))
        else:
            cur.execute("SELECT DISTINCT ticker FROM core.daily_candles ORDER BY ticker")
        rows = cur.fetchall()
    return [row[0] for row in rows]


def load_daily_candles(conn: psycopg2.extensions.connection, ticker: str) -> pd.DataFrame:
    query = """
        SELECT bucket, open, high, low, close
        FROM core.daily_candles
        WHERE ticker = %s
        ORDER BY bucket
    """
    return pd.read_sql_query(query, conn, params=[ticker])


def _fit_scaler(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mins = values.min(axis=0)
    maxs = values.max(axis=0)
    ranges = maxs - mins
    ranges = np.where(ranges == 0, 1.0, ranges)
    return mins, maxs, ranges


def _scale(values: np.ndarray, mins: np.ndarray, ranges: np.ndarray) -> np.ndarray:
    return (values - mins) / ranges


def _inverse_scale(values: np.ndarray, mins: np.ndarray, ranges: np.ndarray) -> np.ndarray:
    return values * ranges + mins


def _create_sequences(values: np.ndarray, timesteps: int) -> tuple[np.ndarray, np.ndarray]:
    x_data: list[np.ndarray] = []
    y_data: list[np.ndarray] = []

    for idx in range(timesteps, len(values)):
        x_data.append(values[idx - timesteps : idx])
        y_data.append(values[idx])

    if not x_data:
        return np.empty((0, timesteps, 4), dtype=np.float32), np.empty((0, 4), dtype=np.float32)

    return np.asarray(x_data, dtype=np.float32), np.asarray(y_data, dtype=np.float32)


def _save_meta(meta_path: Path, data: dict) -> None:
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


def _load_meta(meta_path: Path) -> dict:
    return json.loads(meta_path.read_text(encoding="utf-8"))


def _to_prediction_row(
    ticker: str,
    source_bucket: pd.Timestamp,
    predicted_bucket: pd.Timestamp,
    predicted_values: np.ndarray,
    model_path: Path,
    model_version: str,
) -> tuple:
    pred_open, pred_high, pred_low, pred_close = [float(x) for x in predicted_values]
    return (
        ticker,
        source_bucket.to_pydatetime(),
        predicted_bucket.to_pydatetime(),
        pred_open,
        pred_high,
        pred_low,
        pred_close,
        str(model_path),
        model_version,
    )


def upsert_prediction(conn: psycopg2.extensions.connection, row: tuple) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mart.lstm_daily_predictions (
                ticker,
                source_bucket,
                predicted_bucket,
                predicted_open,
                predicted_high,
                predicted_low,
                predicted_close,
                model_path,
                model_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
                source_bucket = EXCLUDED.source_bucket,
                predicted_bucket = EXCLUDED.predicted_bucket,
                predicted_open = EXCLUDED.predicted_open,
                predicted_high = EXCLUDED.predicted_high,
                predicted_low = EXCLUDED.predicted_low,
                predicted_close = EXCLUDED.predicted_close,
                model_path = EXCLUDED.model_path,
                model_version = EXCLUDED.model_version,
                updated_at = NOW()
            """,
            row,
        )


def _train_for_ticker(
    conn: psycopg2.extensions.connection,
    ticker: str,
    timesteps: int,
    epochs: int,
    batch_size: int,
    model_dir: Path,
    min_rows: int,
    verbose: int,
    model_version: str,
) -> str:
    _, _, _, _, _, EarlyStopping, ReduceLROnPlateau = _load_tf()

    candles = load_daily_candles(conn, ticker)
    candles = candles.dropna(subset=["bucket", *FEATURE_COLUMNS]).copy()
    candles["bucket"] = pd.to_datetime(candles["bucket"], utc=True, errors="coerce")
    candles = candles.dropna(subset=["bucket"]).sort_values("bucket")

    if len(candles) < max(min_rows, timesteps + 1):
        return f"SKIP {ticker}: мало данных ({len(candles)})"

    values = candles.loc[:, FEATURE_COLUMNS].astype("float32").to_numpy()
    mins, maxs, ranges = _fit_scaler(values)
    scaled_values = _scale(values, mins, ranges)
    x_train, y_train = _create_sequences(scaled_values, timesteps)

    if len(x_train) < 10:
        return f"SKIP {ticker}: недостаточно окон ({len(x_train)})"

    model = build_model(timesteps)
    callbacks = [
        EarlyStopping(monitor="val_loss", patience=15, restore_best_weights=True),
        ReduceLROnPlateau(monitor="val_loss", factor=0.7, patience=5, min_lr=0.00001),
    ]

    model.fit(
        x_train,
        y_train,
        epochs=epochs,
        batch_size=batch_size,
        validation_split=0.2,
        callbacks=callbacks,
        verbose=verbose,
        shuffle=False,
    )

    model_dir.mkdir(parents=True, exist_ok=True)
    weights_path = model_dir / f"{ticker.upper()}.weights.h5"
    meta_path = model_dir / f"{ticker.upper()}.meta.json"
    model.save_weights(weights_path)

    source_bucket = candles["bucket"].iloc[-1]
    predicted_bucket = source_bucket + pd.Timedelta(days=1)
    last_window = scaled_values[-timesteps:].reshape(1, timesteps, 4)
    predicted_scaled = model.predict(last_window, verbose=0)[0]
    predicted_values = _inverse_scale(predicted_scaled, mins, ranges)

    meta = {
        "ticker": ticker.upper(),
        "timesteps": timesteps,
        "features": list(FEATURE_COLUMNS),
        "mins": mins.tolist(),
        "maxs": maxs.tolist(),
        "ranges": ranges.tolist(),
        "last_source_bucket": source_bucket.isoformat(),
        "model_version": model_version,
    }
    _save_meta(meta_path, meta)

    row = _to_prediction_row(
        ticker=ticker.upper(),
        source_bucket=source_bucket,
        predicted_bucket=predicted_bucket,
        predicted_values=predicted_values,
        model_path=weights_path,
        model_version=model_version,
    )
    upsert_prediction(conn, row)
    return f"OK {ticker}: модель обучена, прогноз обновлен"


def train_and_predict(
    db_config: DbConfig,
    ticker: str | None,
    timesteps: int,
    epochs: int,
    batch_size: int,
    model_dir: Path,
    min_rows: int = 120,
    verbose: int = 1,
    model_version: str = "lstm_v1",
) -> RunStats:
    conn = psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.dbname,
        user=db_config.user,
        password=db_config.password,
    )

    stats = RunStats()
    try:
        ensure_prediction_table(conn)
        tickers = load_tickers(conn, ticker=ticker)
        stats.total_tickers = len(tickers)

        for tck in tickers:
            try:
                result = _train_for_ticker(
                    conn=conn,
                    ticker=tck,
                    timesteps=timesteps,
                    epochs=epochs,
                    batch_size=batch_size,
                    model_dir=model_dir,
                    min_rows=min_rows,
                    verbose=verbose,
                    model_version=model_version,
                )
                if result.startswith("SKIP"):
                    stats.skipped_tickers += 1
                else:
                    stats.success_tickers += 1
                print(result)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                stats.errors_tickers += 1
                print(f"ERROR {tck}: {exc}")
    finally:
        conn.close()

    return stats


def _predict_for_ticker_from_saved_model(
    conn: psycopg2.extensions.connection,
    ticker: str,
    model_dir: Path,
    fallback_timesteps: int,
    model_version: str,
) -> str:
    ticker_up = ticker.upper()
    weights_path = model_dir / f"{ticker_up}.weights.h5"
    meta_path = model_dir / f"{ticker_up}.meta.json"

    if not weights_path.exists() or not meta_path.exists():
        return f"SKIP {ticker_up}: нет сохраненной модели"

    meta = _load_meta(meta_path)
    timesteps = int(meta.get("timesteps", fallback_timesteps))
    mins = np.asarray(meta.get("mins", [0, 0, 0, 0]), dtype=np.float32)
    ranges = np.asarray(meta.get("ranges", [1, 1, 1, 1]), dtype=np.float32)

    candles = load_daily_candles(conn, ticker_up)
    candles = candles.dropna(subset=["bucket", *FEATURE_COLUMNS]).copy()
    candles["bucket"] = pd.to_datetime(candles["bucket"], utc=True, errors="coerce")
    candles = candles.dropna(subset=["bucket"]).sort_values("bucket")

    if len(candles) < timesteps:
        return f"SKIP {ticker_up}: мало данных для инференса ({len(candles)})"

    model = build_model(timesteps)
    model.load_weights(weights_path)

    values = candles.loc[:, FEATURE_COLUMNS].astype("float32").to_numpy()
    scaled_values = _scale(values, mins, ranges)

    source_bucket = candles["bucket"].iloc[-1]
    predicted_bucket = source_bucket + pd.Timedelta(days=1)
    last_window = scaled_values[-timesteps:].reshape(1, timesteps, 4)
    predicted_scaled = model.predict(last_window, verbose=0)[0]
    predicted_values = _inverse_scale(predicted_scaled, mins, ranges)

    row = _to_prediction_row(
        ticker=ticker_up,
        source_bucket=source_bucket,
        predicted_bucket=predicted_bucket,
        predicted_values=predicted_values,
        model_path=weights_path,
        model_version=model_version,
    )
    upsert_prediction(conn, row)
    return f"OK {ticker_up}: прогноз обновлен"


def predict_with_trained_models(
    db_config: DbConfig,
    ticker: str | None,
    model_dir: Path,
    fallback_timesteps: int = 60,
    model_version: str = "lstm_v1",
) -> RunStats:
    conn = psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.dbname,
        user=db_config.user,
        password=db_config.password,
    )

    stats = RunStats()
    try:
        ensure_prediction_table(conn)
        tickers = load_tickers(conn, ticker=ticker)
        stats.total_tickers = len(tickers)

        for tck in tickers:
            try:
                result = _predict_for_ticker_from_saved_model(
                    conn=conn,
                    ticker=tck,
                    model_dir=model_dir,
                    fallback_timesteps=fallback_timesteps,
                    model_version=model_version,
                )
                if result.startswith("SKIP"):
                    stats.skipped_tickers += 1
                else:
                    stats.success_tickers += 1
                print(result)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                stats.errors_tickers += 1
                print(f"ERROR {tck}: {exc}")
    finally:
        conn.close()

    return stats

