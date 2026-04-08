from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import settings

engine = create_engine(settings.sqlalchemy_url, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def ensure_db_objects() -> None:
    project_root = Path(__file__).resolve().parents[2]
    sql_files = [
        project_root / "sql" / "staging" / "create_stg.sql",
        project_root / "sql" / "core" / "create_core.sql",
        project_root / "sql" / "migrations" / "rename_yndx_to_ydex.sql",
        project_root / "sql" / "migrations" / "rebuild_daily_candles_format.sql",
    ]
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS core"))
        for sql_file in sql_files:
            if sql_file.exists():
                conn.execute(text(sql_file.read_text(encoding="utf-8")))


@contextmanager
def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
