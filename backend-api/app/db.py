from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

from .settings import get_settings


def open_connection() -> connection:
    settings = get_settings()
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )


@contextmanager
def read_cursor(dict_rows: bool = False) -> Iterator:
    conn = open_connection()
    cursor_factory = RealDictCursor if dict_rows else None
    cur = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


@contextmanager
def write_cursor(dict_rows: bool = False) -> Iterator:
    conn = open_connection()
    cursor_factory = RealDictCursor if dict_rows else None
    cur = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

