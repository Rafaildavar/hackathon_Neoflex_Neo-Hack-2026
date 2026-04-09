from __future__ import annotations

import os
from pathlib import Path

try:
    from db_connection import connect_db
except ImportError:
    from script.db_connection import connect_db

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


if load_dotenv is not None:
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)


DDL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE IF NOT EXISTS auth.users (
    user_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CHECK (position('@' in email) > 1),
    CHECK (char_length(password_hash) > 20)
);

CREATE INDEX IF NOT EXISTS idx_auth_users_email
    ON auth.users (email);
"""


def main() -> int:
    conn = connect_db()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
    finally:
        conn.close()
    print("Auth schema initialized: auth.users is ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
