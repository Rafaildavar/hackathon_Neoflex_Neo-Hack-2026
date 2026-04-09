from __future__ import annotations

import os
from typing import Any

import psycopg2
import psycopg2.extensions


DEFAULT_POSTGRES_HOST = "127.0.0.1"
DEFAULT_POSTGRES_PORT = 15432
DEFAULT_POSTGRES_DB = "moex_dwh"
DEFAULT_POSTGRES_USER = "moex"
DEFAULT_POSTGRES_PASSWORD = "moex_pass"


def _decode_connection_error(exc: Exception) -> str:
    if isinstance(exc, UnicodeDecodeError) and isinstance(exc.object, (bytes, bytearray)):
        raw = bytes(exc.object)
        for encoding in ("utf-8", "cp1251", "latin1"):
            try:
                return raw.decode(encoding).strip()
            except UnicodeDecodeError:
                continue
        return repr(raw)
    return str(exc)


def _db_settings() -> dict[str, Any]:
    return {
        "host": os.getenv("POSTGRES_HOST", DEFAULT_POSTGRES_HOST).strip(),
        "port": int(os.getenv("POSTGRES_PORT", str(DEFAULT_POSTGRES_PORT))),
        "dbname": os.getenv("POSTGRES_DB", DEFAULT_POSTGRES_DB).strip(),
        "user": os.getenv("POSTGRES_USER", DEFAULT_POSTGRES_USER).strip(),
        "password": os.getenv("POSTGRES_PASSWORD", DEFAULT_POSTGRES_PASSWORD),
    }


def _candidate_hosts_and_ports(host: str, port: int) -> list[tuple[str, int]]:
    # On Windows, localhost:5432 is often occupied by local PostgreSQL services.
    # We try a small fallback matrix that includes the project's default Docker port.
    candidates = [(host, port)]

    if host == "localhost":
        candidates.append(("127.0.0.1", port))
    elif host == "127.0.0.1":
        candidates.append(("localhost", port))

    if port == 5432:
        candidates.append((host, DEFAULT_POSTGRES_PORT))
        if host == "localhost":
            candidates.append(("127.0.0.1", DEFAULT_POSTGRES_PORT))
        elif host == "127.0.0.1":
            candidates.append(("localhost", DEFAULT_POSTGRES_PORT))

    deduped: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for candidate in candidates:
        if candidate not in seen:
            deduped.append(candidate)
            seen.add(candidate)
    return deduped


def connect_db() -> psycopg2.extensions.connection:
    settings = _db_settings()
    host = str(settings["host"])
    port = int(settings["port"])
    dbname = str(settings["dbname"])
    user = str(settings["user"])
    password = str(settings["password"])

    errors: list[str] = []
    for candidate_host, candidate_port in _candidate_hosts_and_ports(host, port):
        try:
            return psycopg2.connect(
                host=candidate_host,
                port=candidate_port,
                dbname=dbname,
                user=user,
                password=password,
            )
        except Exception as exc:
            message = _decode_connection_error(exc)
            errors.append(f"{candidate_host}:{candidate_port} -> {message}")

    attempted = ", ".join(f"{h}:{p}" for h, p in _candidate_hosts_and_ports(host, port))
    error_text = "; ".join(errors)
    raise RuntimeError(
        "Database connection failed. "
        f"Tried: {attempted}. "
        "Hint: if local PostgreSQL occupies port 5432, use Docker DB on port 15432. "
        f"Details: {error_text}"
    )

