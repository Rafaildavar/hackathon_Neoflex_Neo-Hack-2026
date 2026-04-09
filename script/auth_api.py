from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, request
from flask_cors import CORS

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


def db_conn():
    return connect_db()


def normalize_email(email: str) -> str:
    return email.strip().lower()


app = Flask(__name__)
CORS(app)


@app.get("/health")
def health() -> tuple[dict[str, str], int]:
    return {"status": "ok"}, 200


@app.post("/auth/register")
def register() -> tuple[dict[str, str], int]:
    body = request.get_json(silent=True) or {}
    email = normalize_email(str(body.get("email", "")))
    password = str(body.get("password", ""))

    if not email or "@" not in email:
        return {"error": "Введите корректный email."}, 400
    if len(password) < 8:
        return {"error": "Пароль должен быть не менее 8 символов."}, 400

    try:
        conn = db_conn()
    except RuntimeError as exc:
        return {"error": str(exc)}, 503
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM auth.users WHERE email = %s", (email,))
                if cur.fetchone() is not None:
                    return {"error": "Пользователь с таким email уже существует."}, 409

                cur.execute(
                    """
                    INSERT INTO auth.users (email, password_hash, updated_at)
                    VALUES (%s, crypt(%s, gen_salt('bf')), NOW())
                    """,
                    (email, password),
                )
    finally:
        conn.close()

    return {"email": email}, 201


@app.post("/auth/login")
def login() -> tuple[dict[str, str], int]:
    body = request.get_json(silent=True) or {}
    email = normalize_email(str(body.get("email", "")))
    password = str(body.get("password", ""))

    if not email or not password:
        return {"error": "Нужны email и пароль."}, 400

    try:
        conn = db_conn()
    except RuntimeError as exc:
        return {"error": str(exc)}, 503
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id
                FROM auth.users
                WHERE email = %s
                  AND is_active = TRUE
                  AND password_hash = crypt(%s, password_hash)
                """,
                (email, password),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        return {"error": "Неверный email или пароль."}, 401
    return {"email": email}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("AUTH_API_PORT", "8001")), debug=False)
