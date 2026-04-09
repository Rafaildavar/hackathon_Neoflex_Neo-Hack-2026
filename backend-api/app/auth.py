from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from .db import read_cursor, write_cursor
from .schemas import AuthRequest, AuthResponse

router = APIRouter(prefix="/auth", tags=["auth"])


def normalize_email(email: str) -> str:
    return email.strip().lower()


@router.post("/register", response_model=AuthResponse, status_code=status.HTTP_201_CREATED)
def register(payload: AuthRequest) -> AuthResponse:
    email = normalize_email(payload.email)
    password = payload.password

    if "@" not in email:
        raise HTTPException(status_code=400, detail="Введите корректный email.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Пароль должен быть не менее 8 символов.")

    with write_cursor() as cur:
        cur.execute("SELECT 1 FROM auth.users WHERE email = %s", (email,))
        if cur.fetchone() is not None:
            raise HTTPException(status_code=409, detail="Пользователь с таким email уже существует.")

        cur.execute(
            """
            INSERT INTO auth.users (email, password_hash, updated_at)
            VALUES (%s, crypt(%s, gen_salt('bf')), NOW())
            """,
            (email, password),
        )

    return AuthResponse(email=email)


@router.post("/login", response_model=AuthResponse)
def login(payload: AuthRequest) -> AuthResponse:
    email = normalize_email(payload.email)
    password = payload.password

    if not email or not password:
        raise HTTPException(status_code=400, detail="Нужны email и пароль.")

    with read_cursor() as cur:
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

    if row is None:
        raise HTTPException(status_code=401, detail="Неверный email или пароль.")

    return AuthResponse(email=email)
