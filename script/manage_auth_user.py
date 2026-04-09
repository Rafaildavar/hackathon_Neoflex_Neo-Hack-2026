from __future__ import annotations

import argparse
import getpass
from pathlib import Path

import psycopg2

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


def db_conn() -> psycopg2.extensions.connection:
    return connect_db()


def normalize_email(email: str) -> str:
    return email.strip().lower()


def cmd_register(conn: psycopg2.extensions.connection, email: str, password: str) -> None:
    email = normalize_email(email)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO auth.users (email, password_hash, updated_at)
                VALUES (%s, crypt(%s, gen_salt('bf')), NOW())
                ON CONFLICT (email) DO UPDATE
                SET password_hash = crypt(%s, gen_salt('bf')),
                    updated_at = NOW(),
                    is_active = TRUE
                RETURNING user_id, email, created_at, updated_at, is_active
                """,
                (email, password, password),
            )
            row = cur.fetchone()
    print(
        f"Registered/updated user: id={row[0]}, email={row[1]}, "
        f"created_at={row[2]}, updated_at={row[3]}, active={row[4]}"
    )


def cmd_login_check(
    conn: psycopg2.extensions.connection, email: str, password: str
) -> None:
    email = normalize_email(email)
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
    if row is None:
        print("Login check: FAILED")
    else:
        print(f"Login check: OK (user_id={row[0]})")


def cmd_list(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id, email, created_at, updated_at, is_active
            FROM auth.users
            ORDER BY user_id
            """
        )
        rows = cur.fetchall()
    print("Users:")
    for row in rows:
        print(
            f"  id={row[0]}, email={row[1]}, created_at={row[2]}, "
            f"updated_at={row[3]}, active={row[4]}"
        )


def cmd_deactivate(conn: psycopg2.extensions.connection, email: str) -> None:
    email = normalize_email(email)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auth.users
                SET is_active = FALSE,
                    updated_at = NOW()
                WHERE email = %s
                RETURNING user_id
                """,
                (email,),
            )
            row = cur.fetchone()
    if row is None:
        print("No user found for deactivation.")
    else:
        print(f"User deactivated: user_id={row[0]}")


def cmd_activate(conn: psycopg2.extensions.connection, email: str) -> None:
    email = normalize_email(email)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auth.users
                SET is_active = TRUE,
                    updated_at = NOW()
                WHERE email = %s
                RETURNING user_id
                """,
                (email,),
            )
            row = cur.fetchone()
    if row is None:
        print("No user found for activation.")
    else:
        print(f"User activated: user_id={row[0]}")


def cmd_set_password(
    conn: psycopg2.extensions.connection, email: str, password: str
) -> None:
    email = normalize_email(email)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auth.users
                SET password_hash = crypt(%s, gen_salt('bf')),
                    updated_at = NOW()
                WHERE email = %s
                RETURNING user_id
                """,
                (password, email),
            )
            row = cur.fetchone()
    if row is None:
        print("No user found to update password.")
    else:
        print(f"Password updated: user_id={row[0]}")


def cmd_delete(conn: psycopg2.extensions.connection, email: str) -> None:
    email = normalize_email(email)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM auth.users
                WHERE email = %s
                RETURNING user_id
                """,
                (email,),
            )
            row = cur.fetchone()
    if row is None:
        print("No user found to delete.")
    else:
        print(f"User deleted: user_id={row[0]}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage users for DB auth.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_reg = sub.add_parser("register", help="Create/update user with hashed password.")
    p_reg.add_argument("--email", required=True)
    p_reg.add_argument("--password", default=None)

    p_chk = sub.add_parser("check-login", help="Validate email/password against DB hash.")
    p_chk.add_argument("--email", required=True)
    p_chk.add_argument("--password", default=None)

    sub.add_parser("list", help="List users (without password hashes).")

    p_deactivate = sub.add_parser("deactivate", help="Deactivate user by email.")
    p_deactivate.add_argument("--email", required=True)

    p_activate = sub.add_parser("activate", help="Activate user by email.")
    p_activate.add_argument("--email", required=True)

    p_set_password = sub.add_parser(
        "set-password", help="Set new password for an existing user."
    )
    p_set_password.add_argument("--email", required=True)
    p_set_password.add_argument("--password", default=None)

    p_delete = sub.add_parser("delete", help="Delete user by email.")
    p_delete.add_argument("--email", required=True)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    conn = db_conn()
    try:
        if args.command == "register":
            password = args.password or getpass.getpass("Password: ")
            cmd_register(conn, args.email, password)
        elif args.command == "check-login":
            password = args.password or getpass.getpass("Password: ")
            cmd_login_check(conn, args.email, password)
        elif args.command == "list":
            cmd_list(conn)
        elif args.command == "deactivate":
            cmd_deactivate(conn, args.email)
        elif args.command == "activate":
            cmd_activate(conn, args.email)
        elif args.command == "set-password":
            password = args.password or getpass.getpass("New password: ")
            cmd_set_password(conn, args.email, password)
        elif args.command == "delete":
            cmd_delete(conn, args.email)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
