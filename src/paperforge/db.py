from pathlib import Path

import psycopg

from paperforge.config.settings import Settings, get_settings

MIGRATIONS = Path(__file__).resolve().parents[2] / "migrations"


def connect(dsn: str | None = None) -> psycopg.Connection:
    return psycopg.connect(dsn or get_settings().pg_dsn, autocommit=True)


def migrate(conn: psycopg.Connection) -> list[str]:
    conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    conn.execute("CREATE TABLE IF NOT EXISTS meta.schema_migrations (name text PRIMARY KEY, applied_at timestamptz DEFAULT now())")
    done = {r[0] for r in conn.execute("SELECT name FROM meta.schema_migrations")}
    applied = []
    for f in sorted(MIGRATIONS.glob("*.sql")):
        if f.name in done:
            continue
        with conn.transaction():
            conn.execute(f.read_text())
            conn.execute("INSERT INTO meta.schema_migrations(name) VALUES (%s)", (f.name,))
        applied.append(f.name)
    return applied
