import uuid

import psycopg
import pytest

from paperforge.config.settings import get_settings
from paperforge.db import migrate


@pytest.fixture
def pg_dsn():
    """Isolated throw-away database per test: never touches real data."""
    s = get_settings()
    name = f"pf_test_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(s.pg_dsn, autocommit=True)
    admin.execute(f'CREATE DATABASE "{name}"')
    dsn = s.pg_dsn.replace(f"dbname={s.pg_db}", f"dbname={name}")
    with psycopg.connect(dsn, autocommit=True) as c:
        migrate(c)
    yield dsn
    admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
    admin.close()
