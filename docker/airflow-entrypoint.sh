#!/usr/bin/env bash
set -euo pipefail
# Airflow metadata lives in its own database on the shared PostgreSQL instance (no extra infrastructure).
python - <<'PY'
import os, psycopg2
c = psycopg2.connect(host=os.environ["PF_PG_HOST"], user=os.environ["PF_PG_USER"], password=os.environ["PF_PG_PASSWORD"], dbname="postgres")
c.autocommit = True
cur = c.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname='airflow'")
if not cur.fetchone():
    cur.execute("CREATE DATABASE airflow")
PY
exec airflow standalone
