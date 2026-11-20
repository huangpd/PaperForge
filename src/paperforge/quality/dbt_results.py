"""Import dbt test outcomes (target/run_results.json) into meta.quality_result so dbt tests share the same rule records."""
import json
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb


def _dimension(test_name: str) -> str:
    n = test_name.lower()
    for k, d in [("unique", "uniqueness"), ("not_null", "completeness"), ("relationships", "referential_integrity"),
                 ("regex", "validity"), ("accepted", "validity"), ("non_negative", "validity"), ("range", "validity"),
                 ("rate_at_least", "completeness"), ("reconcile", "consistency"), ("equals", "consistency"),
                 ("timeline", "consistency")]:
        if k in n:
            return d
    return "consistency"


def load_run_results(conn: psycopg.Connection, path: str) -> dict:
    data = json.loads(Path(path).read_text())
    run_id, now = uuid.uuid4(), datetime.now(UTC)
    n = {"pass": 0, "warn": 0, "fail": 0}
    with conn.transaction():
        for r in data["results"]:
            uid = r["unique_id"]
            if not uid.startswith("test."):
                continue
            name = "dbt." + uid.split(".")[2]
            name = re.sub(r"\.[0-9a-f]{10}$", "", name)[:120]
            failures = int(r.get("failures") or 0)
            status = r["status"]
            warn = status == "warn"
            severity = "warning" if warn or (status == "pass" and "rate_at_least" in name) else "error"
            conn.execute("INSERT INTO meta.quality_rule(rule_id, version, dimension, severity, dataset, description, engine) "
                         "VALUES (%s,1,%s,%s,'dbt',%s,'dbt') ON CONFLICT (rule_id, version) DO UPDATE SET severity=EXCLUDED.severity, description=EXCLUDED.description",
                         (name, _dimension(name), severity, "dbt test: " + name.removeprefix("dbt.").replace("_", " ")))
            conn.execute("INSERT INTO meta.quality_result(run_id, rule_id, rule_version, dataset, severity, total_rows, failed_rows, passed, details, run_at) "
                         "VALUES (%s,%s,1,'dbt',%s,0,%s,%s,%s,%s)",
                         (run_id, name, severity, failures, status in ("pass", "success"), Jsonb({"status": status, "unique_id": uid}), now))
            n["pass" if status in ("pass", "success") else ("warn" if warn else "fail")] += 1
    return n
