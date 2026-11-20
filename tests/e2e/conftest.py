"""One fully isolated warehouse for the end-to-end tests: own PostgreSQL database, own Kafka topics/consumer group,
own dbt target directory and data directory. Nothing here can touch real papers (fixture DOIs use the 10.99999 prefix)."""
import os
import uuid
from pathlib import Path

import psycopg
import pytest

from paperforge.config.settings import get_settings
from paperforge.connectors.base import Page, SourceConnector, SourceRecord, WorkUnit
from paperforge.db import migrate
from paperforge.ingestion.parquet_export import export_batch
from paperforge.ingestion.runner import IngestionRunner
from paperforge.processing.normalizer import normalize_crossref
from paperforge.storage.local import LocalStorage
from tests.fixtures.papers import payload

ROOT = Path(__file__).resolve().parents[2]
PUBLISHERS = [("42", "Test Press"), ("77", "Open Science Ltd"), ("91", "Bio Publishing")]
JOURNALS = [("Journal of Tests", ["1111-1111", "2222-2222"]), ("Computational Testing", ["3333-3333"]), ("Bio Reports", ["4444-4444"])]


def paper(n, version=1, **over):
    pub, (jt, issns) = PUBLISHERS[n % 3], JOURNALS[n % 3]
    p = payload(n, DOI=f"10.99999/E2E.{n}",   # source casing; the pipeline normalizes it to lower case
                indexed=f"2026-0{version}-01T00:00:00Z", title=f"Study {n} of protein folding v{version}",
                publisher=pub[1], member=pub[0], ISSN=issns if version == 1 or n % 2 else issns[:1],
                issued={"date-parts": [[2015 + n % 8, 1 + n % 12, 1 + n % 28]]}, **{"container-title": [jt]})
    if n % 4 == 0:
        p.pop("abstract")
    if n % 5 == 0:
        p["author"] = []
    p.update(over)
    return p


class E2EConnector(SourceConnector):
    """12+ papers, 3 of them with 2 versions (the older one arriving late), one invalid DOI, one duplicate."""
    name = "crossref"

    def plan(self, **_):
        return [WorkUnit("u")]

    def fetch(self, unit, cursor=None, on_request=None):
        recs = [paper(n) for n in range(1, 25)]
        recs += [paper(2, version=3), paper(3, version=3), paper(4, version=2)]           # newer versions
        recs += [paper(2, version=1)]                                                     # late arrival of an old version
        recs += [paper(1)]                                                                # exact duplicate
        recs += [paper(99, DOI="not-a-valid-doi")]                                        # fails the DOI rule -> quarantine
        out = []
        for p in recs:
            n = normalize_crossref(p)
            out.append(SourceRecord("crossref", n.doi, n.doi, n.source_updated_at, p))
        yield Page(out[:15], "1")
        yield Page(out[15:], None)


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    s = get_settings()
    name = f"pf_e2e_{uuid.uuid4().hex[:8]}"
    admin = psycopg.connect(s.pg_dsn, autocommit=True)
    admin.execute(f'CREATE DATABASE "{name}"')
    dsn = s.pg_dsn.replace(f"dbname={s.pg_db}", f"dbname={name}")
    tmp = tmp_path_factory.mktemp("e2e")
    suffix = uuid.uuid4().hex[:8]
    mp = pytest.MonkeyPatch()
    for k, v in {"PG_DB": name, "DATA_DIR": str(tmp / "data"), "PROJECT_DIR": str(ROOT), "DBT_TARGET_PATH": str(tmp / "dbt-target"),
                 "KAFKA_RAW_TOPIC": f"e2e.raw.{suffix}", "KAFKA_DLQ_TOPIC": f"e2e.dlq.{suffix}", "KAFKA_GROUP": f"e2e-{suffix}"}.items():
        mp.setenv(k, v)
    for k in ("PF_DAG_ID", "PF_RUN_ID", "PF_TASK_ID", "PF_TRY_NUMBER", "PF_TRIGGER"):
        mp.delenv(k, raising=False)
    with psycopg.connect(dsn, autocommit=True) as c:
        migrate(c)
    storage = LocalStorage(tmp / "data")
    runner = IngestionRunner(E2EConnector(), storage, dsn=dsn)
    bid = runner.start("bootstrap", E2EConnector().plan(), {}, None)
    res = runner.run(bid)
    export_batch(storage, bid, dsn)
    yield {"dsn": dsn, "name": name, "batch_id": bid, "ingest": res, "tmp": tmp, "storage": storage}
    mp.undo()
    admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
    admin.close()
