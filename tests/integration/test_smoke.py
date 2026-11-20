"""Stage-0 smoke tests: every component can actually be reached/used."""
import uuid

import duckdb
import pandera.polars as pa
import polars as pl
import psycopg
import pytest
from confluent_kafka import Consumer, Producer

from paperforge.config.settings import get_settings

pytestmark = pytest.mark.integration
S = get_settings()


def test_postgres():
    with psycopg.connect(S.pg_dsn) as c:
        assert c.execute("select 1").fetchone() == (1,)


def test_kafka_roundtrip():
    from paperforge.events.kafka_bus import KafkaEventBus
    topic = f"smoke.{uuid.uuid4().hex[:8]}"
    KafkaEventBus(S.kafka_bootstrap).ensure_topics({topic: 1})   # broker auto-create is disabled on purpose
    p = Producer({"bootstrap.servers": S.kafka_bootstrap})
    p.produce(topic, key=b"k", value=b"v")
    assert p.flush(15) == 0
    c = Consumer({"bootstrap.servers": S.kafka_bootstrap, "group.id": topic,
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    msg = None
    for _ in range(30):
        msg = c.poll(1.0)
        if msg is not None and not msg.error():
            break
    c.close()
    assert msg is not None and msg.value() == b"v"


def test_polars_pandera():
    schema = pa.DataFrameSchema({"doi": pa.Column(str, pa.Check.str_startswith("10."))})
    schema.validate(pl.DataFrame({"doi": ["10.1/x"]}))
    with pytest.raises(pa.errors.SchemaError):
        schema.validate(pl.DataFrame({"doi": ["bad"]}))


def test_duckdb_reads_crossref_parquet():
    import glob
    files = sorted(glob.glob(f"{S.crossref_parquet_dir}/part-00000.parquet"))
    if not files:
        pytest.skip("crossref parquet not configured")
    n = duckdb.sql(f"select count(*) from read_parquet('{files[0]}')").fetchall()[0][0]
    assert n > 0
