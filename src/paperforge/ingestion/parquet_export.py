"""Derive columnar Parquet from immutable raw JSONL. Parquet is regenerable and never the source of truth."""
import hashlib
import json

import polars as pl

from paperforge.db import connect
from paperforge.storage.base import RawStorage


def export_batch(storage: RawStorage, batch_id: str, dsn: str | None = None) -> list[str]:
    with connect(dsn) as c:
        files = [r[0] for r in c.execute("SELECT path FROM ods.raw_file_manifest WHERE batch_id=%s ORDER BY path", (batch_id,))]
    out = []
    for path in files:
        rows = []
        for line in storage.read(path).splitlines():
            o = json.loads(line)
            rows.append({"source": o["source"], "doi": o["doi"], "source_updated_at": o["source_updated_at"],
                         "payload_sha256": hashlib.sha256(json.dumps(o["payload"], sort_keys=True).encode()).hexdigest(),
                         "payload": json.dumps(o["payload"], ensure_ascii=False)})
        dest = path.replace("raw/", "parquet/", 1).replace(".jsonl", ".parquet")
        import io
        buf = io.BytesIO()
        pl.DataFrame(rows).write_parquet(buf, compression="zstd")
        storage.write(dest, buf.getvalue(), overwrite=True)  # derived data: regeneration may overwrite
        out.append(dest)
    return out
