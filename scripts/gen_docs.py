"""Generate docs/data_dictionary.md and docs/er.md from the live database (meta.dataset catalog + information_schema).
Run after `make dbt`:  uv run python scripts/gen_docs.py"""
from pathlib import Path

from paperforge.db import connect

ROOT = Path(__file__).resolve().parents[1]
LAYER_TITLE = {"source": "Sources", "raw": "Raw archive", "kafka": "Kafka topics", "ods": "ODS — raw data layer", "dwd": "DWD — standardized detail",
               "stg": "dbt staging (views)", "int": "dbt intermediate", "dim": "DIM — shared dimensions", "dws": "DWS — summaries",
               "ads": "ADS — application layer", "meta": "META — governance"}
ORDER = list(LAYER_TITLE)


def dictionary(c) -> str:
    ds = c.execute("""SELECT dataset_id, layer, kind, description, grain, primary_key, unique_constraints, update_strategy, materialization,
                             row_count, last_data_at, columns FROM meta.dataset ORDER BY dataset_id""").fetchall()
    ups = {}
    for u, d in c.execute("SELECT upstream, downstream FROM meta.lineage_edge ORDER BY 1, 2"):
        ups.setdefault(d, []).append(u)
    out = ["# Data dictionary", "", "Generated from `meta.dataset` (catalog) and `meta.lineage_edge` by `scripts/gen_docs.py`; row counts are from the last `catalog refresh`.", ""]
    for layer in ORDER:
        rows = [r for r in ds if r[1] == layer]
        if not rows:
            continue
        out += [f"## {LAYER_TITLE[layer]}", ""]
        for (did, _l, kind, desc, grain, pk, uq, upd, mat, n, last, cols) in rows:
            out += [f"### `{did}`", "", f"{desc or '_no description_'}", "",
                    f"- **Kind:** {kind}{f' ({mat})' if mat else ''}", f"- **Grain:** {grain or '—'}", f"- **Primary key:** `{pk or '—'}`",
                    f"- **Unique constraints:** {uq or '—'}", f"- **Update strategy:** {upd or '—'}",
                    f"- **Upstream:** {', '.join(f'`{u}`' for u in ups.get(did, [])) or '—'}",
                    f"- **Rows:** {n:,}" if n is not None else "- **Rows:** —"]
            if cols and any("type" in x for x in cols):
                out += ["", "| column | type | null | description |", "|---|---|---|---|"]
                out += [f"| `{x['name']}` | {x.get('type', '')} | {'yes' if x.get('nullable') else 'no'} | {(x.get('description') or '').replace('|', '/')} |" for x in cols]
            out.append("")
    return "\n".join(out)


def er(c) -> str:
    out = ["# Entity-relationship diagrams", "", "Generated from the foreign keys and primary keys in PostgreSQL (`scripts/gen_docs.py`). Only key columns are shown; see the data dictionary for all columns.", ""]
    for title, schemas in [("Raw layer and ingestion bookkeeping (ods, meta.sync_checkpoint)", ("ods",)), ("Standardized layer (dwd)", ("dwd",)), ("Governance (meta)", ("meta",))]:
        tabs = [r[0] for r in c.execute("SELECT table_schema||'.'||table_name FROM information_schema.tables WHERE table_schema = ANY(%s) AND table_type='BASE TABLE' ORDER BY 1", (list(schemas),))]
        if title.startswith("Raw"):
            tabs.append("meta.sync_checkpoint")
        fks = c.execute("""SELECT con.conrelid::regclass::text, con.confrelid::regclass::text FROM pg_constraint con
                           WHERE con.contype='f' AND con.conrelid::regclass::text ~ '^(ods|dwd|meta)\\.' ORDER BY 1, 2""").fetchall()
        fks = [(a, b) for a, b in fks if a in tabs and b in tabs]
        out += [f"## {title}", "", "```mermaid", "erDiagram"]
        for t in tabs:
            s, n = t.split(".")
            pk = {r[0] for r in c.execute("""SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum = ANY(i.indkey)
                                              WHERE i.indisprimary AND i.indrelid = %s::regclass""", (t,))}
            fk = {r[0] for r in c.execute("""SELECT a.attname FROM pg_constraint con JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum = ANY(con.conkey)
                                              WHERE con.contype='f' AND con.conrelid = %s::regclass""", (t,))}
            cols = c.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (s, n)).fetchall()
            keep = [(cn, dt) for cn, dt in cols if cn in pk or cn in fk or cn in ("primary_doi", "doi_norm", "content_hash", "payload_hash", "status")]
            out.append(f"  {n} {{")
            for cn, dt in keep:
                tag = "PK" if cn in pk else "FK" if cn in fk else ""
                out.append(f"    {dt.replace(' ', '_').replace('[]', 'arr')} {cn} {tag}".rstrip())
            out.append("  }")
        for a, b in fks:
            out.append(f"  {b.split('.')[1]} ||--o{{ {a.split('.')[1]} : \"{a.split('.')[1]}→{b.split('.')[1]}\"")
        out += ["```", ""]
    out += ["## Logical flow between layers", "", "```mermaid", "flowchart LR", "  CR[(Crossref API / offline dump)] --> RAW[raw JSONL + parquet]", "  RAW --> ODS[ods.crossref_works]", "  ODS --> K{{Kafka paper.raw.events}}",
            "  K --> DWD[dwd.paper / version / author / reference / identifier]", "  RAW -. batch replay .-> DWD", "  DWD --> DIM[dim.journal / publisher / date]", "  DWD --> DWS[dws.* summaries]", "  DIM --> DWS", "  DWS --> ADS[ads.paper_search / overview / quality]", "  DWD --> ADS", "```", ""]
    return "\n".join(out)


if __name__ == "__main__":
    with connect() as c:
        (ROOT / "docs/data_dictionary.md").write_text(dictionary(c))
        (ROOT / "docs/er.md").write_text(er(c))
    print("wrote docs/data_dictionary.md and docs/er.md")
