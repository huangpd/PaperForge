"""Derive dataset-level lineage and catalog entries from dbt's manifest.json (no SQL parsing of our own)."""
import json
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from paperforge.lineage.base import Edge, LineageEmitter
from paperforge.lineage.datasets import ensure_dataset, layer_of


def _model_id(n: dict) -> str:
    return f"{n['schema']}.{n.get('alias') or n['name']}"


def _source_id(n: dict) -> str:
    return f"{n['schema']}.{n['name']}"


def parse_manifest(path: str | Path) -> tuple[list[Edge], dict[str, dict]]:
    m = json.loads(Path(path).read_text())
    ids: dict[str, str] = {}
    info: dict[str, dict] = {}
    for uid, n in m["nodes"].items():
        if n["resource_type"] == "model":
            ids[uid] = _model_id(n)
            meta = {**(n.get("meta") or {}), **((n.get("config") or {}).get("meta") or {})}
            info[ids[uid]] = {"description": n.get("description") or None, "materialization": (n.get("config") or {}).get("materialized"),
                              "kind": "view" if (n.get("config") or {}).get("materialized") == "view" else "table",
                              "columns": [{"name": c, "description": v.get("description") or None} for c, v in (n.get("columns") or {}).items()],
                              "meta": meta, "dbt_unique_id": uid}
    for uid, s in m["sources"].items():
        ids[uid] = _source_id(s)
    edges = []
    for uid, parents in m["parent_map"].items():
        if uid not in ids or not uid.startswith("model."):
            continue
        for p in parents:
            if p in ids:
                edges.append(Edge(ids[p], ids[uid], m["nodes"][uid]["unique_id"].split(".", 2)[2], "dbt"))
    return edges, info


def sync_dbt(conn: psycopg.Connection, emitter: LineageEmitter, manifest_path: str | Path) -> dict:
    edges, info = parse_manifest(manifest_path)
    with conn.transaction():
        for ds, i in info.items():
            ensure_dataset(conn, ds)
            meta = i["meta"]
            conn.execute(
                """UPDATE meta.dataset SET layer=%s, kind=%s, materialization=%s,
                          description=coalesce(%s, description), grain=coalesce(%s, grain), primary_key=coalesce(%s, primary_key),
                          unique_constraints=coalesce(%s, unique_constraints), update_strategy=coalesce(%s, update_strategy)
                   WHERE dataset_id=%s""",
                (layer_of(ds), i["kind"], i["materialization"], i["description"], meta.get("grain"), meta.get("primary_key"),
                 meta.get("unique_constraints"), meta.get("update_strategy"), ds))
    removed = emitter.replace_edges("dbt", edges)
    return {"models": len(info), "edges": len(edges), "removed_stale_edges": removed}
