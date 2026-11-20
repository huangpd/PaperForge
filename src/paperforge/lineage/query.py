import psycopg

NODE_CHAIN = ["external.crossref_api", "raw.crossref_jsonl", "ods.crossref_works", "kafka.paper.raw.events", "dwd.paper"]


def trace(conn: psycopg.Connection, dataset: str, direction: str = "up", max_depth: int = 12) -> list[dict]:
    """All edges reachable from `dataset` going upstream (up) or downstream (down)."""
    assert direction in ("up", "down")
    start, nxt = ("downstream", "upstream") if direction == "up" else ("upstream", "downstream")
    rows = conn.execute(f"""
        WITH RECURSIVE walk(upstream, downstream, source, task_id, depth) AS (
            SELECT upstream, downstream, source, task_id, 1 FROM meta.lineage_edge WHERE {start} = %s
          UNION
            SELECT e.upstream, e.downstream, e.source, e.task_id, w.depth + 1
            FROM meta.lineage_edge e JOIN walk w ON e.{start} = w.{nxt} WHERE w.depth < %s)
        SELECT upstream, downstream, source, task_id, min(depth) FROM walk GROUP BY 1,2,3,4 ORDER BY 5, 1, 2""",
        (dataset, max_depth)).fetchall()
    return [{"upstream": u, "downstream": d, "source": s, "task_id": t, "depth": dp} for u, d, s, t, dp in rows]


def path_exists(conn: psycopg.Connection, src: str, dst: str) -> bool:
    return any(e["downstream"] == dst or e["upstream"] == dst for e in trace(conn, src, "down")) or src == dst


def full_graph(conn: psycopg.Connection) -> dict:
    nodes = conn.execute("""SELECT dataset_id, layer, kind, description, row_count, last_data_at FROM meta.dataset
                            WHERE dataset_id IN (SELECT upstream FROM meta.lineage_edge UNION SELECT downstream FROM meta.lineage_edge)
                            ORDER BY dataset_id""").fetchall()
    edges = conn.execute("SELECT upstream, downstream, source, task_id FROM meta.lineage_edge ORDER BY 1,2").fetchall()
    return {"nodes": [dict(zip(("id", "layer", "kind", "description", "row_count", "last_data_at"), n)) for n in nodes],
            "edges": [dict(zip(("upstream", "downstream", "source", "task_id"), e)) for e in edges]}
