import psycopg

from paperforge.lineage.base import Edge, LineageEmitter
from paperforge.lineage.datasets import ensure_dataset


class PgLineageEmitter(LineageEmitter):
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def emit_edges(self, edges: list[Edge], task_run_id: str | None = None) -> None:
        with self.conn.transaction():
            for e in edges:
                ensure_dataset(self.conn, e.upstream)
                ensure_dataset(self.conn, e.downstream)
                self.conn.execute(
                    """INSERT INTO meta.lineage_edge(upstream, downstream, source, task_id, last_task_run_id)
                       VALUES (%s,%s,%s,%s,%s)
                       ON CONFLICT (upstream, downstream, source) DO UPDATE
                       SET task_id=EXCLUDED.task_id, last_seen_at=now(),
                           last_task_run_id=coalesce(EXCLUDED.last_task_run_id, meta.lineage_edge.last_task_run_id)""",
                    (e.upstream, e.downstream, e.source, e.task_id, task_run_id))

    def replace_edges(self, source: str, edges: list[Edge]) -> int:
        self.emit_edges(edges)
        keep = [(e.upstream, e.downstream) for e in edges]
        with self.conn.transaction():
            cur = self.conn.execute(
                "DELETE FROM meta.lineage_edge WHERE source=%s AND (upstream, downstream) NOT IN (SELECT * FROM unnest(%s::text[], %s::text[]))",
                (source, [k[0] for k in keep], [k[1] for k in keep]))
            return cur.rowcount
