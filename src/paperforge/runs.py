"""Pipeline/task run bookkeeping (meta.pipeline_run, meta.task_run) + runtime lineage registration.
Airflow passes PF_DAG_ID / PF_RUN_ID / PF_TRIGGER / PF_TRY_NUMBER so CLI tasks are grouped under the orchestrated run;
a CLI call without them is its own single-task run with dag_id='cli'."""
import os
import time
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field

from psycopg.types.json import Jsonb

from paperforge.db import connect
from paperforge.lineage.base import Edge
from paperforge.lineage.datasets import RUNTIME_EDGES, register_static
from paperforge.lineage.pg_emitter import PgLineageEmitter


@dataclass
class TaskHandle:
    task_run_id: str
    run_id: str
    rows_in: int | None = None
    rows_out: int | None = None
    rows_failed: int | None = None
    data_version: str | None = None
    details: dict = field(default_factory=dict)

    def set(self, rows_in=None, rows_out=None, rows_failed=None, data_version=None, **details):
        for k, v in (("rows_in", rows_in), ("rows_out", rows_out), ("rows_failed", rows_failed), ("data_version", data_version)):
            if v is not None:
                setattr(self, k, v)
        self.details.update(details)


def _pipeline_run(conn, params: dict) -> tuple[str, bool]:
    dag, ext = os.environ.get("PF_DAG_ID"), os.environ.get("PF_RUN_ID")
    if dag and ext:
        row = conn.execute(
            """INSERT INTO meta.pipeline_run(run_id, dag_id, external_run_id, trigger, status, params)
               VALUES (%s,%s,%s,%s,'running',%s)
               ON CONFLICT (dag_id, external_run_id) DO UPDATE SET dag_id = EXCLUDED.dag_id RETURNING run_id""",
            (uuid.uuid4(), dag, ext, os.environ.get("PF_TRIGGER", "scheduled"), Jsonb(params))).fetchone()
        return str(row[0]), False
    rid = uuid.uuid4()
    conn.execute("INSERT INTO meta.pipeline_run(run_id, dag_id, trigger, status, params) VALUES (%s,'cli','manual','running',%s)",
                 (rid, Jsonb(params)))
    return str(rid), True


@contextmanager
def tracked(task_id: str, params: dict | None = None, lineage_task: str | None = None):
    """Record one task attempt. `lineage_task` selects RUNTIME_EDGES entries that are registered on success."""
    conn = connect()
    register_static(conn)
    run_id, standalone = _pipeline_run(conn, params or {})
    h = TaskHandle(str(uuid.uuid4()), run_id)
    t0 = time.monotonic()
    conn.execute("INSERT INTO meta.task_run(task_run_id, run_id, task_id, attempt, status, details) VALUES (%s,%s,%s,%s,'running',%s)",
                 (h.task_run_id, run_id, os.environ.get("PF_TASK_ID", task_id), int(os.environ.get("PF_TRY_NUMBER", "1")),
                  Jsonb({**(params or {}), "command": task_id})))        # stable stage key even when Airflow names the task differently
    status, err = "success", None
    try:
        yield h
    except BaseException as e:  # noqa: BLE001 - recorded, then re-raised
        status, err = "failed", "".join(traceback.format_exception_only(type(e), e)).strip()[:2000]
        code = getattr(e, "exit_code", getattr(e, "code", "n/a"))     # typer.Exit / SystemExit
        if code != "n/a" and code in (0, None):
            status, err = "success", None
        raise
    finally:
        conn.execute("""UPDATE meta.task_run SET status=%s, rows_in=%s, rows_out=%s, rows_failed=%s, data_version=%s, ended_at=now(),
                        duration_ms=%s, error=%s, details=details || %s WHERE task_run_id=%s""",
                     (status, h.rows_in, h.rows_out, h.rows_failed, h.data_version, int((time.monotonic() - t0) * 1000), err,
                      Jsonb(h.details), h.task_run_id))
        if status == "success" and lineage_task:
            PgLineageEmitter(conn).emit_edges([Edge(u, d, lineage_task, "runtime") for u, d in RUNTIME_EDGES[lineage_task]], h.task_run_id)
        if standalone:
            conn.execute("UPDATE meta.pipeline_run SET status=%s, ended_at=now() WHERE run_id=%s", (status, run_id))
        conn.close()


def finish_run(status: str | None = None) -> str:
    """Close the orchestrated run: failed if any task's latest attempt failed, else success."""
    dag, ext = os.environ["PF_DAG_ID"], os.environ["PF_RUN_ID"]
    with connect() as c:
        row = c.execute("SELECT run_id FROM meta.pipeline_run WHERE dag_id=%s AND external_run_id=%s", (dag, ext)).fetchone()
        if not row:
            raise RuntimeError("no such pipeline run")
        if status is None:
            bad = c.execute("""SELECT count(*) FROM (SELECT DISTINCT ON (task_id) status FROM meta.task_run WHERE run_id=%s
                               ORDER BY task_id, attempt DESC, started_at DESC) t WHERE status <> 'success'""", (row[0],)).fetchone()[0]
            status = "failed" if bad else "success"
        c.execute("UPDATE meta.pipeline_run SET status=%s, ended_at=now() WHERE run_id=%s", (status, row[0]))
    return status


LONG_RUNNING = ("stream_worker",)       # services legitimately stay 'running' for days


def reap_stale(older_than_minutes: int = 120) -> dict:
    """A task killed by the OS (OOM, SIGKILL) never reaches its bookkeeping. Mark such attempts failed so dashboards and the
    run status are truthful. Long-lived services are exempt."""
    with connect() as c:
        t = c.execute("""UPDATE meta.task_run SET status='failed', ended_at=now(),
                         error='abandoned: no completion recorded (process killed or crashed)'
                         WHERE status='running' AND started_at < now() - make_interval(mins => %s) AND task_id <> ALL(%s)""",
                      (older_than_minutes, list(LONG_RUNNING)))
        r = c.execute("""UPDATE meta.pipeline_run p SET status='failed', ended_at=now()
                         WHERE p.status='running' AND p.started_at < now() - make_interval(mins => %s)
                           AND NOT EXISTS (SELECT 1 FROM meta.task_run t WHERE t.run_id=p.run_id AND t.status='running')""",
                      (older_than_minutes,))
    return {"tasks_marked_failed": t.rowcount, "runs_marked_failed": r.rowcount}
