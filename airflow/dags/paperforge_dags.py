"""PaperForge DAGs. Airflow only schedules: every task is one `paperforge` CLI call (installed in its own venv),
which records its own pipeline_run / task_run / lineage in PostgreSQL. No cleaning logic lives here.

    crossref_bootstrap     manual    initial load (offline sample)            ingest -> transform -> publish
    crossref_incremental   @daily    index-date window sync                   sync -> publish
    paper_daily_pipeline   @daily    validate, model, quality gate, publish   reconcile -> dbt -> quality -> lineage/catalog/report
    paper_replay           manual    replay a raw batch / quarantine          replay events -> replay quarantine -> reconcile
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PF = "/opt/pf-venv/bin/python -m paperforge.cli"
CTX = {  # lets the CLI group its task_run rows under the Airflow run
    "PF_DAG_ID": "{{ dag.dag_id }}", "PF_RUN_ID": "{{ run_id }}", "PF_TASK_ID": "{{ task.task_id }}",
    "PF_TRY_NUMBER": "{{ ti.try_number }}", "PF_TRIGGER": "{{ dag_run.run_type }}",
}
DEFAULT_ARGS = {"owner": "paperforge", "retries": 2, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True,
                "max_retry_delay": timedelta(minutes=10)}


def sh(task_id: str, cmd: str, timeout_min: int = 60, **kw) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=f"{PF} {cmd}", cwd="/opt/paperforge", env=CTX, append_env=True,
                        execution_timeout=timedelta(minutes=timeout_min), **kw)


def finalize() -> BashOperator:
    """Runs even after failures; marks meta.pipeline_run and fails the DAG run if any task failed."""
    return BashOperator(task_id="finalize_run", bash_command=f"{PF} pipeline finish --fail-if-failed", cwd="/opt/paperforge",
                        env=CTX, append_env=True, trigger_rule="all_done", retries=0, execution_timeout=timedelta(minutes=5))


COMMON = dict(default_args=DEFAULT_ARGS, start_date=datetime(2026, 11, 1), catchup=False, max_active_runs=1,
              dagrun_timeout=timedelta(hours=6), tags=["paperforge"])

with DAG("crossref_bootstrap", schedule=None, params={"limit": Param(1000, type="integer", minimum=1, description="records to ingest")},
         doc_md=__doc__, **COMMON) as bootstrap:
    migrate = sh("migrate", "migrate", 5)
    topics = sh("init_topics", "events init-topics", 5)
    ingest = sh("ingest", "ingest --limit {{ params.limit }}", 240)
    transform = sh("transform", "transform", 120)
    publish = sh("publish_outbox", "events publish", 60)
    migrate >> topics >> ingest >> transform >> publish >> finalize()

with DAG("crossref_incremental", schedule="@daily", doc_md=__doc__,
         params={"since": Param("", type="string", description="YYYY-MM-DD; empty = the run's data interval day"),
                 "limit": Param(0, type="integer", minimum=0, description="0 = unlimited")}, **COMMON) as incremental:
    sync = sh("sync", "sync --since {{ params.since or ds }} --until {{ params.since or ds }}"
                      "{% if params.limit %} --limit {{ params.limit }}{% endif %}", 240)
    publish = sh("publish_outbox", "events publish", 60)
    sync >> publish >> finalize()

with DAG("paper_daily_pipeline", schedule="@daily", doc_md=__doc__, **COMMON) as daily:
    reap = sh("reap_stale_runs", "pipeline reap --older-than-minutes 120", 5)
    validate = sh("validate_raw_vs_dwd", "reconcile-dwd", 30)
    dbt = sh("dbt_build", "dbt-build", 60)
    load = sh("load_dbt_results", "quality load-dbt", 10)
    gate = sh("quality_gate", "quality run", 30)
    lin = sh("sync_lineage", "lineage sync-dbt", 10)
    cat = sh("refresh_catalog", "catalog refresh", 15)
    report = sh("publish_quality_report", "quality report", 10)
    reap >> validate >> dbt >> load >> gate >> lin >> cat >> report >> finalize()

with DAG("paper_replay", schedule=None, doc_md=__doc__,
         params={"batch_id": Param("", type="string", description="raw ingestion batch id; empty = all raw files"),
                 "replay_quarantine": Param(True, type="boolean")}, **COMMON) as replay:
    ev = sh("replay_events", "events replay{% if params.batch_id %} --batch {{ params.batch_id }}{% endif %}", 240)
    quarantine = sh("replay_quarantine", "{% if params.replay_quarantine %}quality replay-quarantine{% else %}--help >/dev/null{% endif %}", 60)
    check = sh("reconcile_after_replay", "reconcile-dwd", 30)
    ev >> quarantine >> check >> finalize()
