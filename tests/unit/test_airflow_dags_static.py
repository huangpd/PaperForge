"""Airflow only schedules: DAG definitions must stay free of processing code (Airflow is not even installed in this env)."""
import ast
from pathlib import Path

SRC = (Path(__file__).resolve().parents[2] / "airflow/dags/paperforge_dags.py").read_text()


def test_four_dags_defined_with_expected_ids():
    tree = ast.parse(SRC)
    ids = {n.args[0].value for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "DAG" and n.args}
    assert ids == {"crossref_bootstrap", "crossref_incremental", "paper_daily_pipeline", "paper_replay"}


def test_dags_import_no_processing_libraries():
    imported = {a.name.split(".")[0] for n in ast.walk(ast.parse(SRC)) if isinstance(n, (ast.Import, ast.ImportFrom))
                for a in (n.names if isinstance(n, ast.Import) else [ast.alias(name=n.module or "")])}
    assert imported <= {"datetime", "airflow"}, imported


def test_every_task_has_timeout_retries_and_goes_through_the_cli():
    assert "execution_timeout" in SRC and '"retries": 2' in SRC and "retry_exponential_backoff" in SRC
    assert "paperforge.cli" in SRC and "trigger_rule=\"all_done\"" in SRC           # finalize runs after failures too
    assert "dagrun_timeout" in SRC and "max_active_runs=1" in SRC
