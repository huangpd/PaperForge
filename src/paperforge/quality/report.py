from datetime import date
from pathlib import Path

import psycopg


def build_report(conn: psycopg.Connection, day: date | None = None) -> str:
    day = day or date.today()
    run = conn.execute("SELECT run_id, max(run_at) FROM meta.quality_result GROUP BY run_id ORDER BY max(run_at) DESC LIMIT 1").fetchone()
    if not run:
        return f"# Data quality report {day}\n\nNo quality run recorded yet (`quality run`).\n"
    run_id, run_at = run
    rows = conn.execute("""SELECT q.dimension, r.severity, r.rule_id, r.dataset, r.total_rows, r.failed_rows, r.passed, q.description, r.details
                           FROM meta.quality_result r JOIN meta.quality_rule q ON q.rule_id=r.rule_id AND q.version=r.rule_version
                           WHERE r.run_id=%s ORDER BY q.dimension, r.severity, r.rule_id""", (run_id,)).fetchall()
    gate = [r for r in rows if r[1] in ("error", "warning")]
    err_fail = [r for r in gate if r[1] == "error" and r[6] is False]
    warn_fail = [r for r in gate if r[1] == "warning" and r[6] is False]
    L = [f"# Data quality report {day}", "", f"Run `{run_id}` at {run_at:%Y-%m-%d %H:%M:%S %Z}", "",
         f"- Rules executed: {len(rows)} ({sum(r[1]=='error' for r in rows)} error, {sum(r[1]=='warning' for r in rows)} warning, "
         f"{sum(r[1]=='metric' for r in rows)} metric)",
         f"- Error rules failing: **{len(err_fail)}**; warning rules with findings: **{len(warn_fail)}**", ""]
    L += ["## Coverage metrics (completeness)", "", "| metric | rows | missing | coverage |", "|---|---:|---:|---:|"]
    for d, s, rid, ds, tot, fail, passed, desc, det in rows:
        if s == "metric" and d == "completeness":
            L.append(f"| {desc} | {tot:,} | {fail:,} | {100 * (tot - fail) / tot:.2f}% |" if tot else f"| {desc} | 0 | 0 | n/a |")
    t = next((r for r in rows if r[2] == "t_hours_since_last_ingest"), None)
    if t:
        L += ["", f"Timeliness: newest ingestion was {t[8].get('hours')} hours before this run (last at {t[8].get('last_ingested_at')})."]
    L += ["", "## Rule results (error / warning)", "", "| dimension | severity | rule | dataset | rows | failed | status |", "|---|---|---|---|---:|---:|---|"]
    for d, s, rid, ds, tot, fail, passed, desc, det in gate:
        L.append(f"| {d} | {s} | `{rid}` | {ds} | {tot:,} | {fail:,} | {'PASS' if passed else ('FAIL' if s == 'error' else 'WARN')} |")
    q = conn.execute("SELECT rule_id, status, count(*) FROM meta.quarantine_record GROUP BY 1,2 ORDER BY 1,2").fetchall()
    L += ["", "## Quarantine", ""]
    L += ["| rule | status | records |", "|---|---|---:|"] + [f"| `{a}` | {b} | {c} |" for a, b, c in q] if q else ["No quarantined records."]
    try:
        daily = conn.execute("SELECT stat_date, papers, title_rate, authors_rate, abstract_rate, quarantined_open FROM dws.data_quality_daily "
                             "ORDER BY stat_date DESC LIMIT 7").fetchall()
        L += ["", "## Daily load quality (dws.data_quality_daily, last 7 days)", "",
              "| date | papers | title | authors | abstract | open quarantine |", "|---|---:|---:|---:|---:|---:|"]
        L += [f"| {a} | {b:,} | {c} | {d} | {e} | {f} |" for a, b, c, d, e, f in daily]
    except psycopg.errors.UndefinedTable:
        L += ["", "_dws.dws_data_quality_daily not built yet (`make dbt`)._"]
    return "\n".join(L) + "\n"


def write_report(conn, out_dir: str, day: date | None = None) -> Path:
    day = day or date.today()
    p = Path(out_dir) / "reports" / f"quality-{day}.md"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(build_report(conn, day))
    return p
