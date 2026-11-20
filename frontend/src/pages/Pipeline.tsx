import { Fragment, useState } from "react";
import { dur, fmt, useApi, when, Json as J } from "../api";
import { Badge, Card, Load, statusBadge } from "../components/ui";
import { useT } from "../i18n";

// Stages grouped by where they sit in the data flow. Left to right = the order data moves; the last group is rarely used.
type Phase = { name: string; tasks: string[]; quiet?: boolean };
const PHASES: Phase[] = [
  { name: "Ingest", tasks: ["ingest", "ingest_arxiv", "sync", "fetch_doi"] },
  { name: "Stream", tasks: ["publish", "stream_worker"] },
  { name: "Standardize", tasks: ["transform", "reconcile_dwd"] },
  { name: "Model", tasks: ["dbt_build", "load_dbt_results"] },
  { name: "Quality & governance", tasks: ["quality_run", "quality_report", "lineage_sync_dbt", "catalog_refresh"] },
  { name: "Repair & replay", tasks: ["replay_events", "replay_dlq", "replay_quarantine", "reconcile_raw"], quiet: true },
];
const LABEL: Record<string, string> = { ingest_arxiv: "Ingest (arXiv)", ingest: "Ingest (offline sample)", sync: "Sync (Crossref API)", fetch_doi: "Fetch by DOI", publish: "Publish to Kafka", stream_worker: "Stream worker", transform: "Batch transform", dbt_build: "dbt build",
  load_dbt_results: "Load dbt results", quality_run: "Quality run", lineage_sync_dbt: "Lineage sync", catalog_refresh: "Catalog refresh", replay_events: "Replay events", replay_dlq: "Replay DLQ", replay_quarantine: "Replay quarantine", reconcile_dwd: "Reconcile ODS ↔ DWD", reconcile_raw: "Reconcile raw files", quality_report: "Quality report" };

const short = (v: string | null | undefined) => (v ? when(v).slice(5, 16) : "—");   // MM-DD HH:MM, viewer time zone
const dot = (s: string) => (s === "failed" ? "bad" : s === "running" ? "run" : s === "success" ? "ok" : "none");

export default function Pipeline() {
  const t = useT();
  const q = useApi<J>("/pipelines/runs", { limit: 30 });
  return (
    <>
      <h1>{t("Pipeline")}</h1>
      <p className="sub">{t("Latest state of every pipeline stage, taken from")} <code>meta.task_run</code> / <code>meta.pipeline_run</code>.</p>
      <Load q={q}>{(d) => <Body d={d} />}</Load>
    </>
  );
}

function Body({ d }: { d: J }) {
  const t = useT();
  const [open, setOpen] = useState<string | null>(null);
  const [allRuns, setAllRuns] = useState(false), [onlyBad, setOnlyBad] = useState(false), [allBatches, setAllBatches] = useState(false);
  const byTask: Record<string, J> = Object.fromEntries(d.stages.map((s: J) => [s.task_id, s]));
  const totals: Record<string, J> = Object.fromEntries(d.task_totals.map((t: J) => [t.task_id, t]));
  const known = new Set(PHASES.flatMap((p) => p.tasks));
  const phases: Phase[] = [...PHASES, ...(d.stages.some((s: J) => !known.has(s.task_id)) ? [{ name: "Other", tasks: d.stages.filter((s: J) => !known.has(s.task_id)).map((s: J) => s.task_id) }] : [])]
    .map((p) => ({ ...p, tasks: p.tasks.filter((k: string) => byTask[k]) })).filter((p) => p.tasks.length);
  const bad: J[] = d.stages.filter((s: J) => s.status === "failed");
  const isBad = (r: J) => r.status === "failed" || r.tasks.some((x: J) => x.status === "failed" || (x.rows_failed ?? 0) > 0);
  const runs: J[] = (onlyBad ? d.runs.filter(isBad) : d.runs).slice(0, allRuns || onlyBad ? 999 : 8);
  const batches: J[] = allBatches ? d.batches : d.batches.slice(0, 5);
  return (
    <>
      <Card title={t("Stages — latest run of each task")} right={bad.length
        ? <Badge kind="bad">{t("{n} need attention", { n: bad.length })}</Badge> : <Badge kind="ok">{t("All stages healthy")}</Badge>}>
        {bad.length > 0 && <div className="err" style={{ marginTop: 0 }}>{bad.map((s) => <div key={s.task_id}><b>{t(LABEL[s.task_id] ?? s.task_id)}</b> — {t("last run failed")} {short(s.started_at)}</div>)}</div>}
        <div className="pipe">
          {phases.map((p, i) => (
            <Fragment key={p.name}>
              {p.quiet && !phases[i - 1]?.quiet ? <div className="pipe-break" /> : i > 0 && <div className="pipe-arrow" aria-hidden>→</div>}
              <div className={`phase ${p.quiet ? "quiet" : ""}`}>
                <div className="phase-h">{t(p.name)}</div>
                {p.tasks.map((k: string) => { const s = byTask[k]; return (
                  <div key={k} className={`st ${dot(s.status)}`} tabIndex={0}>
                    <i className={`dot ${dot(s.status)}`} /><span className="st-n">{t(LABEL[k] ?? k)}</span>{s.status !== "success" && <span className="st-t">{short(s.started_at)}</span>}
                    <div className="pop">
                      <div className="pop-h"><b>{t(LABEL[k] ?? k)}</b>{statusBadge(s.status)}</div>
                      <div className="stage-m">
                        <div><span>{t("In")}</span><b>{fmt(s.rows_in)}</b></div>
                        <div><span>{t("Out")}</span><b>{fmt(s.rows_out)}</b></div>
                        <div className={s.rows_failed ? "bad" : ""}><span>{t("Failed")}</span><b>{fmt(s.rows_failed)}</b></div>
                      </div>
                      <div className="stage-f"><span>{dur(s.duration_ms)}</span><span>{when(s.started_at)}</span></div>
                      <div className="stage-f"><span>{t("{n} runs, {m} failed (all time)", { n: totals[k]?.runs ?? 0, m: totals[k]?.failed ?? 0 })}</span></div>
                    </div>
                  </div>); })}
              </div>
            </Fragment>
          ))}
        </div>
        <div className="muted" style={{ fontSize: 12, marginTop: 10 }}>{t("Hover a stage for its numbers. Healthy stages stay quiet; failures turn red.")}</div>
      </Card>
      <div className="grid" style={{ marginTop: 14, gridTemplateColumns: "minmax(0,2fr) minmax(0,1fr)" }}>
        <Card title={t("Ingestion batches")} pad={false} right={d.batches.length > 5 && <button onClick={() => setAllBatches(!allBatches)}>{allBatches ? t("Show less") : t("Show all ({n})", { n: d.batches.length })}</button>}>
          <div className="scroll"><table><thead><tr><th>{t("Batch")}</th><th>{t("Mode")}</th><th>{t("Status")}</th><th className="num">{t("Seen")}</th><th className="num">{t("New")}</th><th className="num">{t("Upd")}</th><th className="num">{t("Same")}</th><th className="num">{t("Skip")}</th></tr></thead>
            <tbody>{batches.map((b: J) => <tr key={b.batch_id}><td className="mono nw" title={b.batch_id}>{b.batch_id.slice(4, 13)}…{b.batch_id.slice(-4)}</td><td>{b.mode}</td><td>{statusBadge(b.status)}</td><td className="num">{fmt(b.rows_seen)}</td><td className="num">{fmt(b.rows_inserted)}</td><td className="num">{fmt(b.rows_updated)}</td><td className="num">{fmt(b.rows_unchanged)}</td><td className="num">{fmt(b.rows_skipped)}</td></tr>)}</tbody></table></div>
        </Card>
        <Card title={t("Crossref API requests")} pad={false}>
          {d.http_requests.length ? <table><thead><tr><th>{t("Status")}</th><th className="num">{t("Requests")}</th><th className="num">{t("Avg latency")}</th></tr></thead>
            <tbody>{d.http_requests.map((h: J) => <tr key={h.status}><td>{statusBadge(h.status.startsWith("2") ? "ok" : "failed")} {h.status}</td><td className="num">{fmt(h.n)}</td><td className="num">{dur(h.avg_ms)}</td></tr>)}</tbody></table>
            : <div className="muted pad">{t("No API requests recorded yet.")}</div>}
        </Card>
      </div>
      <Card title={t("Recent pipeline runs")} pad={false} right={<>
        <label style={{ fontSize: 12, marginRight: 10 }}><input type="checkbox" checked={onlyBad} onChange={(e) => setOnlyBad(e.target.checked)} /> {t("Only problems")}</label>
        {!onlyBad && d.runs.length > 8 && <button onClick={() => setAllRuns(!allRuns)}>{allRuns ? t("Show less") : t("Show all ({n})", { n: d.runs.length })}</button>}</>}>
        <div className="scroll"><table><thead><tr><th>{t("Started")}</th><th>{t("DAG / source")}</th><th>{t("Trigger")}</th><th>{t("Status")}</th><th className="num">{t("Tasks")}</th><th className="num">{t("Failed rows")}</th><th className="num">{t("Duration")}</th></tr></thead>
          <tbody>{runs.map((r: J) => {
            const failed = r.tasks.reduce((n: number, t: J) => n + (t.rows_failed ?? 0), 0);
            const ms = r.ended_at ? new Date(r.ended_at).getTime() - new Date(r.started_at).getTime() : null;
            return [
              <tr key={r.run_id} className={`click ${r.status === "failed" ? "rowbad" : ""}`} onClick={() => setOpen(open === r.run_id ? null : r.run_id)}>
                <td>{when(r.started_at)}</td><td>{r.dag_id}{r.dag_id === "cli" && r.tasks[0] ? ` · ${r.tasks[0].task_id}` : ""}</td><td>{r.trigger}</td><td>{statusBadge(r.status)}</td>
                <td className="num">{r.tasks.length}</td><td className="num">{fmt(failed)}</td><td className="num">{dur(ms)}</td></tr>,
              open === r.run_id && <tr key={r.run_id + "t"}><td colSpan={7} style={{ background: "#fafbfc" }}>
                <table><thead><tr><th>{t("Task")}</th><th>{t("Try")}</th><th>{t("Status")}</th><th className="num">{t("In")}</th><th className="num">{t("Out")}</th><th className="num">{t("Failed")}</th><th>{t("Data version")}</th><th className="num">{t("Duration")}</th><th>{t("Error")}</th></tr></thead>
                  <tbody>{r.tasks.map((t: J, i: number) => <tr key={i}><td>{t.task_id}</td><td>{t.attempt}</td><td>{statusBadge(t.status)}</td><td className="num">{fmt(t.rows_in)}</td><td className="num">{fmt(t.rows_out)}</td><td className="num">{fmt(t.rows_failed)}</td><td className="mono">{t.data_version ?? ""}</td><td className="num">{dur(t.duration_ms)}</td><td className="mono">{t.error ? <Badge kind="bad">{String(t.error).slice(0, 90)}</Badge> : ""}</td></tr>)}</tbody></table>
              </td></tr>,
            ];
          })}{!runs.length && <tr><td colSpan={7} className="muted">{t("No problem runs. Everything recent succeeded.")}</td></tr>}</tbody></table></div>
      </Card>
    </>
  );
}
