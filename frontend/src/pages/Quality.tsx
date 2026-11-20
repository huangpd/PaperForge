import { useMemo, useState } from "react";
import { fmt, pct, useApi, when, Json as J } from "../api";
import Chart from "../components/Chart";
import { Badge, Card, Json, Load, Stat, statusBadge } from "../components/ui";
import { useT } from "../i18n";

export default function Quality() {
  const t = useT();
  const q = useApi<J>("/quality/metrics", { quarantine_limit: 100 });
  const rec = useApi<J>("/quality/reconciliation");
  return (
    <>
      <h1>{t("Data Quality")}</h1>
      <p className="sub">{t("Rule results from Pandera, SQL, ingest-time validators and dbt tests (")}<code>meta.quality_result</code>{t("), plus quarantine and DuckDB reconciliation.")}</p>
      <Load q={q}>{(d) => <Body d={d} rec={rec.data} />}</Load>
    </>
  );
}

function Body({ d, rec }: { d: J; rec?: J }) {
  const t = useT();
  const [sev, setSev] = useState("all");
  const [dim, setDim] = useState("all");
  const [onlyFail, setOnlyFail] = useState(false);
  const [view, setView] = useState<J>();
  const gate = d.rules.filter((r: J) => r.severity !== "metric");
  const failing = gate.filter((r: J) => r.passed === false);
  const doi = d.rules.find((r: J) => r.rule_id === "v_doi_not_null");
  const cov = useMemo(() => [...(doi ? [{ label: t("DOI present"), v: 1 - doi.failed_rows / doi.total_rows }] : []),
    ...d.metrics.map((m: J) => ({ label: m.description.replace(/ completeness| coverage/, ""), v: m.coverage }))], [d, doi, t]);
  const covOpt = { grid: { left: 8, right: 40, top: 6, bottom: 6, containLabel: true }, tooltip: { formatter: (p: J) => `${p.name}: ${(p.value * 100).toFixed(2)}%` },
    xAxis: { type: "value", max: 1, axisLabel: { formatter: (v: number) => `${v * 100}%` } }, yAxis: { type: "category", inverse: true, data: cov.map((c: J) => c.label) },
    series: [{ type: "bar", data: cov.map((c: J) => c.v), barMaxWidth: 14, label: { show: true, position: "right", formatter: (p: J) => `${(p.value * 100).toFixed(1)}%` } }] };
  const dimOpt = { grid: { left: 8, right: 12, top: 28, bottom: 8, containLabel: true }, tooltip: { trigger: "axis", axisPointer: { type: "shadow" } }, legend: { top: 0 },
    xAxis: { type: "category", data: d.dimensions.map((x: J) => t(x.dimension.replace("_", " "))), axisLabel: { interval: 0, rotate: 18 } }, yAxis: { type: "value" },
    series: [{ name: t("passed"), type: "bar", stack: "a", data: d.dimensions.map((x: J) => x.passed) }, { name: t("failing"), type: "bar", stack: "a", data: d.dimensions.map((x: J) => x.rules - x.passed), itemStyle: { color: "#e5484d" } }] };
  const dims = [...new Set(d.rules.map((r: J) => r.dimension))] as string[];
  const rows = d.rules.filter((r: J) => (sev === "all" || r.severity === sev) && (dim === "all" || r.dimension === dim) && (!onlyFail || r.passed === false));
  return (
    <>
      <div className="grid g4">
        <Stat label={t("Error/warning rules")} value={fmt(gate.length)} sub={t("{n} passing", { n: fmt(gate.filter((r: J) => r.passed).length) })} />
        <Stat label={t("Failing rules")} value={fmt(failing.length)} tone={failing.some((r: J) => r.severity === "error") ? "bad" : failing.length ? "warn" : "ok"} sub={t("{a} error · {b} warning", { a: failing.filter((r: J) => r.severity === "error").length, b: failing.filter((r: J) => r.severity === "warning").length })} />
        <Stat label={t("Quarantined (open)")} value={fmt(d.quarantine_summary.filter((x: J) => x.status === "open").reduce((n: number, x: J) => n + x.n, 0))} tone="warn" sub={t("kept out of the standardized layer")} />
        <Stat label="Parquet ↔ PostgreSQL" value={rec?.parquet_available ? (Object.values(rec.checks).every(Boolean) ? t("match") : t("MISMATCH")) : "—"} tone={rec?.parquet_available ? (Object.values(rec.checks).every(Boolean) ? "ok" : "bad") : undefined}
          sub={rec?.parquet_available ? t("{a} parquet rows · {b} ODS · {c} DWD", { a: fmt(rec.parquet_rows), b: fmt(rec.ods_rows), c: fmt(rec.dwd_papers) }) : t("DuckDB reconciliation")} />
      </div>
      <div className="grid g2" style={{ marginTop: 14 }}>
        <Card title={t("Field coverage (completeness)")}><Chart option={covOpt} height={300} /></Card>
        <Card title={t("Rules by quality dimension")}><Chart option={dimOpt} height={300} /></Card>
      </div>
      <div style={{ height: 14 }} />
      <Card title={t("Quality rules")} right={<div className="filters">
        <select value={sev} onChange={(e) => setSev(e.target.value)}><option value="all">{t("all severities")}</option><option value="error">{t("error")}</option><option value="warning">{t("warning")}</option><option value="metric">{t("metric")}</option></select>
        <select value={dim} onChange={(e) => setDim(e.target.value)}><option value="all">{t("all dimensions")}</option>{dims.map((x) => <option key={x} value={x}>{t(x.replace("_", " "))}</option>)}</select>
        <label style={{ flexDirection: "row", alignItems: "center", gap: 6 }}><input type="checkbox" checked={onlyFail} onChange={(e) => setOnlyFail(e.target.checked)} />{t("failing only")}</label></div>} pad={false}>
        <div className="scroll"><table><thead><tr><th>{t("Rule")}</th><th>{t("Engine")}</th><th>{t("Dimension")}</th><th>{t("Severity")}</th><th>{t("Dataset")}</th><th className="num">{t("Rows")}</th><th className="num">{t("Failed")}</th><th>{t("Result")}</th></tr></thead>
          <tbody>{rows.map((r: J) => <tr key={r.rule_id}><td><span className="mono">{r.rule_id}</span><div className="muted">{r.description}</div></td><td>{r.engine}</td><td>{t(String(r.dimension).replace("_", " "))}</td><td><Badge kind={r.severity === "error" ? "bad" : r.severity === "warning" ? "warn" : ""}>{t(r.severity)}</Badge></td><td>{r.dataset}</td>
            <td className="num">{r.total_rows ? fmt(r.total_rows) : "—"}</td><td className="num">{fmt(r.failed_rows)}</td><td>{r.passed === null ? <span className="muted">{t("metric")}</span> : statusBadge(r.passed ? "pass" : r.severity === "error" ? "fail" : "warning")}</td></tr>)}</tbody></table></div>
      </Card>
      <div className="grid g2" style={{ marginTop: 14 }}>
        <Card title={t("Quarantine records ({n} latest)", { n: d.quarantine.length })} pad={false}>
          <div className="scroll"><table><thead><tr><th>{t("Id")}</th><th>{t("DOI")}</th><th>{t("Rule")}</th><th>{t("Reason")}</th><th>{t("Status")}</th><th>{t("When")}</th></tr></thead>
            <tbody>{d.quarantine.map((x: J) => <tr key={x.id} className="click" onClick={() => setView(x)}><td>{x.id}</td><td className="mono">{x.source_id}</td><td>{x.rule_id} v{x.rule_version}</td><td>{x.reason}</td><td>{statusBadge(x.status)}</td><td>{when(x.created_at)}</td></tr>)}
              {!d.quarantine.length && <tr><td colSpan={6} className="muted">{t("Nothing quarantined.")}</td></tr>}</tbody></table></div>
          {view && <div className="pad"><b>{t("Raw record #{id}", { id: view.id })}</b> <button onClick={() => setView(undefined)}>{t("close")}</button><Json value={view.payload} height={260} /></div>}
        </Card>
        <Card title={t("Daily load quality (UTC days)")} pad={false}>
          <div className="scroll"><table><thead><tr><th>{t("Date")}</th><th className="num">{t("Papers")}</th><th className="num">{t("Title")}</th><th className="num">{t("Authors")}</th><th className="num">{t("Abstract")}</th><th className="num">{t("Quarantined")}</th></tr></thead>
            <tbody>{[...d.daily].reverse().map((x: J) => <tr key={x.stat_date}><td>{x.stat_date}</td><td className="num">{fmt(x.papers)}</td><td className="num">{pct(x.title_rate)}</td><td className="num">{pct(x.authors_rate)}</td><td className="num">{pct(x.abstract_rate)}</td><td className="num">{fmt(x.quarantined_total)}</td></tr>)}</tbody></table></div>
        </Card>
      </div>
    </>
  );
}
