import { useMemo } from "react";
import { fmt, pct, useApi, when, Json as J } from "../api";
import Chart from "../components/Chart";
import { Card, Load, Stat } from "../components/ui";
import { useT } from "../i18n";

export default function Overview() {
  const t = useT();
  const q = useApi<J>("/statistics/overview");
  return (
    <>
      <h1>{t("Overview")}</h1>
      <p className="sub">{t("Live numbers from the warehouse (ads / dws layers) — nothing on this page is static.")}</p>
      <Load q={q}>{(o) => <Body o={o} />}</Load>
    </>
  );
}

function Body({ o }: { o: J }) {
  const t = useT();
  const years = useMemo(() => ({
    grid: { left: 48, right: 12, top: 16, bottom: 28 }, tooltip: { trigger: "axis" },
    xAxis: { type: "category", data: o.by_year.map((r: J) => r.issued_year) },
    yAxis: { type: "value" }, series: [{ type: "bar", data: o.by_year.map((r: J) => r.paper_count), barMaxWidth: 14 }],
  }), [o]);
  const daily = useMemo(() => ({
    grid: { left: 48, right: 12, top: 30, bottom: 28 }, tooltip: { trigger: "axis" }, legend: { top: 0 },
    xAxis: { type: "category", data: o.daily_changes.map((r: J) => r.change_date) }, yAxis: { type: "value" },
    series: [{ name: t("new papers"), type: "bar", stack: "a", data: o.daily_changes.map((r: J) => r.new_papers) },
             { name: t("new versions"), type: "bar", stack: "a", data: o.daily_changes.map((r: J) => r.new_versions) }],
  }), [o, t]);
  const bars = (rows: J[], name: string, key: string) => ({
    grid: { left: 8, right: 24, top: 8, bottom: 8, containLabel: true }, tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    yAxis: { type: "category", inverse: true, data: rows.map((r) => (r[name] ?? r[key]).slice(0, 38)), axisLabel: { width: 230, overflow: "truncate" } },
    xAxis: { type: "value" }, series: [{ type: "bar", data: rows.map((r) => r.paper_count), barMaxWidth: 14 }],
  });
  const rate = o.quality_pass_rate;
  return (
    <>
      <div className="grid g4">
        <Stat label={t("Papers (valid, in DWD)")} value={fmt(o.papers)} sub={t("{n} versions · {m} journals", { n: fmt(o.versions), m: o.journals ? fmt(o.journals) : "—" })} />
        <Stat label={t("Raw records (ODS)")} value={fmt(o.raw_records)} sub={t("{n} lines in {m} immutable files", { n: fmt(o.raw_file_rows), m: fmt(o.raw_files) })} />
        <Stat label={t("Quality pass rate")} value={pct(rate)} tone={rate === 1 ? "ok" : rate > 0.9 ? "warn" : "bad"} sub={t("{n} error/warning rules checked", { n: fmt(o.quality_rules_checked) })} />
        <Stat label={t("Open quarantine")} value={fmt(o.open_quarantine)} tone={o.open_quarantine ? "warn" : "ok"} sub={t("last ingestion {when}", { when: when(o.last_ingested_at) })} />
      </div>
      <div className="grid g2" style={{ marginTop: 14 }}>
        <Card title={t("Papers by publication year")}><Chart option={years} /></Card>
        <Card title={t("Daily increment (version writes per UTC day)")}><Chart option={daily} /></Card>
        <Card title={t("Top journals")}><Chart option={bars(o.top_journals, "journal_title", "journal_key")} height={300} /></Card>
        <Card title={t("Top publishers")}><Chart option={bars(o.top_publishers, "publisher_name", "publisher_key")} height={300} /></Card>
      </div>
    </>
  );
}
