import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { fmt, useApi, when, Json as J } from "../api";
import { Badge, Card, Load } from "../components/ui";
import { useT } from "../i18n";

const ORDER = ["source", "raw", "kafka", "ods", "dwd", "stg", "int", "dim", "dws", "ads", "meta"];

export default function Catalog() {
  const t = useT();
  const [sp, setSp] = useSearchParams();
  const list = useApi<J[]>("/datasets");
  const [filter, setFilter] = useState("");
  const sel = sp.get("dataset");
  useEffect(() => { if (!sel && list.data?.length) setSp({ dataset: list.data.find((d) => d.dataset_id === "dwd.paper")?.dataset_id ?? list.data[0].dataset_id }, { replace: true }); }, [list.data, sel, setSp]);
  return (
    <>
      <h1>{t("Data Catalog")}</h1>
      <p className="sub">{t("Every dataset of the warehouse by layer: description, grain, keys, update strategy, columns, size and freshness (")}<code>meta.dataset</code>).</p>
      <Load q={list}>{(ds) => {
        const layers = ORDER.filter((l) => ds.some((d) => d.layer === l));
        return (
          <div style={{ display: "grid", gridTemplateColumns: "330px 1fr", gap: 14, alignItems: "start" }}>
            <Card title={t("Datasets")} pad={false}>
              <div className="pad"><input style={{ width: "100%" }} placeholder={t("filter…")} value={filter} onChange={(e) => setFilter(e.target.value)} /></div>
              <div className="scroll" style={{ maxHeight: "74vh" }}>{layers.map((l) => <div key={l}><div className="pad muted" style={{ paddingBottom: 2, fontWeight: 600 }}>{l.toUpperCase()}</div>
                {ds.filter((d) => d.layer === l && d.dataset_id.includes(filter)).map((d) => <div key={d.dataset_id} className={`pad click ${sel === d.dataset_id ? "sel" : ""}`} style={{ padding: "4px 16px", cursor: "pointer", background: sel === d.dataset_id ? "#eaf0fe" : undefined }} onClick={() => setSp({ dataset: d.dataset_id })}>
                  <span className="mono">{d.dataset_id}</span><span className="muted" style={{ float: "right" }}>{d.row_count != null ? fmt(d.row_count) : t(d.kind)}</span></div>)}</div>)}</div>
            </Card>
            {sel ? <Detail id={sel} /> : null}
          </div>);
      }}</Load>
    </>
  );
}

function Detail({ id }: { id: string }) {
  const t = useT();
  const q = useApi<J>(`/datasets/${encodeURIComponent(id)}`);
  return <Load q={q}>{(d) => (
    <Card title={<><span className="mono">{d.dataset_id}</span> <Badge>{d.layer}</Badge> <Badge>{t(d.kind)}{d.materialization ? ` · ${d.materialization}` : ""}</Badge></>}>
      <p>{d.description ?? <span className="muted">{t("No description.")}</span>}</p>
      <dl className="dl">
        <dt>{t("Grain")}</dt><dd>{d.grain ?? "—"}</dd><dt>{t("Primary key")}</dt><dd className="mono">{d.primary_key ?? "—"}</dd><dt>{t("Unique constraints")}</dt><dd className="mono">{d.unique_constraints ?? "—"}</dd>
        <dt>{t("Update strategy")}</dt><dd>{d.update_strategy ?? "—"}</dd><dt>{t("Rows")}</dt><dd>{d.row_count != null ? fmt(d.row_count) : "—"}</dd><dt>{t("Newest data")}</dt><dd>{when(d.last_data_at)}</dd><dt>{t("Catalog refreshed")}</dt><dd>{when(d.refreshed_at)}</dd>
        <dt>{t("Upstream")}</dt><dd>{d.upstream.length ? d.upstream.map((u: string) => <div key={u}><Link className="mono" to={`/catalog?dataset=${encodeURIComponent(u)}`}>{u}</Link></div>) : "—"}</dd>
        <dt>{t("Downstream")}</dt><dd>{d.downstream.length ? d.downstream.map((u: string) => <div key={u}><Link className="mono" to={`/catalog?dataset=${encodeURIComponent(u)}`}>{u}</Link></div>) : "—"}</dd></dl>
      <h3>{t("Columns ({n})", { n: d.columns.length })}</h3>
      {d.columns.length ? <div className="scroll"><table><thead><tr><th>{t("Column")}</th><th>{t("Type")}</th><th>{t("Null")}</th><th>{t("Description")}</th></tr></thead>
        <tbody>{d.columns.map((c: J) => <tr key={c.name}><td className="mono">{c.name}</td><td className="mono">{c.type ?? "—"}</td><td>{c.nullable === undefined ? "—" : c.nullable ? t("yes") : t("no")}</td><td>{c.description ?? ""}</td></tr>)}</tbody></table></div>
        : <div className="muted">{t("Not a table (no columns), or not built yet.")}</div>}
    </Card>)}</Load>;
}
