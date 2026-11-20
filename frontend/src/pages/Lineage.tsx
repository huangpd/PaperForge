import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { fmt, useApi, Json as J } from "../api";
import Chart from "../components/Chart";
import { Badge, Card, Load } from "../components/ui";
import { useT } from "../i18n";

const LAYERS = ["source", "raw", "ods", "kafka", "dwd", "stg", "int", "dim", "dws", "ads", "meta"];
const COLORS: Record<string, string> = { source: "#6e7b8b", raw: "#8a6d3b", ods: "#b7791f", kafka: "#8e4ec6", dwd: "#2f6fed", stg: "#5a8dee", int: "#5a8dee", dim: "#12a594", dws: "#0d8a78", ads: "#e5484d", meta: "#9aa4b2" };

export default function Lineage() {
  const t = useT();
  const q = useApi<J>("/datasets/lineage");
  return (
    <>
      <h1>{t("Data Lineage")}</h1>
      <p className="sub">{t("Dataset-level dependencies: dbt edges come from")} <code>manifest.json</code>{t("; the rest are registered at runtime by the tasks that move the data. Click a node to see its neighbours.")}</p>
      <Load q={q}>{(g) => <Graph g={g} />}</Load>
    </>
  );
}

function Graph({ g }: { g: J }) {
  const t = useT();
  const [hide, setHide] = useState(true);
  const [sel, setSel] = useState<string | null>(null);
  const nodes: J[] = g.nodes.filter((n: J) => !(hide && n.layer === "meta"));
  const ids = new Set(nodes.map((n) => n.id));
  const edges: J[] = g.edges.filter((e: J) => ids.has(e.upstream) && ids.has(e.downstream));
  const option = useMemo(() => {
    // one column per layer (stg + int share a column); nodes stacked and vertically centred
    const col = (l: string) => (l === "int" ? LAYERS.indexOf("stg") : LAYERS.indexOf(l));
    const cols: Record<number, J[]> = {};
    [...nodes].sort((a, b) => LAYERS.indexOf(a.layer) - LAYERS.indexOf(b.layer) || a.id.localeCompare(b.id)).forEach((n) => (cols[col(n.layer)] ??= []).push(n));
    const used = Object.keys(cols).map(Number).sort((a, b) => a - b);
    const place: Record<string, [number, number]> = {};
    used.forEach((c, xi) => cols[c].forEach((n, i) => (place[n.id] = [xi * 150, (i - (cols[c].length - 1) / 2) * 52])));
    const short = (id: string) => id.slice(id.indexOf(".") + 1);
    const near = new Set<string>(sel ? [sel, ...edges.filter((e) => e.upstream === sel || e.downstream === sel).flatMap((e) => [e.upstream, e.downstream])] : []);
    return {
      tooltip: { formatter: (p: J) => p.dataType === "edge" ? `${p.data.source} → ${p.data.target}<br/>${p.data.task} (${p.data.origin})` : `<b>${p.data.name}</b><br/>${p.data.layer} · ${p.data.kind}${p.data.rows != null ? ` · ${fmt(p.data.rows)} rows` : ""}` },
      series: [{ type: "graph", layout: "none", roam: true, draggable: true, edgeSymbol: ["none", "arrow"], edgeSymbolSize: 6,
        label: { show: true, position: "bottom", fontSize: 10, width: 118, overflow: "truncate", formatter: (p: J) => short(p.data.name) },
        data: nodes.map((n) => ({ name: n.id, layer: n.layer, kind: n.kind, rows: n.row_count, x: place[n.id][0], y: place[n.id][1],
          symbolSize: n.kind === "topic" ? 18 : n.kind === "view" ? 13 : 15, symbol: n.kind === "topic" ? "diamond" : n.kind === "view" ? "roundRect" : "circle",
          itemStyle: { color: COLORS[n.layer] ?? "#999", opacity: !sel || near.has(n.id) ? 1 : 0.18 }, label: { opacity: !sel || near.has(n.id) ? 1 : 0.25 } })),
        links: edges.map((e) => ({ source: e.upstream, target: e.downstream, task: e.task_id, origin: e.source,
          lineStyle: { color: e.source === "dbt" ? "#8fb0f5" : "#d9a441", width: sel && (e.upstream === sel || e.downstream === sel) ? 2.6 : 1, opacity: !sel || e.upstream === sel || e.downstream === sel ? 0.95 : 0.1, curveness: 0.12 } })) }],
    };
  }, [nodes, edges, sel]);
  const up = sel ? edges.filter((e) => e.downstream === sel) : [];
  const down = sel ? edges.filter((e) => e.upstream === sel) : [];
  return (
    <>
      <Card title={t("{n} datasets · {m} edges", { n: nodes.length, m: edges.length })} right={<label className="muted"><input type="checkbox" checked={hide} onChange={(e) => setHide(e.target.checked)} /> {t("hide meta tables")}</label>}>
        <div style={{ display: "flex", gap: 14, flexWrap: "wrap", marginBottom: 6 }}>{LAYERS.filter((l) => nodes.some((n) => n.layer === l)).map((l) => <span key={l}><i style={{ display: "inline-block", width: 10, height: 10, borderRadius: 5, background: COLORS[l], marginRight: 5 }} />{l}</span>)}
          <span><i style={{ display: "inline-block", width: 18, height: 2, background: "#9db8f3", marginRight: 5, verticalAlign: "middle" }} />dbt</span><span><i style={{ display: "inline-block", width: 18, height: 2, background: "#d9a441", marginRight: 5, verticalAlign: "middle" }} />{t("runtime")}</span></div>
        <Chart option={option} height={720} onClick={(p) => p.dataType === "node" && setSel(sel === p.name ? null : p.name)} />
      </Card>
      {sel && <div style={{ marginTop: 14 }}><Card title={<>{t("Selected:")} <span className="mono">{sel}</span> <Link to={`/catalog?dataset=${encodeURIComponent(sel)}`}>{t("open in catalog →")}</Link></>}>
        <div className="two"><div><b>{t("Upstream ({n})", { n: up.length })}</b>{up.map((e) => <div key={e.upstream} className="mono"><a href="#/lineage" onClick={() => setSel(e.upstream)}>{e.upstream}</a> <Badge>{e.task_id}</Badge></div>)}</div>
          <div><b>{t("Downstream ({n})", { n: down.length })}</b>{down.map((e) => <div key={e.downstream} className="mono"><a href="#/lineage" onClick={() => setSel(e.downstream)}>{e.downstream}</a> <Badge>{e.task_id}</Badge></div>)}</div></div></Card></div>}
    </>
  );
}
