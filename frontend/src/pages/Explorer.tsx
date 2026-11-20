import { Fragment, useEffect, useState } from "react";
import { api, exportUrl, fmt, useApi, when, Json as J } from "../api";
import { Badge, Card, Json, Load, statusBadge } from "../components/ui";
import { useT } from "../i18n";

// Canonical link: doi.org resolves any DOI to the publisher's page. Derived from the standardized `doi`, so it needs no extra field.
const doiUrl = (doi: string) => `https://doi.org/${doi.split("/").map(encodeURIComponent).join("/")}`;
// Publisher landing page: only present in the raw source record (Crossref resource.primary.URL); accept http(s) only.
const landingUrl = (raw: J) => { const u = raw?.payload?.resource?.primary?.URL; return typeof u === "string" && /^https?:\/\//i.test(u) ? u : null; };
// arXiv: canonical abs page (the id is what arXiv itself uses in URLs; old-style ids like hep-th/9901001 contain a slash).
const arxivUrl = (id: string) => `https://arxiv.org/abs/${id.split("/").map(encodeURIComponent).join("/")}`;
const pdfUrl = (id: string) => `https://arxiv.org/pdf/${id.split("/").map(encodeURIComponent).join("/")}`;
const Ext = ({ href, children, title }: { href: string; children: React.ReactNode; title?: string }) =>
  <a href={href} target="_blank" rel="noopener noreferrer" title={title} onClick={(e) => e.stopPropagation()}>{children} ↗</a>;

const EMPTY = { q: "", doi: "", arxiv_id: "", category: "", title: "", journal: "", publisher: "", year_from: "", year_to: "" };

export default function Explorer() {
  const t = useT();
  const [form, setForm] = useState(EMPTY);
  const [applied, setApplied] = useState(EMPTY);
  const [page, setPage] = useState(1);
  const [sort, setSort] = useState("year_desc");
  const [sel, setSel] = useState<string | null>(null);
  const params = { ...applied, page, page_size: 20, sort };
  const q = useApi<J>("/papers", params);
  const ex = Object.fromEntries(Object.entries(applied).filter(([, v]) => v));
  const set = (k: keyof typeof EMPTY) => (e: React.ChangeEvent<HTMLInputElement>) => setForm({ ...form, [k]: e.target.value });
  const submit = (e: React.FormEvent) => { e.preventDefault(); setApplied(form); setPage(1); };
  return (
    <>
      <h1>{t("Data Explorer")}</h1>
      <p className="sub">{t("Search standardized papers; open one to compare the raw source record with the standardized result and browse its version history.")}</p>
      <Card>
        <form className="filters" onSubmit={submit}>
          <label>{t("Full text")}<input value={form.q} onChange={set("q")} placeholder={t("title, journal, author")} /></label>
          <label>{t("DOI")}<input value={form.doi} onChange={set("doi")} placeholder="10.1000/xyz" /></label>
          <label>{t("arXiv id")}<input value={form.arxiv_id} onChange={set("arxiv_id")} placeholder="2210.03629" /></label>
          <label>{t("arXiv category")}<input value={form.category} onChange={set("category")} placeholder="cs.CL" size={8} /></label>
          <label>{t("Title contains")}<input value={form.title} onChange={set("title")} /></label>
          <label>{t("Journal / ISSN key")}<input value={form.journal} onChange={set("journal")} /></label>
          <label>{t("Publisher / member id")}<input value={form.publisher} onChange={set("publisher")} /></label>
          <label>{t("Year from")}<input value={form.year_from} onChange={set("year_from")} size={5} inputMode="numeric" /></label>
          <label>{t("Year to")}<input value={form.year_to} onChange={set("year_to")} size={5} inputMode="numeric" /></label>
          <label>{t("Sort")}<select value={sort} onChange={(e) => { setSort(e.target.value); setPage(1); }}>
            <option value="year_desc">{t("Newest")}</option><option value="year_asc">{t("Oldest")}</option><option value="cited_desc">{t("Most cited")}</option><option value="title_asc">{t("Title A–Z")}</option>{applied.q && <option value="relevance">{t("Relevance")}</option>}</select></label>
          <button className="primary" type="submit">{t("Search")}</button>
          <button type="button" onClick={() => { setForm(EMPTY); setApplied(EMPTY); setPage(1); }}>{t("Reset")}</button>
        </form>
      </Card>
      <div style={{ height: 14 }} />
      <Card title={q.data ? t("{n} papers", { n: fmt(q.data.total) }) : t("Papers")} right={<>
        <a className="btn" href={exportUrl({ ...ex } as never, "csv")}>{t("Export CSV")}</a>{" "}<a className="btn" href={exportUrl({ ...ex } as never, "jsonl")}>{t("Export JSONL")}</a></>} pad={false}>
        <Load q={q}>{(d) => <>
          <div className="scroll"><table><thead><tr><th>{t("DOI / arXiv")}</th><th>{t("Title")}</th><th>{t("Category")}</th><th>{t("Journal")}</th><th>{t("Publisher")}</th><th className="num">{t("Year")}</th><th className="num">{t("Cited")}</th><th className="num">{t("Authors")}</th></tr></thead>
            <tbody>{d.items.map((p: J) => <tr key={p.paper_id} className={`click ${sel === p.paper_id ? "sel" : ""}`} onClick={() => setSel(p.paper_id)}>
              <td className="mono nw">{p.doi && <div><Ext href={doiUrl(p.doi)} title={t("Open on doi.org")}>{p.doi}</Ext></div>}{p.arxiv_id && <div><Ext href={arxivUrl(p.arxiv_id)} title={t("Open on arXiv")}>arXiv:{p.arxiv_id}</Ext></div>}</td><td className="paper-cell">
                <div className="paper-title">{p.title ?? <span className="muted">{t("(no title)")}</span>}</div>
                {p.authors?.length > 0 && <div className="paper-authors"><span className="tag">{t("By")}</span>{p.authors.join(", ")}{p.author_count > p.authors.length && <span className="muted"> {t("+{n} more", { n: p.author_count - p.authors.length })}</span>}</div>}
                <div className={`paper-abs ${p.abstract ? "" : "none"}`}>{p.abstract ? <><span className="tag">{t("Abstract")}</span>{p.abstract}</> : t("No abstract")}</div></td><td className="nw">{p.primary_category ?? ""}</td><td>{p.journal_title}</td><td>{p.publisher}</td><td className="num">{p.issued_year ?? "—"}</td><td className="num">{fmt(p.cited_by_count)}</td><td className="num">{p.author_count}</td></tr>)}
              {!d.items.length && <tr><td colSpan={8} className="muted">{t("No papers match.")}</td></tr>}</tbody></table></div>
          <div className="pager"><span className="muted">{t("page {p} of {n}", { p: d.page, n: Math.max(1, Math.ceil(d.total / d.page_size)) })}</span>
            <button disabled={page <= 1} onClick={() => setPage(page - 1)}>{t("Prev")}</button><button disabled={page * d.page_size >= d.total} onClick={() => setPage(page + 1)}>{t("Next")}</button></div></>}</Load>
      </Card>
      {sel && <Detail id={sel} onClose={() => setSel(null)} />}
    </>
  );
}

const a0 = (p: J): J | undefined => p.arxiv?.[0];

function Detail({ id, onClose }: { id: string; onClose: () => void }) {
  const t = useT();
  const [tab, setTab] = useState("standardized");
  const [v, setV] = useState<J>();
  const q = useApi<J>(`/papers/${id}`);
  useEffect(() => { setV(undefined); api(`/papers/${id}/versions`).then(setV); }, [id]);
  return (
    <div className="drawer">
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}><div><h1 style={{ marginBottom: 2 }}>{q.data?.title ?? t("Paper")}</h1><div className="mono muted">{q.data?.doi ?? q.data?.paper_key}</div>
        {q.data && <div style={{ marginTop: 4 }}>{[
          q.data.doi && <Ext key="d" href={doiUrl(q.data.doi)}>{t("Open on doi.org")}</Ext>,
          landingUrl(q.data.raw) && <Ext key="l" href={landingUrl(q.data.raw)!}>{t("Publisher page")}</Ext>,
          ...(q.data.arxiv ?? []).flatMap((a: J) => [<Ext key={`a${a.arxiv_id}`} href={arxivUrl(a.arxiv_id)}>arXiv:{a.arxiv_id}</Ext>, <Ext key={`p${a.arxiv_id}`} href={pdfUrl(a.arxiv_id)}>{t("PDF")}</Ext>]),
        ].filter(Boolean).map((x, i) => <Fragment key={i}>{i > 0 && " · "}{x}</Fragment>)}</div>}</div><button onClick={onClose}>{t("Close")}</button></div>
      <div className="tabs" style={{ marginTop: 14 }}>{[["standardized", t("Standardized")], ["compare", t("Raw vs standardized")], ["versions", v ? t("Versions ({n})", { n: v.versions.length }) : t("Versions")]].map(([k, l]) =>
        <button key={k} className={tab === k ? "on" : ""} onClick={() => setTab(k)}>{l}</button>)}</div>
      <Load q={q}>{(p) => <>
        {tab === "standardized" && <>
          <dl className="dl">{([["paper_id", p.paper_id], [t("Journal"), p.container_title], ["ISSN", (p.issns ?? []).join(", ")], [t("Journal key"), p.journal_key], [t("Publisher"), `${p.publisher ?? ""} (member ${p.member ?? "—"})`],
            [t("Published"), `${p.issued_date ?? "—"} (${p.issued_precision ?? "n/a"})`], [t("Type / language"), `${p.type ?? "—"} / ${p.language ?? "—"}`], [t("Cited by / refs"), `${fmt(p.cited_by_count)} / ${fmt(p.reference_count)}`],
            [t("Updated at source"), when(p.source_updated_at)], [t("Current version"), p.current_version], [t("Content hash"), p.content_hash]] as [string, J][]).map(([k, val]) => <Fragment key={k}><dt>{k}</dt><dd className={k === "paper_id" || k === t("Content hash") ? "mono" : ""}>{val}</dd></Fragment>)}</dl>
          <h3>{t("Identifiers")}</h3><table><thead><tr><th>{t("Scheme")}</th><th>{t("Value")}</th><th>{t("Status")}</th><th>{t("Valid from")}</th><th>{t("Valid to")}</th></tr></thead><tbody>{p.identifiers.map((i: J, n: number) => <tr key={n}><td>{i.scheme}</td><td className="mono">{i.value}</td><td>{statusBadge(i.status)}</td><td>{when(i.valid_from)}</td><td>{when(i.valid_to)}</td></tr>)}</tbody></table>
          {p.arxiv?.length > 0 && <><h3>{t("arXiv ({n})", { n: p.arxiv.length })}</h3><table><thead><tr><th>{t("arXiv id")}</th><th>{t("Categories")}</th><th>{t("Journal ref")}</th><th>{t("License")}</th><th>{t("Versions")}</th><th>{t("Submitted")}</th><th>{t("Updated")}</th></tr></thead>
            <tbody>{p.arxiv.map((a: J) => <tr key={a.arxiv_id}><td className="mono"><Ext href={arxivUrl(a.arxiv_id)}>{a.arxiv_id}</Ext></td><td>{(a.categories ?? []).map((c: string) => <span key={c} className="chip">{c}</span>)}</td><td>{a.journal_ref ?? <span className="muted">—</span>}</td><td>{(a.license ?? "").replace(/^https?:\/\/(arxiv|creativecommons)\.org\/licenses\//, "").replace(/\/$/, "") || "—"}</td><td className="num">{a.version_count}</td><td>{when(a.submitted_at)}</td><td>{a.updated_date ?? "—"}</td></tr>)}</tbody></table>
            {a0(p)?.comments && <div className="muted" style={{ marginTop: 6 }}>{t("Author comments")}: {a0(p).comments}</div>}</>}
          {p.abstract && <><h3>{t("Abstract")}{p.abstract_source === "arxiv" && <span className="muted"> ({t("from arXiv")})</span>}</h3><p>{p.abstract}</p></>}
          <h3>{t("Authors ({n})", { n: p.authors.length })}</h3><div>{p.authors.map((a: J) => <div key={a.ordinal}>{a.given} {a.family ?? a.name} {a.orcid && <span className="chip">ORCID {a.orcid}</span>}<span className="muted"> {(a.affiliations ?? []).join("; ")}</span></div>)}{!p.authors.length && <span className="muted">{t("none in source")}</span>}</div>
          <h3>{p.references.length < (p.reference_count ?? 0) ? t("References ({n}, first {m} shown)", { n: fmt(p.reference_count), m: p.references.length }) : t("References ({n})", { n: fmt(p.reference_count) })}</h3>
          <div className="scroll" style={{ maxHeight: 260 }}><table><tbody>{p.references.map((r: J) => <tr key={r.ordinal}><td>{r.ordinal + 1}</td><td className="mono">{r.ref_doi ?? ""}</td><td>{r.article_title ?? r.unstructured}</td><td>{r.ref_year ?? ""}</td></tr>)}</tbody></table></div>
          {p.quality_flags?.length > 0 && <><h3>{t("Quality warnings")}</h3>{p.quality_flags.map((f: J, i: number) => <div key={i}><Badge kind="warn">{f.rule_id}</Badge> {f.message}</div>)}</>}
        </>}
        {tab === "compare" && <div className="two">
          <div><h3>{t("Raw source record")} <span className="muted">{t("(ODS, batch {b}, file {f} line {l})", { b: p.raw?.batch_id, f: p.raw?.raw_file_id, l: p.raw?.raw_line })}</span></h3><Json value={p.raw?.payload} height={620} /></div>
          <div><h3>{t("Standardized snapshot")} <span className="muted">{t("(DWD version {v})", { v: p.current_version })}</span></h3><Json value={p.normalized} height={620} /></div></div>}
        {tab === "versions" && (v ? <table><thead><tr><th>#</th><th>{t("Source updated")}</th><th>{t("Valid from → to")}</th><th>{t("Changed fields")}</th><th>{t("Title in this version")}</th><th>{t("Raw file/line")}</th></tr></thead>
          <tbody>{v.versions.map((x: J) => <tr key={x.version_no}><td>{x.version_no} {x.is_current && <Badge kind="ok">{t("current")}</Badge>}</td><td>{when(x.source_updated_at)}</td><td>{when(x.valid_from)} → {x.valid_to ? when(x.valid_to) : t("now")}</td>
            <td>{x.changed_fields.length ? x.changed_fields.map((c: string) => <span key={c} className="chip">{c}</span>) : <span className="muted">{t("(first on timeline)")}</span>}</td><td>{x.snapshot.title}</td><td className="mono">{x.raw_file_id}/{x.raw_line}</td></tr>)}</tbody></table> : <div className="muted">{t("Loading…")}</div>)}
      </>}</Load>
    </div>
  );
}
