import { ReactNode } from "react";
import { Async } from "../api";
import { useT } from "../i18n";

export const Card = ({ title, right, children, pad = true }: { title?: ReactNode; right?: ReactNode; children: ReactNode; pad?: boolean }) => (
  <section className="card">
    {(title || right) && <header className="card-h"><h3>{title}</h3><div>{right}</div></header>}
    <div className={pad ? "card-b" : ""}>{children}</div>
  </section>
);

export const Stat = ({ label, value, sub, tone }: { label: string; value: ReactNode; sub?: ReactNode; tone?: "ok" | "warn" | "bad" }) => (
  <div className={`stat ${tone ?? ""}`}><div className="stat-l">{label}</div><div className="stat-v">{value}</div>{sub && <div className="stat-s">{sub}</div>}</div>
);

export const Badge = ({ kind, children }: { kind?: string; children: ReactNode }) => <span className={`badge ${kind ?? ""}`}>{children}</span>;

// status words from the API are translated at render time
const TStatus = ({ v }: { v: string }) => { const t = useT(); return <>{t(v)}</>; };

export const statusBadge = (s: string | boolean | null | undefined) => {
  const v = s === true ? "pass" : s === false ? "fail" : s ?? "—";
  const k = ["success", "pass", "completed", "ok", "active", "done"].includes(String(v)) ? "ok"
    : ["failed", "fail", "error"].includes(String(v)) ? "bad" : ["running", "warning", "open", "retired", "pending"].includes(String(v)) ? "warn" : "";
  return <Badge kind={k}><TStatus v={String(v)} /></Badge>;
};

export function Load<T>({ q, children }: { q: Async<T>; children: (d: T) => ReactNode }) {
  const t = useT();
  if (q.error) return <div className="err">{t("Could not load data:")} {q.error} <button onClick={q.reload}>{t("retry")}</button></div>;
  if (!q.data) return <div className="muted pad">{t("Loading…")}</div>;
  return <>{children(q.data)}</>;
}

export const Json = ({ value, height = 420 }: { value: unknown; height?: number }) => <pre className="json" style={{ maxHeight: height }}>{JSON.stringify(value, null, 2)}</pre>;
