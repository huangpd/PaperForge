import { useEffect, useState } from "react";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Json = any;

export async function api<T = Json>(path: string, params?: Record<string, string | number | undefined | null>): Promise<T> {
  const qs = new URLSearchParams();
  Object.entries(params ?? {}).forEach(([k, v]) => v !== undefined && v !== null && v !== "" && qs.set(k, String(v)));
  const url = `/api/v1${path}${qs.size ? `?${qs}` : ""}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} — ${url}`);
  return r.json();
}

export function exportUrl(params: Record<string, string | number | undefined>, format: "csv" | "jsonl"): string {
  const qs = new URLSearchParams({ format });
  Object.entries(params).forEach(([k, v]) => v !== undefined && v !== "" && qs.set(k, String(v)));
  return `/api/v1/papers/export?${qs}`;
}

export interface Async<T> { data?: T; error?: string; loading: boolean; reload: () => void }

export function useApi<T = Json>(path: string, params?: Record<string, string | number | undefined | null>, deps: unknown[] = []): Async<T> {
  const [state, set] = useState<{ data?: T; error?: string; loading: boolean }>({ loading: true });
  const [tick, setTick] = useState(0);
  const key = JSON.stringify(params ?? {});
  useEffect(() => {
    let live = true;
    set((s) => ({ ...s, loading: true, error: undefined }));
    api<T>(path, params).then((data) => live && set({ data, loading: false }), (e) => live && set({ error: String(e.message ?? e), loading: false }));
    return () => { live = false; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, key, tick, ...deps]);
  return { ...state, reload: () => setTick((t) => t + 1) };
}

export const fmt = (n: number | null | undefined): string => (n === null || n === undefined ? "—" : n.toLocaleString("en-US"));
export const pct = (x: number | null | undefined, d = 1): string => (x === null || x === undefined ? "—" : `${(x * 100).toFixed(d)}%`);
// The API returns UTC instants (ISO 8601, +00:00). Show them in the viewer's own time zone (e.g. UTC+8 in China), never as raw UTC.
const DT = new Intl.DateTimeFormat("sv-SE", { year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" });
export const tzLabel = (): string => {
  const off = -new Date().getTimezoneOffset();
  const h = Math.trunc(Math.abs(off) / 60), m = Math.abs(off) % 60;
  return `UTC${off < 0 ? "-" : "+"}${h}${m ? ":" + String(m).padStart(2, "0") : ""}`;
};
export const when = (s: string | null | undefined): string => {
  if (!s) return "—";
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;          // a plain date has no time zone: leave it alone
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? s : DT.format(d);     // sv-SE gives "YYYY-MM-DD HH:mm:ss"
};
export const dur = (ms: number | null | undefined): string => (ms === null || ms === undefined ? "—" : ms < 1000 ? `${ms} ms` : ms < 60000 ? `${(ms / 1000).toFixed(1)} s` : `${(ms / 60000).toFixed(1)} min`);
