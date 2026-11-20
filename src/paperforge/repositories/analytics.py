"""AnalyticsRepository: read-side queries for the API. PostgreSQL serves live queries; DuckDB reads the parquet archive
for offline reconciliation (never used for writes)."""
import json
from abc import ABC, abstractmethod

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from paperforge.processing.normalizer import norm_doi

SORTS = {"year_desc": "issued_year DESC NULLS LAST, paper_id", "year_asc": "issued_year ASC NULLS LAST, paper_id",
         "cited_desc": "cited_by_count DESC NULLS LAST, paper_id", "title_asc": "title ASC NULLS LAST, paper_id"}
EXPORT_COLS = ["doi", "arxiv_id", "primary_category", "title", "journal_title", "publisher", "issued_year", "type", "cited_by_count", "author_count"]


class AnalyticsRepository(ABC):
    @abstractmethod
    def search_papers(self, f: dict, page: int, page_size: int, sort: str) -> dict: ...
    @abstractmethod
    def get_paper(self, paper_id: str) -> dict | None: ...
    @abstractmethod
    def paper_versions(self, paper_id: str) -> list[dict] | None: ...
    @abstractmethod
    def overview(self) -> dict: ...
    @abstractmethod
    def quality(self, quarantine_limit: int) -> dict: ...
    @abstractmethod
    def pipeline_runs(self, limit: int) -> dict: ...
    @abstractmethod
    def lineage(self, dataset: str | None, direction: str) -> dict: ...
    @abstractmethod
    def catalog(self, dataset: str | None) -> dict | list: ...
    @abstractmethod
    def reconciliation(self, data_dir: str) -> dict: ...


def _snippet(text: str | None, limit: int) -> str | None:
    """First `limit` characters, cut at a word boundary, with an ellipsis when shortened."""
    if not text:
        return None
    if len(text) <= limit:
        return text
    cut = text[:limit].rsplit(" ", 1)[0].rstrip(" ,;:.-")
    return cut + "…"


def _where(f: dict) -> tuple[str, list]:
    cond, args = [], []
    if f.get("doi"):
        d = norm_doi(f["doi"]) or ""
        cond.append("doi = %s"); args.append(d)
    if f.get("arxiv_id"):
        cond.append("arxiv_id = %s"); args.append(f["arxiv_id"].strip().lower().removeprefix("arxiv:"))
    if f.get("category"):
        cond.append("categories @> ARRAY[%s]::text[]"); args.append(f["category"].strip())
    if f.get("doi_prefix"):
        cond.append("doi LIKE %s"); args.append((norm_doi(f["doi_prefix"]) or "").replace("%", "") + "%")
    if f.get("q"):
        cond.append("search_tsv @@ websearch_to_tsquery('simple', %s)"); args.append(f["q"])
    if f.get("title"):
        cond.append("title ILIKE %s"); args.append(f"%{f['title']}%")
    if f.get("journal"):
        cond.append("(journal_title ILIKE %s OR journal_key = %s)"); args += [f"%{f['journal']}%", f["journal"]]
    if f.get("publisher"):
        cond.append("(publisher ILIKE %s OR publisher_key = %s)"); args += [f"%{f['publisher']}%", f["publisher"]]
    if f.get("year") is not None:
        cond.append("issued_year = %s"); args.append(f["year"])
    if f.get("year_from") is not None:
        cond.append("issued_year >= %s"); args.append(f["year_from"])
    if f.get("year_to") is not None:
        cond.append("issued_year <= %s"); args.append(f["year_to"])
    return (" WHERE " + " AND ".join(cond)) if cond else "", args


class PgAnalyticsRepository(AnalyticsRepository):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def _q(self, sql: str, args=None, one=False):
        with self.pool.connection() as c, c.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, args or None)
            return cur.fetchone() if one else cur.fetchall()

    # ---- papers --------------------------------------------------------------------------------
    def search_papers(self, f: dict, page: int, page_size: int, sort: str) -> dict:
        where, args = _where(f)
        order = SORTS.get(sort, SORTS["year_desc"])
        if f.get("q") and sort == "relevance":
            order = "ts_rank(search_tsv, websearch_to_tsquery('simple', %s)) DESC, paper_id"
            order_args = [f["q"]]
        else:
            order_args = []
        total = self._q(f"SELECT count(*) AS n FROM ads.paper_search{where}", args, one=True)["n"]
        items = self._q(f"""SELECT paper_id::text, doi, arxiv_id, primary_category, categories, title, journal_title, publisher, journal_key, publisher_key,
                                   issued_year, type, cited_by_count, author_count, has_abstract
                            FROM ads.paper_search{where} ORDER BY {order} LIMIT %s OFFSET %s""",
                        [*args, *order_args, page_size, (page - 1) * page_size])
        self._decorate(items)
        return {"total": total, "page": page, "page_size": page_size, "items": items}

    SNIPPET_CHARS, LIST_AUTHORS = 360, 6

    def _decorate(self, items: list[dict]) -> None:
        """Add `authors` (first few display names) and `abstract` (snippet) to one page of results.
        Done per page from DWD (primary-key lookups on <=200 ids) instead of widening ads.paper_search: the data is never stale
        relative to DWD and the search table does not grow by a text column per paper."""
        if not items:
            return
        ids = [i["paper_id"] for i in items]
        names: dict[str, list[str]] = {}
        for r in self._q("""SELECT paper_id::text AS pid, coalesce(nullif(btrim(concat_ws(' ', given, family)), ''), name) AS n
                            FROM dwd.paper_author WHERE paper_id = ANY(%s::uuid[]) AND ordinal < %s ORDER BY paper_id, ordinal""",
                         [ids, self.LIST_AUTHORS]):
            if r["n"]:
                names.setdefault(r["pid"], []).append(r["n"])
        abstracts = {r["pid"]: r["a"] for r in self._q(
            """SELECT p.paper_id::text AS pid,
                      coalesce(p.abstract, (SELECT a.abstract FROM dwd.paper_arxiv a WHERE a.paper_id = p.paper_id AND a.abstract IS NOT NULL
                                            ORDER BY a.arxiv_id LIMIT 1)) AS a
               FROM dwd.paper p WHERE p.paper_id = ANY(%s::uuid[])""", [ids])}
        for i in items:
            i["authors"] = names.get(i["paper_id"], [])
            i["abstract"] = _snippet(abstracts.get(i["paper_id"]), self.SNIPPET_CHARS)

    def export_cursor(self, f: dict, limit: int):
        """Server-side cursor generator for CSV/JSONL export."""
        where, args = _where(f)
        with self.pool.connection() as c, c.transaction(), c.cursor(name="export", row_factory=dict_row) as cur:   # named cursors need a txn
            cur.execute(f"SELECT {', '.join(EXPORT_COLS)} FROM ads.paper_search{where} ORDER BY paper_id LIMIT %s", [*args, limit])
            yield from cur

    def get_paper(self, paper_id: str) -> dict | None:
        p = self._q("""SELECT p.paper_id::text, p.paper_key, p.primary_doi AS doi, p.title, p.abstract, p.container_title, p.publisher, p.member, p.type, p.language,
                              p.volume, p.issue, p.page, p.issns, p.issued_date, p.issued_year, p.issued_precision, p.reference_count,
                              p.cited_by_count, p.author_count, p.current_version, p.content_hash, p.source_updated_at, p.created_at, p.updated_at,
                              k.journal_key, k.publisher_key
                       FROM dwd.paper p LEFT JOIN int.int_paper_keys k USING (paper_id) WHERE p.paper_id = %s""", [paper_id], one=True)
        if not p:
            return None
        authors = self._q("SELECT ordinal, given, family, name, orcid, sequence, affiliations FROM dwd.paper_author WHERE paper_id=%s ORDER BY ordinal", [paper_id])
        refs = self._q("SELECT ordinal, ref_key, ref_doi, unstructured, article_title, journal_title, ref_year FROM dwd.paper_reference "
                       "WHERE paper_id=%s ORDER BY ordinal LIMIT 200", [paper_id])
        idents = self._q("SELECT scheme, value, source, status, valid_from, valid_to FROM dwd.paper_identifier WHERE paper_id=%s ORDER BY id", [paper_id])
        raw = self._q("SELECT payload, source, source_updated_at, batch_id, raw_file_id, raw_line, ingested_at FROM ods.crossref_works WHERE doi_norm=%s",
                      [p["doi"]], one=True) if p["doi"] else None
        arxiv = self._q("""SELECT a.arxiv_id, a.doi, a.primary_category, a.categories, a.title, a.abstract, a.journal_ref, a.comments, a.report_no,
                                  a.license, a.submitted_at, a.updated_date, a.version_count, a.raw_file_id, a.raw_line
                           FROM dwd.paper_arxiv a WHERE a.paper_id=%s ORDER BY a.arxiv_id""", [paper_id])
        if raw is None and arxiv:       # an arXiv-only paper: its source-native record is the arXiv one
            raw = self._q("SELECT payload, source, source_updated_at, batch_id, raw_file_id, raw_line, ingested_at FROM ods.arxiv_works "
                          "WHERE arxiv_id=%s", [arxiv[0]["arxiv_id"]], one=True)
        ver = self._q("SELECT version_no, snapshot, quality_flags, raw_file_id, raw_line FROM dwd.paper_version WHERE paper_id=%s AND version_no=%s",
                      [paper_id, p["current_version"]], one=True)
        p["abstract_source"] = (raw["source"] if raw else None) if p["abstract"] else None   # the record the paper content came from
        if not p["abstract"] and any(a["abstract"] for a in arxiv):      # Crossref has none; the arXiv abstract fills the gap
            p["abstract"], p["abstract_source"] = next(a["abstract"] for a in arxiv if a["abstract"]), "arxiv"
        return {**p, "authors": authors, "references": refs, "identifiers": idents, "arxiv": arxiv,
                "raw": raw, "normalized": ver["snapshot"] if ver else None, "quality_flags": ver["quality_flags"] if ver else []}

    def paper_versions(self, paper_id: str) -> list[dict] | None:
        if not self._q("SELECT 1 FROM dwd.paper WHERE paper_id=%s", [paper_id], one=True):
            return None
        rows = self._q("""SELECT version_no, content_hash, snapshot, source_updated_at, valid_from, valid_to, raw_file_id, raw_line,
                                 normalizer_version, quality_flags, created_at FROM dwd.paper_version WHERE paper_id=%s
                          ORDER BY source_updated_at, version_no""", [paper_id])
        prev = None
        for r in rows:
            snap = r["snapshot"]
            r["changed_fields"] = sorted(k for k in snap if prev is None or snap.get(k) != prev.get(k)) if prev is not None else []
            prev = snap
            r["is_current"] = r["valid_to"] is None
        return rows

    # ---- overview ------------------------------------------------------------------------------
    def overview(self) -> dict:
        o = self._q("SELECT * FROM ads.paper_overview", one=True)
        raw_rows = self._q("SELECT coalesce(sum(record_count),0)::bigint AS n, count(*) AS files FROM ods.raw_file_manifest", one=True)
        q = self._q("""SELECT count(*) FILTER (WHERE passed) AS passed, count(*) AS total FROM ads.quality_overview
                       WHERE severity IN ('error','warning')""", one=True)
        by_year = self._q("SELECT issued_year, paper_count FROM dws.publication_year_statistics WHERE issued_year >= 1990 ORDER BY 1")
        journals = self._q("""SELECT s.journal_key, j.journal_title, s.paper_count FROM dws.journal_statistics s JOIN dim.journal j USING (journal_key)
                              ORDER BY s.paper_count DESC LIMIT 10""")
        publishers = self._q("""SELECT s.publisher_key, p.publisher_name, s.paper_count FROM dws.publisher_statistics s JOIN dim.publisher p USING (publisher_key)
                                ORDER BY s.paper_count DESC LIMIT 10""")
        daily = self._q("SELECT change_date, new_papers, new_versions, versions_written FROM dws.paper_change_daily ORDER BY 1 DESC LIMIT 30")
        return {**o, "raw_file_rows": raw_rows["n"], "raw_files": raw_rows["files"],
                "quality_pass_rate": round(q["passed"] / q["total"], 4) if q["total"] else None,
                "quality_rules_checked": q["total"], "by_year": by_year, "top_journals": journals,
                "top_publishers": publishers, "daily_changes": list(reversed(daily))}

    # ---- quality -------------------------------------------------------------------------------
    def quality(self, quarantine_limit: int) -> dict:
        rules = self._q("""SELECT rule_id, dimension, severity, dataset, description, engine, total_rows, failed_rows, passed, pass_rate, run_at
                           FROM ads.quality_overview ORDER BY (passed IS NOT FALSE), severity, dimension, rule_id""")
        metrics = [{"rule_id": r["rule_id"], "description": r["description"], "total": r["total_rows"], "missing": r["failed_rows"],
                    "coverage": round(1 - r["failed_rows"] / r["total_rows"], 4) if r["total_rows"] else None}
                   for r in rules if r["severity"] == "metric" and r["dimension"] == "completeness"]
        dims = self._q("""SELECT dimension, count(*) FILTER (WHERE severity IN ('error','warning')) AS rules,
                                 count(*) FILTER (WHERE severity IN ('error','warning') AND passed) AS passed
                          FROM ads.quality_overview GROUP BY 1 ORDER BY 1""")
        daily = self._q("SELECT * FROM dws.data_quality_daily ORDER BY stat_date DESC LIMIT 30")
        for d in daily:
            d.pop("_loaded_at", None)
        quarantine = self._q("""SELECT id, source, source_id, rule_id, rule_version, reason, status, created_at, replayed_at, payload
                                FROM meta.quarantine_record ORDER BY id DESC LIMIT %s""", [quarantine_limit])
        qsum = self._q("SELECT rule_id, status, count(*) AS n FROM meta.quarantine_record GROUP BY 1,2 ORDER BY 1,2")
        return {"rules": rules, "metrics": metrics, "dimensions": dims, "daily": list(reversed(daily)),
                "quarantine": quarantine, "quarantine_summary": qsum}

    # ---- pipelines -----------------------------------------------------------------------------
    def pipeline_runs(self, limit: int) -> dict:
        runs = self._q("""SELECT run_id::text, dag_id, external_run_id, trigger, status, params, started_at, ended_at FROM meta.pipeline_run
                          ORDER BY started_at DESC LIMIT %s""", [limit])
        ids = [r["run_id"] for r in runs]
        tasks = self._q("""SELECT run_id::text, task_id, attempt, status, rows_in, rows_out, rows_failed, data_version, started_at, ended_at,
                                  duration_ms, error FROM meta.task_run WHERE run_id::text = ANY(%s) ORDER BY started_at""", [ids]) if ids else []
        by = {}
        for t in tasks:
            by.setdefault(t["run_id"], []).append(t)
        for r in runs:
            r["tasks"] = by.get(r["run_id"], [])
        stages = self._q("""SELECT DISTINCT ON (cmd) cmd AS task_id, status, rows_in, rows_out, rows_failed, duration_ms, started_at, error
                            FROM (SELECT coalesce(details->>'command', task_id) AS cmd, * FROM meta.task_run) t ORDER BY cmd, started_at DESC""")
        totals = self._q("""SELECT coalesce(details->>'command', task_id) AS task_id, count(*) AS runs, count(*) FILTER (WHERE status='failed') AS failed,
                                   sum(rows_failed)::bigint AS rows_failed, round(avg(duration_ms))::bigint AS avg_ms FROM meta.task_run GROUP BY 1""")
        http = self._q("""SELECT coalesce(status_code::text,'transport-error') AS status, count(*) AS n, round(avg(elapsed_ms))::bigint AS avg_ms
                          FROM ods.ingestion_request GROUP BY 1 ORDER BY 1""")
        batches = self._q("""SELECT batch_id, source, mode, status, rows_seen, rows_inserted, rows_updated, rows_unchanged, rows_skipped, started_at, finished_at
                             FROM ods.ingestion_batch ORDER BY started_at DESC LIMIT 10""")
        return {"runs": runs, "stages": stages, "task_totals": totals, "http_requests": http, "batches": batches}

    # ---- lineage / catalog ---------------------------------------------------------------------
    def lineage(self, dataset: str | None, direction: str) -> dict:
        from paperforge.lineage.query import full_graph, trace
        with self.pool.connection() as c:
            if dataset:
                edges = trace(c, dataset, direction)
                return {"dataset": dataset, "direction": direction, "edges": edges}
            return full_graph(c)

    def catalog(self, dataset: str | None):
        if dataset:
            d = self._q("SELECT * FROM meta.dataset WHERE dataset_id=%s", [dataset], one=True)
            if d:
                d["upstream"] = [r["upstream"] for r in self._q("SELECT upstream FROM meta.lineage_edge WHERE downstream=%s ORDER BY 1", [dataset])]
                d["downstream"] = [r["downstream"] for r in self._q("SELECT downstream FROM meta.lineage_edge WHERE upstream=%s ORDER BY 1", [dataset])]
            return d
        return self._q("""SELECT dataset_id, layer, kind, description, grain, primary_key, unique_constraints, update_strategy, materialization,
                                 row_count, last_data_at, refreshed_at, jsonb_array_length(columns) AS column_count FROM meta.dataset ORDER BY layer, dataset_id""")

    # ---- DuckDB reconciliation -----------------------------------------------------------------
    def reconciliation(self, data_dir: str) -> dict:
        import duckdb
        pq = f"{data_dir}/parquet/**/*.parquet"
        con = duckdb.connect()
        try:
            pq_rows, pq_dois = con.execute(f"SELECT count(*), count(DISTINCT doi) FROM read_parquet('{pq}')").fetchall()[0]
        except Exception as e:  # noqa: BLE001 - no parquet yet
            return {"parquet_available": False, "error": str(e)[:200]}
        finally:
            con.close()
        raw = self._q("SELECT coalesce(sum(record_count),0)::bigint AS n FROM ods.raw_file_manifest", one=True)["n"]
        ods = self._q("SELECT count(*) AS n FROM ods.crossref_works", one=True)["n"]
        dwd = self._q("SELECT count(*) AS n FROM dwd.paper", one=True)["n"]
        quar = self._q("SELECT count(DISTINCT source_id) AS n FROM meta.quarantine_record WHERE status='open'", one=True)["n"]
        return {"parquet_available": True, "parquet_rows": pq_rows, "parquet_distinct_doi": pq_dois, "raw_manifest_rows": raw,
                "ods_rows": ods, "dwd_papers": dwd, "open_quarantine": quar,
                "checks": {"parquet_rows_equal_raw_rows": pq_rows == raw, "parquet_doi_equals_ods": pq_dois == ods,
                           "ods_equals_dwd_plus_quarantine": ods == dwd + quar}}
