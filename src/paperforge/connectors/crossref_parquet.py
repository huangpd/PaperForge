"""Offline Crossref dump (flattened parquet) connector: domain-stratified deterministic sampling."""
import json
from collections.abc import Iterator
from pathlib import Path

import duckdb
import yaml

from paperforge.connectors.base import OnRequest, Page, SourceConnector, SourceRecord, WorkUnit
from paperforge.processing.normalizer import norm_doi, parse_crossref_indexed

DEFAULT_DOMAINS = Path(__file__).resolve().parents[1] / "config" / "domains.yaml"


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def domain_case_sql(domains: dict) -> str:
    """CASE expression assigning each row to the first matching domain."""
    whens = []
    for name, cfg in domains.items():
        conds = []
        if cfg.get("doi_prefixes"):
            conds.append("split_part(DOI,'/',1) IN (" + ",".join(_q(p) for p in cfg["doi_prefixes"]) + ")")
        if cfg.get("journal_regex"):
            conds.append(f"regexp_matches(lower(coalesce(\"container-title\"[1],'')), {_q(cfg['journal_regex'])})")
        whens.append(f"WHEN {' OR '.join(conds)} THEN {_q(name)}" if conds else f"ELSE {_q(name)}")
    return "CASE " + " ".join(whens) + " END"


def split_quota(total: int, shares: dict[str, float]) -> dict[str, int]:
    names = list(shares)
    q = {n: int(total * shares[n]) for n in names}
    q[names[-1]] += total - sum(q.values())  # remainder to last domain
    return q


class CrossrefParquetConnector(SourceConnector):
    name = "crossref"

    def __init__(self, parquet_dir: str, domains_file: Path = DEFAULT_DOMAINS,
                 files: list[str] | None = None, page_size: int = 1000,
                 memory_limit: str = "10GB", threads: int = 4):
        cfg = yaml.safe_load(Path(domains_file).read_text())
        self.domains = cfg["domains"]
        if abs(sum(d["share"] for d in self.domains.values()) - 1) > 1e-6:
            raise ValueError("domain shares must sum to 1")
        d = Path(parquet_dir)
        self.files = files or [str(d / f"part-{n:05d}.parquet") for n in cfg["sample_parts"]]
        self.page_size = page_size
        self.memory_limit, self.threads = memory_limit, threads

    def plan(self, limit: int, **_) -> list[WorkUnit]:
        quotas = split_quota(limit, {n: c["share"] for n, c in self.domains.items()})
        return [WorkUnit(key=f"domain={n}", params={"domain": n, "quota": q}) for n, q in quotas.items() if q > 0]

    def _select_sql(self, unit: WorkUnit) -> str:
        """Phase 1: narrow columns only (cheap) -> deterministic DOI selection for this domain."""
        files = "[" + ",".join(_q(f) for f in self.files) + "]"
        return f"""
          SELECT DOI FROM (
            SELECT DOI, {domain_case_sql(self.domains)} AS _domain
            FROM read_parquet({files})
            WHERE type = 'journal-article' AND DOI IS NOT NULL AND title IS NOT NULL AND len(title) > 0
          ) WHERE _domain = {_q(unit.params['domain'])}
          ORDER BY hash(DOI), DOI
          LIMIT {int(unit.params['quota'])}"""

    def fetch(self, unit: WorkUnit, cursor: str | None = None, on_request: OnRequest | None = None) -> Iterator[Page]:
        skip = int(cursor) if cursor else 0  # cursor = rows already ingested; ordering by DOI is deterministic
        files = "[" + ",".join(_q(f) for f in self.files) + "]"
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{self.memory_limit}'; SET threads={int(self.threads)}; SET preserve_insertion_order=false")
        try:
            con.execute(f"CREATE TEMP TABLE sel AS {self._select_sql(unit)}")
            # Phase 2: full rows for the selected DOIs only. Selection is a set; ORDER BY DOI makes paging stable.
            cur = con.execute(f"""SELECT to_json(t) FROM (
                SELECT * FROM read_parquet({files}) WHERE type = 'journal-article' AND DOI IN (SELECT DOI FROM sel)
                ORDER BY DOI) t""")
            seen = 0
            while seen < skip:
                n = len(cur.fetchmany(min(self.page_size, skip - seen)))
                if n == 0:
                    return
                seen += n
            while True:
                rows = cur.fetchmany(self.page_size)
                if not rows:
                    return
                recs = [self._record(r[0]) for r in rows]
                seen += len(rows)
                yield Page(recs, next_cursor=str(seen) if seen < unit.params["quota"] else None)
        finally:
            con.close()

    def _record(self, js: str) -> SourceRecord:
        payload = {k: v for k, v in json.loads(js).items() if v is not None}
        doi = norm_doi(payload.get("DOI"))
        return SourceRecord(self.name, doi or "", doi, parse_crossref_indexed(payload), payload)
