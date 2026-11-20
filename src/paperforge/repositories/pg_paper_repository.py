import json
from collections.abc import Callable

import psycopg
from psycopg.types.json import Jsonb

from paperforge.db import connect
from paperforge.processing.entity_resolver import ExistingPaper, Incoming, PaperDecision
from paperforge.processing.models import Issue, Kept, ProcessResult, RawRecord
from paperforge.processing.normalizer import NORMALIZER_VERSION, content_dict, raw_payload_hash
from paperforge.processing.transformer import Frames
from paperforge.repositories.base import PaperRepository, RaceLost

PAPER_COLS = ["title", "abstract", "container_title", "publisher", "member", "type", "language", "volume", "issue",
              "page", "issns", "issued_date", "issued_year", "issued_precision", "reference_count", "cited_by_count",
              "author_count"]


class PgPaperRepository(PaperRepository):
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn
        self.conn: psycopg.Connection = connect(dsn)

    def reconnect(self) -> None:
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass
        self.conn = connect(self.dsn)

    def close(self):
        self.conn.close()

    # ---- quarantine ------------------------------------------------------------------------
    def quarantine(self, items: list[tuple[RawRecord, Issue]]) -> None:
        if not items:
            return
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO meta.quarantine_record(source, source_id, raw_file_id, raw_line, payload, payload_hash, rule_id, rule_version, reason) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                [(r.source, r.source_id or r.doi, r.raw_file_id, r.raw_line, Jsonb(r.payload), raw_payload_hash(r.payload), i.rule_id, i.version,
                  i.message) for r, i in items])

    # ---- main write path -------------------------------------------------------------------
    def upsert_batch(self, kept: list[Kept], frames: Frames, resolver: Callable) -> ProcessResult:
        res = ProcessResult(received=len(kept), warnings=sum(len(k.issues) for k in kept))
        if not kept:
            return res
        by_idx = {k.idx: k for k in kept}
        paper_rows = {r["rec_idx"]: r for r in frames.papers.to_dicts()}
        arx = [k for k in kept if k.n.arxiv is not None]
        with self.conn.transaction():
            side = self._route_arxiv(arx) if arx else {}          # arXiv records that must NOT write a paper version
            core = [k for k in kept if k.idx not in side]
            decisions: list[PaperDecision] = []
            if core:
                existing = self._load(sorted({k.n.key for k in core}))
                decisions = resolver([Incoming(k.idx, k.n.key, k.n.source_updated_at, k.n.content_hash) for k in core], existing)
                self._write(decisions, by_idx, paper_rows, frames)
            if arx:
                self._write_arxiv(arx, side, res)
        for d in decisions:
            for o in d.outcomes.values():
                setattr(res, o, getattr(res, o) + 1)
        return res

    def _load(self, keys: list[str]) -> dict[str, ExistingPaper]:
        """Existing papers by incoming key. A key matches the paper_key or the primary DOI (an arXiv-created paper may have
        key 'arxiv:<id>' and still carry the DOI that Crossref later announces)."""
        rows = self.conn.execute(
            "SELECT paper_key, primary_doi, paper_id, current_version, content_hash, source_updated_at FROM dwd.paper "
            "WHERE paper_key = ANY(%s) OR primary_doi = ANY(%s) ORDER BY paper_key FOR UPDATE", (keys, keys)).fetchall()
        wanted = set(keys)
        out = {(r[0] if r[0] in wanted else r[1]): ExistingPaper(r[2], r[3], r[4], r[5], []) for r in rows}
        by_id = {e.paper_id: e for e in out.values()}
        if by_id:
            for pid, vno, su, h in self.conn.execute(
                    "SELECT paper_id, version_no, source_updated_at, content_hash FROM dwd.paper_version WHERE paper_id = ANY(%s)",
                    (list(by_id),)):
                by_id[pid].versions.append((su, vno, h))
        return out

    # ---- arXiv ------------------------------------------------------------------------------
    def _route_arxiv(self, arx: list[Kept]) -> dict[int, tuple[str, object]]:
        """Decide which arXiv records may write core paper data (paper row + version) and which only enrich dwd.paper_arxiv.

        side-only (returned, rec_idx -> ('id', paper_id) | ('key', paper_key)):
          * the paper already has a Crossref version: Crossref owns the content, arXiv never writes versions (they would
            overwrite publisher/journal/ISSN/references with nothing and make the timeline flip-flop);
          * the record's DOI belongs to a paper reached through another arXiv id (several ids for one DOI);
          * a later record in this chunk for a paper that an earlier record creates.
        core (not returned): new papers, and updates of papers that only arXiv has described so far."""
        c = self.conn
        ids = [k.n.arxiv["arxiv_id"] for k in arx]
        dois = sorted({k.n.doi for k in arx if k.n.doi})
        linked = {r[0]: r[1:] for r in c.execute(
            "SELECT a.arxiv_id, p.paper_id, p.paper_key, EXISTS (SELECT 1 FROM dwd.paper_version v WHERE v.paper_id = p.paper_id "
            "AND v.source <> 'arxiv') FROM dwd.paper_arxiv a JOIN dwd.paper p USING (paper_id) WHERE a.arxiv_id = ANY(%s)", (ids,))}
        by_doi = {}
        if dois:
            for pid, pkey, pdoi in c.execute("SELECT paper_id, paper_key, primary_doi FROM dwd.paper WHERE primary_doi = ANY(%s) "
                                             "OR paper_key = ANY(%s)", (dois, dois)):
                by_doi[pdoi if pdoi in dois else pkey] = pid
        side, creating = {}, set()
        for k in arx:
            aid = k.n.arxiv["arxiv_id"]
            if aid in linked:
                pid, pkey, crossref_owned = linked[aid]
                if crossref_owned:
                    side[k.idx] = ("id", pid)
                else:
                    k.n.paper_key = pkey            # keep the paper's existing key even if the record now carries a DOI
            elif k.n.doi in by_doi:
                side[k.idx] = ("id", by_doi[k.n.doi])
            elif k.n.key in creating:
                side[k.idx] = ("key", k.n.key)
            else:
                creating.add(k.n.key)
        return side

    def _write_arxiv(self, arx: list[Kept], side: dict, res: ProcessResult) -> None:
        c = self.conn
        keys = sorted({k.n.key for k in arx if side.get(k.idx, ("key",))[0] == "key"})
        by_key = {r[0]: (r[1], r[2]) for r in c.execute(
            "SELECT paper_key, paper_id, primary_doi FROM dwd.paper WHERE paper_key = ANY(%s)", (keys,))} if keys else {}
        pids = {k.idx: (side[k.idx][1] if k.idx in side and side[k.idx][0] == "id" else by_key[k.n.key][0]) for k in arx}
        old = dict(c.execute("SELECT arxiv_id, content_hash FROM dwd.paper_arxiv WHERE arxiv_id = ANY(%s)",
                             ([k.n.arxiv["arxiv_id"] for k in arx],)).fetchall())
        rows, idents, side_idx = [], [], set(side)
        for k in arx:
            a, aid = k.n.arxiv, k.n.arxiv["arxiv_id"]
            rows.append((aid, pids[k.idx], a["doi"], a["primary_category"], a["categories"], a["title"], a["abstract"], a["journal_ref"],
                         a["comments"], a["report_no"], a["license"], a["submitted_at"], a["updated_date"], a["version_count"],
                         a["content_hash"], k.rec.raw_file_id, k.rec.raw_line))
            idents.append((pids[k.idx], "arxiv", aid, k.rec.source, k.n.source_updated_at))
            if k.idx in side_idx:         # core records are already counted by the resolver
                o = "inserted" if aid not in old else ("unchanged" if old[aid] == a["content_hash"] else "updated")
                setattr(res, o, getattr(res, o) + 1)
        with c.cursor() as cur:
            cur.executemany(
                "INSERT INTO dwd.paper_arxiv (arxiv_id, paper_id, doi, primary_category, categories, title, abstract, journal_ref, comments, "
                "report_no, license, submitted_at, updated_date, version_count, content_hash, raw_file_id, raw_line) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (arxiv_id) DO UPDATE SET paper_id=EXCLUDED.paper_id, doi=EXCLUDED.doi, primary_category=EXCLUDED.primary_category, "
                "categories=EXCLUDED.categories, title=EXCLUDED.title, abstract=EXCLUDED.abstract, journal_ref=EXCLUDED.journal_ref, "
                "comments=EXCLUDED.comments, report_no=EXCLUDED.report_no, license=EXCLUDED.license, submitted_at=EXCLUDED.submitted_at, "
                "updated_date=EXCLUDED.updated_date, version_count=EXCLUDED.version_count, content_hash=EXCLUDED.content_hash, "
                "raw_file_id=EXCLUDED.raw_file_id, raw_line=EXCLUDED.raw_line, updated_at=now() "
                "WHERE dwd.paper_arxiv.content_hash <> EXCLUDED.content_hash AND (dwd.paper_arxiv.updated_date IS NULL "
                "OR EXCLUDED.updated_date IS NULL OR EXCLUDED.updated_date >= dwd.paper_arxiv.updated_date)", rows)
            cur.executemany(
                "INSERT INTO dwd.paper_identifier(paper_id, scheme, value, source, valid_from) VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (scheme, value, source) WHERE valid_to IS NULL DO NOTHING", idents)
            # An arXiv-only paper whose record later gained a DOI: record the DOI unless another paper already has it.
            for k in arx:
                if not k.n.doi:
                    continue
                pid = pids[k.idx]
                cur.execute("UPDATE dwd.paper SET primary_doi=%s, updated_at=now() WHERE paper_id=%s AND primary_doi IS NULL "
                            "AND NOT EXISTS (SELECT 1 FROM dwd.paper o WHERE o.primary_doi=%s)", (k.n.doi, pid, k.n.doi))
                if cur.rowcount:
                    cur.execute("INSERT INTO dwd.paper_identifier(paper_id, scheme, value, source, valid_from) VALUES (%s,'doi',%s,%s,%s) "
                                "ON CONFLICT (scheme, value, source) WHERE valid_to IS NULL DO NOTHING",
                                (pid, k.n.doi, k.rec.source, k.n.source_updated_at))

    def _write(self, decisions, by_idx, paper_rows, frames: Frames):
        c = self.conn
        new, changed, refresh, versions, ids, touched, child_ids = [], [], [], [], [], [], []
        child_rec = {}   # paper_id -> rec_idx whose authors/refs are current
        for d in decisions:
            if d.adds or d.refresh_rec_idx is not None:
                touched.append(d.paper_id)
            for a in d.adds:
                k = by_idx[a.rec_idx]
                versions.append((d.paper_id, a.version_no, k.n.content_hash, Jsonb(self._snapshot(k)), k.n.source_updated_at,
                                 k.n.source_updated_at, k.rec.raw_file_id, k.rec.raw_line, NORMALIZER_VERSION,
                                 Jsonb([i.__dict__ for i in k.issues]), k.rec.source))
            if d.current_rec_idx is not None:
                r = dict(paper_rows[d.current_rec_idx])
                if d.refresh_rec_idx is not None:
                    # same content re-indexed later inside this very chunk: the paper row carries the freshest volatile fields
                    rf = paper_rows[d.refresh_rec_idx]
                    r["source_updated_at"], r["cited_by_count"] = rf["source_updated_at"], rf["cited_by_count"]
                vals = [r[col] for col in PAPER_COLS]
                cur_n = by_idx[d.current_rec_idx].n
                row = (d.paper_id, d.key, cur_n.doi, *vals, d.current_version_no, r["content_hash"], r["source_updated_at"])
                (new if d.is_new else changed).append(row)
                child_rec[d.paper_id] = d.current_rec_idx
                if d.is_new:
                    ids += [(d.paper_id, sch, val, by_idx[d.current_rec_idx].rec.source, r["source_updated_at"])
                            for sch, val in (cur_n.identifiers or [("doi", cur_n.doi)])]
            elif d.refresh_rec_idx is not None:
                r = paper_rows[d.refresh_rec_idx]
                refresh.append((r["source_updated_at"], r["cited_by_count"], d.paper_id))
        with c.cursor() as cur:
            if new:
                cur.executemany(
                    f"INSERT INTO dwd.paper (paper_id, paper_key, primary_doi, {', '.join(PAPER_COLS)}, current_version, content_hash, source_updated_at) "
                    f"VALUES (%s,%s,%s,{','.join(['%s'] * len(PAPER_COLS))},%s,%s,%s) ON CONFLICT DO NOTHING RETURNING paper_id",
                    new, returning=True)
                got = 0
                while True:
                    got += cur.fetchone() is not None
                    if not cur.nextset():
                        break
                if got != len(new):
                    raise RaceLost()
            if changed:
                sets = ", ".join(f"{col}=%s" for col in PAPER_COLS)
                cur.executemany(
                    f"UPDATE dwd.paper SET {sets}, current_version=%s, content_hash=%s, source_updated_at=%s, updated_at=now() "
                    f"WHERE paper_id=%s", [(*row[3:], row[0]) for row in changed])
            if refresh:
                cur.executemany("UPDATE dwd.paper SET source_updated_at=%s, cited_by_count=%s, updated_at=now() WHERE paper_id=%s", refresh)
            if ids:
                cur.executemany("INSERT INTO dwd.paper_identifier(paper_id, scheme, value, source, valid_from) VALUES (%s,%s,%s,%s,%s)", ids)
            if versions:
                cur.executemany(
                    "INSERT INTO dwd.paper_version(paper_id, version_no, content_hash, snapshot, source_updated_at, valid_from, "
                    "raw_file_id, raw_line, normalizer_version, quality_flags, source) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", versions)
            if child_rec:
                pids = list(child_rec)
                cur.execute("DELETE FROM dwd.paper_author WHERE paper_id = ANY(%s)", (pids,))
                cur.execute("DELETE FROM dwd.paper_reference WHERE paper_id = ANY(%s)", (pids,))
                rec_to_pid = {v: k for k, v in child_rec.items()}
                with cur.copy("COPY dwd.paper_author (paper_id, ordinal, given, family, name, orcid, sequence, affiliations) FROM STDIN") as cp:
                    for r in frames.authors.filter(frames.authors["rec_idx"].is_in(list(rec_to_pid))).iter_rows(named=True):
                        cp.write_row((rec_to_pid[r["rec_idx"]], r["ordinal"], r["given"], r["family"], r["name"], r["orcid"],
                                      r["sequence"], r["affiliations"] or []))
                with cur.copy("COPY dwd.paper_reference (paper_id, ordinal, ref_key, ref_doi, unstructured, article_title, journal_title, ref_year) FROM STDIN") as cp:
                    for r in frames.references.filter(frames.references["rec_idx"].is_in(list(rec_to_pid))).iter_rows(named=True):
                        cp.write_row((rec_to_pid[r["rec_idx"]], r["ordinal"], r["ref_key"], r["ref_doi"], r["unstructured"],
                                      r["article_title"], r["journal_title"], r["ref_year"]))
            if touched:
                # valid_to = start of the next newer version on the source_updated_at timeline
                cur.execute("""
                    UPDATE dwd.paper_version v SET valid_to = n.nxt
                    FROM (SELECT paper_id, version_no,
                                 lead(valid_from) OVER (PARTITION BY paper_id ORDER BY source_updated_at, version_no) AS nxt
                          FROM dwd.paper_version WHERE paper_id = ANY(%s)) n
                    WHERE v.paper_id = n.paper_id AND v.version_no = n.version_no AND v.valid_to IS DISTINCT FROM n.nxt""",
                            (touched,))

    @staticmethod
    def _snapshot(k: Kept) -> dict:
        d = content_dict(k.n)
        d["cited_by_count"] = k.n.cited_by_count
        return json.loads(json.dumps(d))
