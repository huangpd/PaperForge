"""Crossref REST API connector: cursor pagination over index-date windows."""
import threading
import time
from collections.abc import Iterator
from datetime import date, timedelta
from urllib.parse import quote

import httpx
from tenacity import RetryError, Retrying, retry_if_exception, stop_after_attempt, wait_exponential_jitter

from paperforge.connectors.base import OnRequest, Page, RequestStat, SourceConnector, SourceRecord, WorkUnit
from paperforge.processing.normalizer import norm_doi, parse_crossref_indexed


class CursorExpired(Exception):
    pass


class NotFound(Exception):
    """Crossref has no record for the requested DOI (HTTP 404). Not retried."""


class NotJournalArticle(Exception):
    """The DOI resolves, but to a record type this platform does not ingest."""


class _Retryable(Exception):
    def __init__(self, msg: str, retry_after: float | None = None):
        super().__init__(msg)
        self.retry_after = retry_after


class RateLimiter:
    """Shared across threads: at most `rate` requests per second."""

    def __init__(self, rate: float):
        self.interval = 1.0 / rate if rate > 0 else 0
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            t = max(now, self._next)
            self._next = t + self.interval
        if t > now:
            time.sleep(t - now)


class CrossrefApiConnector(SourceConnector):
    name = "crossref"

    def __init__(self, base_url: str = "https://api.crossref.org/works", rate_per_sec: float = 5,
                 rows: int = 1000, timeout: float = 60, max_attempts: int = 6,
                 max_cursor_restarts: int = 2, user_agent: str = "paperforge/0.1 (data-engineering project)",
                 transport: httpx.BaseTransport | None = None, backoff_initial: float = 1.0):
        self.base_url, self.rows, self.max_attempts = base_url, rows, max_attempts
        self.max_cursor_restarts = max_cursor_restarts
        self.backoff_initial = backoff_initial
        self.limiter = RateLimiter(rate_per_sec)
        self.client = httpx.Client(timeout=timeout, headers={"User-Agent": user_agent}, transport=transport)

    def plan(self, since: date, until: date, **_) -> list[WorkUnit]:
        """One unit per calendar day of Crossref index-date (both bounds inclusive)."""
        out, d = [], since
        while d <= until:
            out.append(WorkUnit(key=f"index-date={d.isoformat()}", params={"from": d.isoformat(), "until": d.isoformat()}))
            d += timedelta(days=1)
        return out

    def plan_dois(self, dois: list[str]) -> list[WorkUnit]:
        """One unit per distinct DOI (input order kept). Accepts bare DOIs, doi: and doi.org URLs."""
        out, seen = [], set()
        for raw in dois:
            d = norm_doi(raw)
            if not d or not d.startswith("10."):
                raise ValueError(f"not a DOI: {raw!r}")
            if d not in seen:
                seen.add(d)
                out.append(WorkUnit(key=f"doi={d}", params={"doi": d}))
        return out

    def _get(self, unit: WorkUnit, params: dict, on_request: OnRequest | None, url: str | None = None) -> dict:
        attempt = 0

        def once() -> dict:
            nonlocal attempt
            attempt += 1
            self.limiter.wait()
            t0 = time.monotonic()
            status, err = None, None
            try:
                r = self.client.get(url or self.base_url, params=params)
                status = r.status_code
                if status == 404:
                    err = "not found"
                    raise NotFound(r.text[:200])
                if status == 400 and params.get("cursor", "*") != "*":
                    err = "cursor rejected"
                    raise CursorExpired(r.text[:200])
                if status == 429 or status >= 500:
                    ra = r.headers.get("Retry-After")
                    err = f"http {status}"
                    raise _Retryable(err, float(ra) if ra and ra.isdigit() else None)
                r.raise_for_status()
                return r.json()
            except httpx.TransportError as e:
                err = type(e).__name__
                raise _Retryable(err) from e
            finally:
                if on_request:
                    on_request(RequestStat(unit.key, status, (time.monotonic() - t0) * 1000, attempt, err))

        def wait(rs):
            exc = rs.outcome.exception()
            base = wait_exponential_jitter(multiplier=self.backoff_initial, max=60)(rs)
            ra = getattr(exc, "retry_after", None)
            return max(base, ra) if ra else base

        try:
            return Retrying(stop=stop_after_attempt(self.max_attempts), wait=wait,
                            retry=retry_if_exception(lambda e: isinstance(e, _Retryable)), reraise=True)(once)
        except RetryError as e:  # pragma: no cover
            raise e.last_attempt.exception()

    def _fetch_doi(self, unit: WorkUnit, on_request: OnRequest | None) -> Iterator[Page]:
        """GET /works/{doi}: exactly one source-native record, same shape as the items of the list endpoint."""
        doi = unit.params["doi"]
        item = self._get(unit, {}, on_request, url=f"{self.base_url.rstrip('/')}/{quote(doi, safe='/')}")["message"]
        if item.get("type") != "journal-article":
            raise NotJournalArticle(f"{doi}: type={item.get('type')!r}, only journal-article is ingested")
        d = norm_doi(item.get("DOI"))
        yield Page([SourceRecord(self.name, d or "", d, parse_crossref_indexed(item), item)], None)

    def fetch(self, unit: WorkUnit, cursor: str | None = None, on_request: OnRequest | None = None) -> Iterator[Page]:
        if "doi" in unit.params:
            yield from self._fetch_doi(unit, on_request)
            return
        flt = f"type:journal-article,from-index-date:{unit.params['from']},until-index-date:{unit.params['until']}"
        restarts = 0
        while True:
            try:
                yield from self._pages(unit, flt, cursor or "*", on_request)
                return
            except CursorExpired:
                # Crossref cursors are short-lived. Re-scan the window; ODS upserts are idempotent.
                restarts += 1
                if restarts > self.max_cursor_restarts:
                    raise
                cursor = None

    def _pages(self, unit, flt, cursor, on_request) -> Iterator[Page]:
        while True:
            data = self._get(unit, {"filter": flt, "rows": self.rows, "cursor": cursor}, on_request)["message"]
            items = data.get("items", [])
            nxt = data.get("next-cursor")
            if not items:
                return
            recs = []
            for it in items:
                doi = norm_doi(it.get("DOI"))
                recs.append(SourceRecord(self.name, doi or "", doi, parse_crossref_indexed(it), it))
            last = len(items) < self.rows or not nxt
            yield Page(recs, None if last else nxt)
            if last:
                return
            cursor = nxt
