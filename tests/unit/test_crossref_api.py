import httpx
import pytest

from paperforge.connectors.base import RequestStat
from paperforge.connectors.crossref_api import CrossrefApiConnector


def item(i, ts="2026-01-01T00:00:00Z"):
    return {"DOI": f"10.1/{i}", "indexed": {"date-time": ts}, "title": [f"t{i}"]}


def make(handler, **kw):
    return CrossrefApiConnector("http://x/works", rate_per_sec=0, rows=2, backoff_initial=0.001,
                                transport=httpx.MockTransport(handler), **kw)


def collect(conn, unit, stats=None):
    return list(conn.fetch(unit, None, (stats.append if stats is not None else None)))


def test_plan_daily_windows_inclusive():
    from datetime import date
    units = make(lambda r: None).plan(since=date(2026, 1, 30), until=date(2026, 2, 1))
    assert [u.params["from"] for u in units] == ["2026-01-30", "2026-01-31", "2026-02-01"]
    assert all(u.params["from"] == u.params["until"] for u in units)


def test_cursor_pagination_and_filter():
    calls = []

    def h(req):
        calls.append(dict(req.url.params))
        cur = req.url.params["cursor"]
        pages = {"*": ([item(1), item(2)], "c1"), "c1": ([item(3)], "c2")}
        items, nxt = pages[cur]
        return httpx.Response(200, json={"message": {"items": items, "next-cursor": nxt}})

    conn = make(h)
    pages = collect(conn, make(h).plan(since=__import__("datetime").date(2026, 1, 5), until=__import__("datetime").date(2026, 1, 5))[0])
    assert [len(p.records) for p in pages] == [2, 1]
    assert pages[0].next_cursor == "c1" and pages[-1].next_cursor is None
    assert "from-index-date:2026-01-05" in calls[0]["filter"] and "until-index-date:2026-01-05" in calls[0]["filter"]
    assert "type:journal-article" in calls[0]["filter"]


def test_retry_on_5xx_and_429_records_stats():
    seq = iter([500, 429, 200])

    def h(req):
        code = next(seq)
        return httpx.Response(code, json={"message": {"items": [item(1)], "next-cursor": None}} if code == 200 else {})

    stats: list[RequestStat] = []
    from paperforge.connectors.base import WorkUnit
    pages = collect(make(h), WorkUnit("u", {"from": "2026-01-01", "until": "2026-01-01"}), stats)
    assert len(pages) == 1
    assert [s.status_code for s in stats] == [500, 429, 200] and [s.attempt for s in stats] == [1, 2, 3]


def test_gives_up_after_max_attempts():
    from paperforge.connectors.base import WorkUnit
    conn = make(lambda r: httpx.Response(503), max_attempts=3)
    with pytest.raises(Exception):
        collect(conn, WorkUnit("u", {"from": "2026-01-01", "until": "2026-01-01"}))


def test_cursor_expiry_restarts_window():
    from paperforge.connectors.base import WorkUnit
    state = {"expired": False}

    def h(req):
        cur = req.url.params["cursor"]
        if cur == "stale":
            return httpx.Response(400, text="invalid cursor")
        if cur == "*":
            return httpx.Response(200, json={"message": {"items": [item(1), item(2)], "next-cursor": "fresh"}})
        return httpx.Response(200, json={"message": {"items": [], "next-cursor": "fresh"}})

    u = WorkUnit("u", {"from": "2026-01-01", "until": "2026-01-01"})
    pages = list(make(h).fetch(u, cursor="stale"))  # resume with an expired stored cursor
    assert sum(len(p.records) for p in pages) == 2


# ---- targeted fetch by DOI ------------------------------------------------------------------
def test_plan_dois_normalizes_and_dedupes():
    units = make(lambda r: None).plan_dois(["10.1016/J.X.1", "https://doi.org/10.1016/j.x.1", " doi:10.1/Y "])
    assert [u.params["doi"] for u in units] == ["10.1016/j.x.1", "10.1/y"]
    with pytest.raises(ValueError):
        make(lambda r: None).plan_dois(["not-a-doi"])


def test_fetch_doi_returns_single_native_record():
    seen = []

    def h(req):
        seen.append(str(req.url))
        return httpx.Response(200, json={"message": {**item("a"), "type": "journal-article"}})
    conn = make(h)
    pages = collect(conn, conn.plan_dois(["10.1/a"])[0])
    assert seen == ["http://x/works/10.1/a"]
    assert len(pages) == 1 and pages[0].next_cursor is None
    rec = pages[0].records[0]
    assert rec.doi == "10.1/a" and rec.payload["title"] == ["ta"] and rec.source_updated_at is not None


def test_fetch_doi_404_and_wrong_type_are_errors_not_retried():
    from paperforge.connectors.crossref_api import NotFound, NotJournalArticle
    calls = []

    def h(req):
        calls.append(1)
        if req.url.path.endswith("/missing"):
            return httpx.Response(404, text="Resource not found.")
        return httpx.Response(200, json={"message": {"DOI": "10.1/book", "type": "book-chapter"}})
    conn = make(h)
    with pytest.raises(NotFound):
        collect(conn, conn.plan_dois(["10.1/missing"])[0])
    assert len(calls) == 1                      # a 404 is final, not retried
    with pytest.raises(NotJournalArticle):
        collect(conn, conn.plan_dois(["10.1/book"])[0])
