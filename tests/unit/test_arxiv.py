import json
import zipfile
from datetime import UTC, datetime

import pytest

from paperforge.connectors.arxiv_snapshot import ArxivSnapshotConnector
from paperforge.processing.arxiv import category_match, first_valid_doi, normalize_arxiv
from paperforge.processing.validator import errors, validate


def rec(i="2101.00001", **over):
    r = {"id": i, "submitter": "x", "authors": "A. B", "title": "A study of  $a<b$ and\n  $c>d$", "comments": "5 pages",
         "journal-ref": None, "doi": None, "report-no": None, "categories": "cs.LG stat.ML", "license": None,
         "abstract": "  First line\nsecond line with <b>not html</b>.\n", "update_date": "2021-02-03",
         "versions": [{"version": "v1", "created": "Mon, 4 Jan 2021 19:18:42 GMT"}, {"version": "v2", "created": "Tue, 2 Feb 2021 10:00:00 GMT"}],
         "authors_parsed": [["Balázs", "C.", ""], ["Collab", "", ""], ["Smith", "J.", "Jr"]]}
    r.update(over)
    return r


def test_normalize_keeps_latex_and_collapses_whitespace():
    n = normalize_arxiv(rec())
    assert n.title == "A study of $a<b$ and $c>d$"                   # clean_text() would have eaten "<b$ and $c>"
    assert n.abstract == "First line second line with <b>not html</b>."
    assert n.type == "preprint" and n.doi is None and n.paper_key == "arxiv:2101.00001" and n.key == "arxiv:2101.00001"
    assert (n.issued_year, n.issued_month, n.issued_day) == (2021, 1, 4)        # first submission, not the update date
    assert n.source_updated_at == datetime(2021, 2, 3, tzinfo=UTC)
    assert [(a["family"], a["given"]) for a in n.authors] == [("Balázs", "C."), ("Collab", None), ("Smith", "J. Jr")]
    assert n.identifiers == [("arxiv", "2101.00001")]
    a = n.arxiv
    assert a["primary_category"] == "cs.LG" and a["categories"] == ["cs.LG", "stat.ML"] and a["version_count"] == 2


def test_doi_becomes_the_paper_key_and_multiple_dois_take_the_first_valid():
    n = normalize_arxiv(rec(doi="garbage 10.1103/PhysRevD.76.013009 10.1103/Other"))
    assert n.doi == "10.1103/physrevd.76.013009" and n.key == n.doi
    assert n.identifiers == [("arxiv", "2101.00001"), ("doi", n.doi)]
    assert first_valid_doi("nonsense") == (None, True) and first_valid_doi(None) == (None, False)


def test_validator_rules_for_arxiv():
    ok = validate(normalize_arxiv(rec()))
    assert errors(ok) == []                                           # no DOI is fine for arXiv
    assert [i.rule_id for i in validate(normalize_arxiv(rec(doi="junk")))] == ["arxiv_doi_invalid"]
    bad = errors(validate(normalize_arxiv(rec(i="not an id"))))
    assert [i.rule_id for i in bad] == ["arxiv_id_invalid"]
    assert [i.rule_id for i in errors(validate(normalize_arxiv(rec(categories=""))))] == ["arxiv_category_missing"]
    for good in ("2101.00001", "1501.1234", "hep-th/9901001", "cs.AI/0101001"):
        assert not errors(validate(normalize_arxiv(rec(i=good))))


def test_content_hash_ignores_arxiv_only_fields_but_arxiv_hash_sees_them():
    a, b = normalize_arxiv(rec()), normalize_arxiv(rec(categories="cs.LG stat.ML cs.AI", comments="6 pages"))
    assert a.content_hash == b.content_hash                           # no new paper version for a category change
    assert a.arxiv["content_hash"] != b.arxiv["content_hash"]         # but dwd.paper_arxiv is updated


@pytest.mark.parametrize("cats,pat,match,want", [
    (["cs.LG", "stat.ML"], ["cs.*"], "primary", True), (["math.ST", "cs.LG"], ["cs.*"], "primary", False),
    (["math.ST", "cs.LG"], ["cs.*"], "any", True), (["stat.ML"], ["stat.ML"], "primary", True),
    (["stat.TH"], ["stat.ML"], "any", False), ([], ["cs.*"], "any", False)])
def test_category_match(cats, pat, match, want):
    assert category_match(cats, pat, match) is want


def _write(tmp_path, lines, zipped):
    body = "\n".join(json.dumps(x) for x in lines) + "\n"
    if zipped:
        p = tmp_path / "s.zip"
        with zipfile.ZipFile(p, "w") as z:
            z.writestr("arxiv-metadata-oai-snapshot.json", body)
        return str(p)
    p = tmp_path / "s.json"
    p.write_text(body)
    return str(p)


@pytest.mark.parametrize("zipped", [True, False])
def test_connector_filters_pages_and_resumes_by_input_line(tmp_path, zipped):
    lines = [rec(f"2101.{i:05d}", categories=("cs.LG" if i % 2 == 0 else "math.ST")) for i in range(1, 11)]   # 5 match
    c = ArxivSnapshotConnector(_write(tmp_path, lines, zipped), ["cs.*"], page_size=2)
    unit = c.plan()[0]
    pages = list(c.fetch(unit))
    assert [r.source_id for p in pages for r in p.records] == [f"2101.{i:05d}" for i in (2, 4, 6, 8, 10)]
    assert [p.next_cursor for p in pages] == ["4", "8", None]          # cursor = input lines consumed
    resumed = list(c.fetch(unit, cursor="4"))
    assert [r.source_id for p in resumed for r in p.records] == ["2101.00006", "2101.00008", "2101.00010"]
    r0 = pages[0].records[0]
    assert r0.source == "arxiv" and r0.source_updated_at == datetime(2021, 2, 3, tzinfo=UTC) and r0.doi is None


def test_zip_with_several_members_is_rejected(tmp_path):
    p = tmp_path / "s.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("a.json", "{}\n"); z.writestr("b.json", "{}\n")
    with pytest.raises(ValueError):
        list(ArxivSnapshotConnector(str(p)).fetch(ArxivSnapshotConnector(str(p)).plan()[0]))


def test_bad_match_mode_rejected():
    with pytest.raises(ValueError):
        ArxivSnapshotConnector("x.json", match="all")
