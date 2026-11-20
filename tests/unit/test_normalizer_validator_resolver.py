from datetime import UTC, datetime

import pytest

from paperforge.processing.entity_resolver import ExistingPaper, Incoming, resolve, uuid7
from paperforge.processing.normalizer import normalize_crossref, norm_issn
from paperforge.processing.validator import errors, validate
from tests.fixtures.papers import payload, ts


def test_normalize_basic_fields():
    n = normalize_crossref(payload(1, title="A &amp; B <i>x</i>"))
    assert n.doi == "10.99999/test.1" and n.title == "A & B x" and n.abstract == "An & abstract"
    assert (n.issued_year, n.issued_month, n.issued_day) == (2020, 5, 17)
    assert n.authors[0]["orcid"] == "0000-0002-1825-0097" and n.references[0]["ref_doi"] == "10.1/ref"
    assert n.source_updated_at == datetime(2026, 1, 1, tzinfo=UTC)


def test_content_hash_ignores_volatile_fields_but_sees_content():
    base = normalize_crossref(payload(1)).content_hash
    assert normalize_crossref(payload(1, indexed="2030-01-01T00:00:00Z", score=99.0)).content_hash == base
    p = payload(1); p["is-referenced-by-count"] = 999
    assert normalize_crossref(p).content_hash == base
    assert normalize_crossref(payload(1, title="Changed")).content_hash != base
    p = payload(1); p["author"][0]["family"] = "Babbage"
    assert normalize_crossref(p).content_hash != base


def test_added_and_missing_fields_do_not_break():  # test #7
    p = payload(1, brand_new_future_field={"x": 1})
    for k in ("author", "reference", "abstract", "ISSN", "member", "language"):
        p.pop(k, None)
    n = normalize_crossref(p)
    assert n.authors == [] and n.references == [] and n.abstract is None and n.issns == []
    assert not errors(validate(n))                     # optional fields never block a paper
    n2 = normalize_crossref({"DOI": "10.99999/x", "title": None})   # nulls / minimal payload
    assert n2.title is None and [i.rule_id for i in validate(n2)] == ["title_missing", "source_updated_at_missing"]


@pytest.mark.parametrize("doi,rule", [(None, "doi_missing"), ("not-a-doi", "doi_format"), ("10.1/ has space", "doi_format")])
def test_error_rules(doi, rule):
    n = normalize_crossref(payload(1, DOI=doi) if doi else {k: v for k, v in payload(1).items() if k != "DOI"})
    assert [i.rule_id for i in errors(validate(n))] == [rule]


def test_warning_rules_keep_record():
    n = normalize_crossref(payload(1, ISSN=["12345679", "bad", "1234-5679"], issued={"date-parts": [[2020, 13, 40]]}))
    ids = {i.rule_id: i.severity for i in validate(n)}
    assert ids == {"issued_invalid": "warning", "issn_invalid": "warning"}
    assert n.issns == ["1234-5679"] and n.issued_year is None   # invalid date nulled, valid ISSN kept & de-duplicated


def test_negative_count_is_error():
    assert [i.rule_id for i in errors(validate(normalize_crossref(payload(1, **{"reference-count": -1}))))] == ["count_negative"]


@pytest.mark.parametrize("raw,exp", [("1234-5679", "1234-5679"), ("1234567x", "1234-567X"), ("12345", None), ("abcd-efgh", None)])
def test_issn(raw, exp):
    assert norm_issn(raw) == exp


def inc(i, day, h, doi="10.1/a"):
    return Incoming(i, doi, ts(day), h)


def ex(versions, current):
    su, vno, h = current
    return ExistingPaper(uuid7(), vno, h, su, versions)


def test_resolver_new_then_same_hash_and_refresh():
    (d,) = resolve([inc(0, 1, "A"), inc(1, 1, "A"), inc(2, 5, "A")], {})
    assert d.is_new and [a.version_no for a in d.adds] == [1]
    assert d.outcomes == {0: "inserted", 1: "unchanged", 2: "refreshed"} and d.refresh_rec_idx == 2


def test_resolver_late_and_revert():
    e = ex([(ts(1), 1, "A"), (ts(5), 2, "B")], (ts(5), 2, "B"))
    (d,) = resolve([inc(0, 3, "C")], {"10.1/a": e})          # late: between v1 and v2
    assert d.outcomes == {0: "late"} and d.current_rec_idx is None and d.adds[0].version_no == 3
    (d,) = resolve([inc(0, 9, "A")], {"10.1/a": e})          # content reverts to an old one: a new version, becomes current
    assert d.outcomes == {0: "updated"} and d.current_version_no == 3
    (d,) = resolve([inc(0, 3, "A")], {"10.1/a": e})          # late but same as what was valid then -> no-op
    assert d.outcomes == {0: "unchanged"} and not d.adds


def test_uuid7_is_time_ordered_and_unique():
    ids = [uuid7() for _ in range(200)]
    assert len(set(ids)) == 200 and all(i.version == 7 for i in ids)


def test_garbage_numbers_are_nulled_not_crashing():
    p = payload(1)
    p["reference"] = [{"key": "a", "year": "199537122017"}, {"key": "b", "year": "2019"}, {"key": "c", "year": "12"}]
    p["reference-count"] = 10**12
    n = normalize_crossref(p)
    assert [r["ref_year"] for r in n.references] == [None, 2019, None] and n.reference_count is None
