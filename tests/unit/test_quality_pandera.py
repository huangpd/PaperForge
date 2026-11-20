import polars as pl

from paperforge.quality.runner import PAPER_COLS, run_pandera


def frame(rows):
    return pl.DataFrame(rows, schema=PAPER_COLS, orient="row")


def test_pandera_rules_count_only_offending_rows():
    from datetime import UTC, datetime
    t = datetime(2026, 1, 1, tzinfo=UTC)
    df = frame([
        ("10.1234/ok", "10.1234/ok", 2020, ["1234-5679"], 3, 4, t),
        ("bad-doi", "bad-doi", 2020, [], 0, 0, t),             # doi_format
        ("10.1234/ok2", "10.1234/ok", 1200, ["1234-5679", "x"], -1, -2, t),  # duplicate doi, year out of range, bad issn, negatives
        (None, None, None, [], None, None, None),    # null key + null source time; a null DOI alone is allowed (arXiv papers)
        ("arxiv:2101.00001", None, 2021, [], None, None, t),   # arXiv paper without DOI: fully valid
    ])
    r = run_pandera(df)
    failed = {k: v[1] for k, v in r.items()}
    assert failed["v_doi_format"] == 1
    assert failed["v_paper_key_not_null"] == 1
    assert failed["u_doi_unique"] == 2                   # both copies are reported
    assert failed["u_paper_key_unique"] == 0
    assert failed["v_issued_year_range"] == 1
    assert failed["v_issn_format"] == 1
    assert failed["v_reference_count_non_negative"] == 1
    assert failed["v_cited_by_count_non_negative"] == 1
    assert failed["d_source_updated_at_present"] == 1
    assert all(v[0] == 5 for v in r.values())


def test_clean_frame_has_no_failures():
    from datetime import UTC, datetime
    t = datetime(2026, 1, 1, tzinfo=UTC)
    r = run_pandera(frame([(f"10.1234/{i}", f"10.1234/{i}", 2000 + i, ["1234-5679"], i, i, t) for i in range(5)]))
    assert sum(v[1] for v in r.values()) == 0


def test_quality_validator_interface_is_implemented_by_pandera_validator():
    from paperforge.quality.base import QualityValidator
    from paperforge.quality.runner import PanderaQualityValidator
    assert issubclass(PanderaQualityValidator, QualityValidator)
    assert QualityValidator.blocking([{"severity": "error", "passed": False}]) and not QualityValidator.blocking([{"severity": "warning", "passed": False}])
