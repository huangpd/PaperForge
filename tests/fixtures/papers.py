"""Synthetic Crossref-shaped payloads. DOIs use the reserved-looking 10.99999 prefix so they can never collide with real data."""
import copy
from datetime import UTC, datetime


def ts(day: int, month: int = 1, year: int = 2026) -> datetime:
    return datetime(year, month, day, tzinfo=UTC)


def payload(n=1, indexed="2026-01-01T00:00:00Z", title=None, **over):
    p = {
        "DOI": f"10.99999/Test.{n}", "type": "journal-article", "title": [title or f"Paper {n}"],
        "container-title": ["Journal of Tests"], "publisher": "Test Press", "member": "42",
        "ISSN": ["1234-5679"], "issued": {"date-parts": [[2020, 5, 17]]},
        "indexed": {"date-time": indexed, "timestamp": 1}, "score": 1.0,
        "reference-count": 1, "is-referenced-by-count": 3,
        "author": [{"given": "Ada", "family": "Lovelace", "sequence": "first", "affiliation": [{"name": "Analytical Engine Co"}],
                    "ORCID": "https://orcid.org/0000-0002-1825-0097"}],
        "reference": [{"key": "r1", "DOI": "10.1/REF", "unstructured": "Some ref"}],
        "abstract": "<jats:p>An &amp; abstract</jats:p>",
    }
    p.update(over)
    return copy.deepcopy(p)
