"""arXiv metadata snapshot record -> NormalizedPaper (+ the arXiv-only fields kept in dwd.paper_arxiv).

Differences from Crossref that matter here:
  * titles/abstracts contain LaTeX (`$a<b$`), so they are whitespace-collapsed only; clean_text() would strip `<...>` as HTML;
  * `doi` may hold several DOIs separated by spaces, or garbage; the first valid one is used;
  * no publisher/journal/ISSN/references; `journal-ref` is free text and is kept verbatim in dwd.paper_arxiv;
  * `authors_parsed` ([family, given, suffix]) is used instead of the display string `authors`.
"""
import hashlib
import re
from datetime import UTC, date, datetime

from paperforge.processing.models import NormalizedPaper
from paperforge.processing.normalizer import MIN_YEAR, canonical_json, norm_doi

ARXIV_ID_RE = re.compile(r"^(\d{4}\.\d{4,5}|[a-z][a-z\-]*(\.[A-Z]{2})?/\d{7})$")
_DOI_OK = re.compile(r"^10\.\d{4,9}/\S+$")
_WS = re.compile(r"\s+")


def _ws(s) -> str | None:
    if s is None:
        return None
    s = _WS.sub(" ", str(s)).strip()
    return s or None


def first_valid_doi(raw) -> tuple[str | None, bool]:
    """(doi, had_value_but_invalid). Splits on whitespace/commas/semicolons; the first well-formed DOI wins."""
    if not raw or not str(raw).strip():
        return None, False
    for tok in re.split(r"[\s,;]+", str(raw).strip()):
        d = norm_doi(tok)
        if d and _DOI_OK.match(d):
            return d, False
    return None, True


def _parse_created(s: str | None) -> datetime | None:
    """'Mon, 2 Apr 2007 19:18:42 GMT' -> aware UTC datetime."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=UTC)
    except ValueError:
        return None


def parse_update_date(payload: dict) -> datetime | None:
    s = payload.get("update_date")
    try:
        return datetime.fromisoformat(s).replace(tzinfo=UTC) if s else None
    except ValueError:
        return None


def category_match(categories: list[str], patterns: list[str], match: str = "primary") -> bool:
    """patterns: exact ('stat.ML') or prefix with '*' ('cs.*'). match: 'primary' = first category only, 'any' = any category."""
    def hit(c: str) -> bool:
        return any(c == p or (p.endswith("*") and c.startswith(p[:-1])) for p in patterns)
    if not categories:
        return False
    return hit(categories[0]) if match == "primary" else any(hit(c) for c in categories)


def normalize_arxiv(payload: dict, source: str = "arxiv", source_updated_at=None, fallback_ts=None) -> NormalizedPaper:
    aid = _ws(payload.get("id")) or ""
    su = parse_update_date(payload) or source_updated_at or fallback_ts
    doi, doi_bad = first_valid_doi(payload.get("doi"))
    n = NormalizedPaper(doi=doi, source=source, source_updated_at=su)
    n.paper_key = doi or (f"arxiv:{aid}" if aid else None)
    n.title = _ws(payload.get("title"))
    n.abstract = _ws(payload.get("abstract"))
    n.type = "preprint"
    versions = payload.get("versions") or []
    submitted = _parse_created(versions[0].get("created")) if versions and isinstance(versions[0], dict) else None
    if submitted and submitted.year >= MIN_YEAR:
        n.issued_year, n.issued_month, n.issued_day = submitted.year, submitted.month, submitted.day
    for i, a in enumerate(payload.get("authors_parsed") or []):
        if not isinstance(a, list) or not a:
            continue
        family, given, suffix = (list(a) + ["", "", ""])[:3]
        family, given = _ws(family), _ws(" ".join(x for x in (given, suffix) if x))
        if family or given:
            n.authors.append({"ordinal": len(n.authors), "given": given, "family": family, "name": None, "orcid": None,
                              "sequence": "first" if i == 0 else "additional", "affiliations": []})
    cats = (payload.get("categories") or "").split()
    n.identifiers = [("arxiv", aid)] + ([("doi", doi)] if doi else [])
    n.arxiv = {"arxiv_id": aid, "doi": doi, "doi_invalid": doi_bad, "primary_category": cats[0] if cats else None,
               "categories": cats, "title": n.title, "abstract": n.abstract, "journal_ref": _ws(payload.get("journal-ref")),
               "comments": _ws(payload.get("comments")), "report_no": _ws(payload.get("report-no")),
               "license": _ws(payload.get("license")), "submitted_at": submitted, "updated_date": su.date() if su else None,
               "version_count": max(1, len(versions))}
    from paperforge.processing.normalizer import content_hash
    n.content_hash = content_hash(n)
    n.arxiv["content_hash"] = arxiv_hash(n.arxiv)
    return n


def arxiv_hash(a: dict) -> str:
    keep = {k: (v.isoformat() if isinstance(v, (date, datetime)) else v) for k, v in a.items() if k not in ("content_hash", "doi_invalid")}
    return hashlib.sha256(canonical_json(keep).encode()).hexdigest()
