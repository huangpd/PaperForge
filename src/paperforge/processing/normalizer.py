import hashlib
import json
import re
from datetime import datetime

_DOI_PREFIX = re.compile(r"^(?:https?://)?(?:dx\.)?(?:doi\.org/|doi:\s*)", re.I)


def norm_doi(doi: str | None) -> str | None:
    """Lower-case, strip URL/doi: prefixes and whitespace. None if empty."""
    if doi is None:
        return None
    d = _DOI_PREFIX.sub("", doi.strip()).strip().lower()
    return d or None


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def raw_payload_hash(payload: dict) -> str:
    """Hash of the full source-native payload (raw-level change detection).
    Stage 2 adds a separate *content* hash that ignores volatile fields."""
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def parse_crossref_indexed(payload: dict) -> datetime | None:
    s = (payload.get("indexed") or {}).get("date-time")
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------------------------------------
# Crossref -> NormalizedPaper. This is the ONLY place where standardisation rules live; batch and
# streaming paths both call it through ProcessingEngine.
# ---------------------------------------------------------------------------------------------
import calendar  # noqa: E402
import html  # noqa: E402
from datetime import UTC  # noqa: E402

from paperforge.processing.models import NormalizedPaper  # noqa: E402

NORMALIZER_VERSION = 1
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_ISSN = re.compile(r"^\d{4}-\d{3}[\dX]$")
_ORCID = re.compile(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dX])", re.I)
MIN_YEAR, MAX_YEAR_AHEAD = 1600, 2


def clean_text(s) -> str | None:
    if s is None:
        return None
    if not isinstance(s, str):
        s = str(s)
    s = _WS.sub(" ", html.unescape(_TAG.sub(" ", html.unescape(s)))).strip()
    return s or None


def _first(v):
    if isinstance(v, list):
        return next((x for x in v if x not in (None, "")), None)
    return v


def norm_issn(s: str) -> str | None:
    s = (s or "").strip().upper().replace(" ", "")
    if len(s) == 8 and "-" not in s:
        s = f"{s[:4]}-{s[4:]}"
    return s if _ISSN.match(s) else None


def _int(v) -> int | None:
    try:
        i = int(v) if v is not None and not isinstance(v, bool) else None
    except (TypeError, ValueError):
        return None
    return i if i is not None and -(2**31) <= i < 2**31 else None   # target columns are int32


def _ref_year(v) -> int | None:
    y = _int(v)
    return y if y is not None and 1000 <= y <= 2200 else None       # real data contains garbage like 199537122017


def _issued(payload: dict) -> tuple[int | None, int | None, int | None, bool]:
    for key in ("issued", "published", "published-print", "published-online"):
        parts = ((payload.get(key) or {}).get("date-parts") or [[None]])[0] or [None]
        y = _int(parts[0])
        if y is None:
            continue
        m = _int(parts[1]) if len(parts) > 1 else None
        d = _int(parts[2]) if len(parts) > 2 else None
        from datetime import datetime as _dt
        ok = MIN_YEAR <= y <= _dt.now(UTC).year + MAX_YEAR_AHEAD
        if m is not None and not 1 <= m <= 12:
            ok = False
        if ok and m is not None and d is not None and not 1 <= d <= calendar.monthrange(y, m)[1]:
            ok = False
        if not ok:
            return None, None, None, True
        return y, m, (d if m is not None else None), False
    return None, None, None, False


def normalize_crossref(payload: dict, source: str = "crossref", source_updated_at=None, fallback_ts=None) -> NormalizedPaper:
    su = parse_crossref_indexed(payload) or source_updated_at or fallback_ts
    n = NormalizedPaper(doi=norm_doi(payload.get("DOI")), source=source, source_updated_at=su)
    n.title = clean_text(_first(payload.get("title")))
    n.abstract = clean_text(payload.get("abstract"))
    n.container_title = clean_text(_first(payload.get("container-title")))
    n.publisher = clean_text(payload.get("publisher"))
    m = payload.get("member")
    n.member = str(m).strip() if m not in (None, "") else None
    n.type = clean_text(payload.get("type"))
    n.language = (clean_text(payload.get("language")) or "").lower() or None
    n.volume, n.issue, n.page = (clean_text(payload.get(k)) for k in ("volume", "issue", "page"))
    seen = set()
    for raw in payload.get("ISSN") or []:
        v = norm_issn(str(raw))
        if v is None:
            n.invalid_issns.append(str(raw))
        elif v not in seen:
            seen.add(v)
            n.issns.append(v)
    n.issued_year, n.issued_month, n.issued_day, n.issued_invalid = _issued(payload)
    n.reference_count = _int(payload.get("reference-count"))
    n.cited_by_count = _int(payload.get("is-referenced-by-count"))
    for i, a in enumerate(payload.get("author") or []):
        if not isinstance(a, dict):
            continue
        orcid = _ORCID.search(a.get("ORCID") or "")
        affs = [x for x in (clean_text(f.get("name")) for f in a.get("affiliation") or [] if isinstance(f, dict)) if x]
        n.authors.append({"ordinal": i, "given": clean_text(a.get("given")), "family": clean_text(a.get("family")),
                          "name": clean_text(a.get("name")), "orcid": orcid.group(1).upper() if orcid else None,
                          "sequence": clean_text(a.get("sequence")), "affiliations": affs})
    for i, r in enumerate(payload.get("reference") or []):
        if not isinstance(r, dict):
            continue
        n.references.append({"ordinal": i, "ref_key": clean_text(r.get("key")), "ref_doi": norm_doi(r.get("DOI")),
                             "unstructured": clean_text(r.get("unstructured")),
                             "article_title": clean_text(r.get("article-title")),
                             "journal_title": clean_text(r.get("journal-title")), "ref_year": _ref_year(r.get("year"))})
    n.content_hash = content_hash(n)
    return n


def content_dict(n: NormalizedPaper) -> dict:
    """Business content only. Excludes volatile fields (indexed/score/deposited timestamps, cited-by count)
    so that re-indexing or citation drift alone never creates a new version."""
    return {"doi": n.doi, "title": n.title, "abstract": n.abstract, "container_title": n.container_title,
            "publisher": n.publisher, "member": n.member, "type": n.type, "language": n.language,
            "volume": n.volume, "issue": n.issue, "page": n.page, "issns": n.issns,
            "issued": [n.issued_year, n.issued_month, n.issued_day], "reference_count": n.reference_count,
            "authors": n.authors, "references": n.references}


def content_hash(n: NormalizedPaper) -> str:
    return hashlib.sha256(canonical_json(content_dict(n)).encode()).hexdigest()
