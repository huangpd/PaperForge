"""Rule-based validation. Severity: error -> quarantine, warning -> keep with flag, metric -> statistics only.
(Stage 3 adds Pandera schemas, per-rule execution records and daily reports on top of these rules.)"""
import re

from paperforge.processing.models import Issue, NormalizedPaper

DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$")

RULES = {  # rule_id -> (severity, version)
    "doi_missing": ("error", 1),
    "doi_format": ("error", 1),
    "count_negative": ("error", 1),
    "title_missing": ("warning", 1),
    "issued_invalid": ("warning", 1),
    "issn_invalid": ("warning", 1),
    "source_updated_at_missing": ("warning", 1),
    "arxiv_id_invalid": ("error", 1),
    "arxiv_category_missing": ("error", 1),
    "arxiv_doi_invalid": ("warning", 1),
}


def _issue(rule_id: str, msg: str) -> Issue:
    sev, ver = RULES[rule_id]
    return Issue(rule_id, sev, msg, ver)


def validate(n: NormalizedPaper) -> list[Issue]:
    out: list[Issue] = []
    if n.arxiv is not None:      # arXiv: the entity key is the arXiv id; a DOI is optional (and already vetted by the normalizer)
        from paperforge.processing.arxiv import ARXIV_ID_RE
        if not ARXIV_ID_RE.match(n.arxiv["arxiv_id"]):
            out.append(_issue("arxiv_id_invalid", f"not an arXiv identifier: {n.arxiv['arxiv_id']!r}"))
        if not n.arxiv["primary_category"]:
            out.append(_issue("arxiv_category_missing", "record has no category"))
        if n.arxiv["doi_invalid"]:
            out.append(_issue("arxiv_doi_invalid", "DOI field has no well-formed DOI; ignored"))
    elif not n.doi:
        out.append(_issue("doi_missing", "DOI is empty"))
    elif not DOI_RE.match(n.doi):
        out.append(_issue("doi_format", f"DOI does not match 10.<registrant>/<suffix>: {n.doi!r}"))
    if (n.reference_count is not None and n.reference_count < 0) or (n.cited_by_count is not None and n.cited_by_count < 0):
        out.append(_issue("count_negative", f"negative count: reference={n.reference_count} cited_by={n.cited_by_count}"))
    if not n.title:
        out.append(_issue("title_missing", "title is empty"))
    if n.issued_invalid:
        out.append(_issue("issued_invalid", "publication date out of range or invalid; set to NULL"))
    if n.invalid_issns:
        out.append(_issue("issn_invalid", f"dropped malformed ISSN: {n.invalid_issns}"))
    if n.source_updated_at is None:
        out.append(_issue("source_updated_at_missing", "no usable update time"))
    return out


def errors(issues: list[Issue]) -> list[Issue]:
    return [i for i in issues if i.severity == "error"]
