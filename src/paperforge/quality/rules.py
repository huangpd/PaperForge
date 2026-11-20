"""Quality rule catalog. Every rule has: id, version, dimension, severity (error|warning|metric), dataset, engine.
error   -> must be 0 failed rows (run exits non-zero); at ingest time such rows never reach dwd.* (quarantined).
warning -> allowed into dwd.*, kept visible.
metric  -> coverage statistic only (no pass/fail)."""
from dataclasses import dataclass

import polars as pl

from paperforge.processing.validator import RULES as VALIDATOR_RULES

DOI_RE = r"^10\.\d{4,9}/\S+$"
ISSN_RE = r"^\d{4}-\d{3}[\dX]$"


@dataclass(frozen=True)
class Rule:
    rule_id: str
    dimension: str
    severity: str
    dataset: str
    description: str
    engine: str
    version: int = 1
    # pandera rules: expression that is True for rows that PASS; sql rules: SQL returning (total, failed)
    column: str | None = None
    expr: pl.Expr | None = None
    sql: str | None = None


def _issn_ok() -> pl.Expr:
    return pl.col("issns").list.eval(pl.element().str.contains(ISSN_RE)).list.all()


PANDERA_RULES = [
    Rule("v_paper_key_not_null", "completeness", "error", "dwd.paper", "paper key (DOI, or arxiv:<id>) is present", "pandera",
         column="paper_key", expr=pl.col("paper_key").is_not_null()),
    Rule("v_doi_format", "validity", "error", "dwd.paper", "DOI (when present) matches 10.<registrant>/<suffix>", "pandera", column="primary_doi",
         expr=pl.col("primary_doi").is_null() | pl.col("primary_doi").str.contains(DOI_RE)),
    Rule("u_doi_unique", "uniqueness", "error", "dwd.paper", "DOI is unique across papers (NULL allowed)", "pandera", column="primary_doi",
         expr=pl.col("primary_doi").is_null() | ~pl.col("primary_doi").is_duplicated()),
    Rule("u_paper_key_unique", "uniqueness", "error", "dwd.paper", "paper key is unique", "pandera", column="paper_key",
         expr=~pl.col("paper_key").is_duplicated()),
    Rule("v_issued_year_range", "validity", "warning", "dwd.paper", "publication year within 1600..2100 (NULL allowed)", "pandera",
         column="issued_year", expr=pl.col("issued_year").is_null() | pl.col("issued_year").is_between(1600, 2100)),
    Rule("v_issn_format", "validity", "warning", "dwd.paper", "every ISSN is NNNN-NNN[0-9X]", "pandera", column="issns", expr=_issn_ok()),
    Rule("v_reference_count_non_negative", "validity", "error", "dwd.paper", "reference_count >= 0", "pandera", column="reference_count",
         expr=pl.col("reference_count").is_null() | (pl.col("reference_count") >= 0)),
    Rule("v_cited_by_count_non_negative", "validity", "error", "dwd.paper", "cited_by_count >= 0", "pandera", column="cited_by_count",
         expr=pl.col("cited_by_count").is_null() | (pl.col("cited_by_count") >= 0)),
    Rule("d_source_updated_at_present", "source_completeness", "error", "dwd.paper", "source update time present", "pandera",
         column="source_updated_at", expr=pl.col("source_updated_at").is_not_null()),
]

# metric rules: SQL gives the *missing* count
def _metric(rid, col_expr, desc):
    return Rule(rid, "completeness", "metric", "dwd.paper", desc, "sql",
                sql=f"SELECT count(*), count(*) FILTER (WHERE {col_expr}) FROM dwd.paper")


def _check(rid, dim, sev, ds, desc, failed_sql, total_sql="SELECT count(*) FROM dwd.paper"):
    return Rule(rid, dim, sev, ds, desc, "sql", sql=f"SELECT ({total_sql}), ({failed_sql})")


SQL_RULES = [
    _metric("c_title", "title IS NULL", "title completeness"),
    _metric("c_abstract", "abstract IS NULL", "abstract completeness"),
    _metric("c_authors", "author_count = 0", "author coverage"),
    _metric("c_references", "coalesce(reference_count,0) = 0", "reference coverage"),
    _metric("c_issn", "cardinality(issns) = 0", "ISSN coverage"),
    _metric("c_issued_date", "issued_date IS NULL", "publication date coverage"),
    _metric("c_member", "member IS NULL", "publisher (member id) coverage"),
    _metric("c_container_title", "container_title IS NULL", "journal title coverage"),
    _check("u_identifier_active_unique", "uniqueness", "error", "dwd.paper_identifier", "one active (scheme,value,source) per identifier",
           "SELECT count(*) FROM (SELECT 1 FROM dwd.paper_identifier WHERE valid_to IS NULL GROUP BY scheme, value, source HAVING count(*) > 1) x",
           "SELECT count(*) FROM dwd.paper_identifier"),
    _check("s_author_count_matches", "consistency", "error", "dwd.paper", "author_count equals paper_author rows",
           "SELECT count(*) FROM dwd.paper p WHERE p.author_count <> (SELECT count(*) FROM dwd.paper_author a WHERE a.paper_id = p.paper_id)"),
    _check("s_current_version_consistent", "consistency", "error", "dwd.paper", "current_version row exists with same content hash",
           "SELECT count(*) FROM dwd.paper p WHERE NOT EXISTS (SELECT 1 FROM dwd.paper_version v WHERE v.paper_id=p.paper_id "
           "AND v.version_no=p.current_version AND v.content_hash=p.content_hash)"),
    _check("s_single_open_version", "consistency", "error", "dwd.paper_version", "exactly one open-ended version per paper",
           "SELECT count(*) FROM (SELECT p.paper_id FROM dwd.paper p LEFT JOIN dwd.paper_version v ON v.paper_id=p.paper_id AND v.valid_to IS NULL "
           "GROUP BY p.paper_id HAVING count(v.paper_id) <> 1) x"),
    _check("s_ods_dwd_coverage", "consistency", "error", "ods.crossref_works", "every ODS DOI is a paper or an open quarantine record",
           "SELECT count(*) FROM ods.crossref_works w WHERE NOT EXISTS (SELECT 1 FROM dwd.paper p WHERE p.primary_doi = w.doi_norm) "
           "AND NOT EXISTS (SELECT 1 FROM meta.quarantine_record q WHERE q.status='open' AND q.source_id = w.doi_norm)",
           "SELECT count(*) FROM ods.crossref_works"),
    _check("r_author_has_paper", "referential_integrity", "error", "dwd.paper_author", "paper_author -> paper",
           "SELECT count(*) FROM dwd.paper_author a WHERE NOT EXISTS (SELECT 1 FROM dwd.paper p WHERE p.paper_id=a.paper_id)",
           "SELECT count(*) FROM dwd.paper_author"),
    _check("r_reference_has_paper", "referential_integrity", "error", "dwd.paper_reference", "paper_reference -> paper",
           "SELECT count(*) FROM dwd.paper_reference r WHERE NOT EXISTS (SELECT 1 FROM dwd.paper p WHERE p.paper_id=r.paper_id)",
           "SELECT count(*) FROM dwd.paper_reference"),
    _check("r_version_has_paper", "referential_integrity", "error", "dwd.paper_version", "paper_version -> paper",
           "SELECT count(*) FROM dwd.paper_version v WHERE NOT EXISTS (SELECT 1 FROM dwd.paper p WHERE p.paper_id=v.paper_id)",
           "SELECT count(*) FROM dwd.paper_version"),
    _check("r_paper_has_doi_identifier", "referential_integrity", "error", "dwd.paper", "a paper with a DOI has an active DOI identifier equal to primary_doi",
           "SELECT count(*) FROM dwd.paper p WHERE p.primary_doi IS NOT NULL AND NOT EXISTS (SELECT 1 FROM dwd.paper_identifier i "
           "WHERE i.paper_id=p.paper_id AND i.scheme='doi' AND i.value=p.primary_doi AND i.valid_to IS NULL)"),
    _check("r_paper_has_identifier", "referential_integrity", "error", "dwd.paper", "every paper has at least one active identifier",
           "SELECT count(*) FROM dwd.paper p WHERE NOT EXISTS (SELECT 1 FROM dwd.paper_identifier i WHERE i.paper_id=p.paper_id AND i.valid_to IS NULL)"),
    _check("r_arxiv_has_identifier", "referential_integrity", "error", "dwd.paper_arxiv", "every arXiv row has an active arxiv identifier on its paper",
           "SELECT count(*) FROM dwd.paper_arxiv a WHERE NOT EXISTS (SELECT 1 FROM dwd.paper_identifier i WHERE i.paper_id=a.paper_id "
           "AND i.scheme='arxiv' AND i.value=a.arxiv_id AND i.valid_to IS NULL)", "SELECT count(*) FROM dwd.paper_arxiv"),
    _check("s_ods_arxiv_dwd_coverage", "consistency", "error", "ods.arxiv_works", "every ODS arXiv record is in dwd.paper_arxiv or an open quarantine record",
           "SELECT count(*) FROM ods.arxiv_works w WHERE NOT EXISTS (SELECT 1 FROM dwd.paper_arxiv a WHERE a.arxiv_id = w.arxiv_id) "
           "AND NOT EXISTS (SELECT 1 FROM meta.quarantine_record q WHERE q.status='open' AND q.source_id = w.arxiv_id)",
           "SELECT count(*) FROM ods.arxiv_works"),
    _check("r_version_raw_lineage", "referential_integrity", "error", "dwd.paper_version", "every version traces to an existing raw file",
           "SELECT count(*) FROM dwd.paper_version v WHERE v.raw_file_id IS NULL OR NOT EXISTS "
           "(SELECT 1 FROM ods.raw_file_manifest m WHERE m.file_id=v.raw_file_id)", "SELECT count(*) FROM dwd.paper_version"),
    _check("d_ods_source_complete", "source_completeness", "error", "ods.crossref_works", "ODS rows carry source, batch and raw-file lineage",
           "SELECT count(*) FROM ods.crossref_works WHERE source IS NULL OR source = '' OR batch_id IS NULL OR raw_file_id IS NULL",
           "SELECT count(*) FROM ods.crossref_works"),
]

TIMELINESS = Rule("t_hours_since_last_ingest", "timeliness", "metric", "ods.crossref_works",
                  "hours since the newest ODS ingestion (details.hours)", "sql")

# Ingest-time validator rules (python): results are derived from quarantine_record / quality_flags
_DIM = {"doi_missing": "completeness", "doi_format": "validity", "count_negative": "validity", "title_missing": "completeness",
        "issued_invalid": "validity", "issn_invalid": "validity", "source_updated_at_missing": "source_completeness",
        "arxiv_id_invalid": "validity", "arxiv_category_missing": "completeness", "arxiv_doi_invalid": "validity"}
VALIDATOR_CATALOG = [
    Rule(f"ingest_{rid}", _DIM[rid], sev, "ods.crossref_works" if sev == "error" else "dwd.paper_version",
         f"ingest-time rule {rid} ({'quarantines record' if sev == 'error' else 'warning kept with version'})",
         "python-validator", version=ver)
    for rid, (sev, ver) in VALIDATOR_RULES.items()
]

ALL_RULES = PANDERA_RULES + SQL_RULES + [TIMELINESS] + VALIDATOR_CATALOG
