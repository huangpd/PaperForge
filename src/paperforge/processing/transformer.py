"""NormalizedPaper list -> typed Polars frames (nested arrays exploded, dates/ints cast). No business rules here."""
from dataclasses import dataclass

import polars as pl

from paperforge.processing.models import NormalizedPaper

PAPER_SCHEMA = {
    "rec_idx": pl.Int64, "doi": pl.Utf8, "title": pl.Utf8, "abstract": pl.Utf8, "container_title": pl.Utf8,
    "publisher": pl.Utf8, "member": pl.Utf8, "type": pl.Utf8, "language": pl.Utf8, "volume": pl.Utf8,
    "issue": pl.Utf8, "page": pl.Utf8, "issns": pl.List(pl.Utf8), "issued_year": pl.Int32, "issued_month": pl.Int32,
    "issued_day": pl.Int32, "reference_count": pl.Int32, "cited_by_count": pl.Int32, "author_count": pl.Int32,
    "content_hash": pl.Utf8, "source_updated_at": pl.Datetime("us", "UTC"),
}
AUTHOR_SCHEMA = {"rec_idx": pl.Int64, "ordinal": pl.Int32, "given": pl.Utf8, "family": pl.Utf8, "name": pl.Utf8,
                 "orcid": pl.Utf8, "sequence": pl.Utf8, "affiliations": pl.List(pl.Utf8)}
REF_SCHEMA = {"rec_idx": pl.Int64, "ordinal": pl.Int32, "ref_key": pl.Utf8, "ref_doi": pl.Utf8,
              "unstructured": pl.Utf8, "article_title": pl.Utf8, "journal_title": pl.Utf8, "ref_year": pl.Int32}


@dataclass
class Frames:
    papers: pl.DataFrame
    authors: pl.DataFrame
    references: pl.DataFrame


def transform(items: list[tuple[int, NormalizedPaper]]) -> Frames:
    """items: (rec_idx, normalized). rec_idx links child rows back to their paper record."""
    papers = [{"rec_idx": i, "doi": n.doi, "title": n.title, "abstract": n.abstract, "container_title": n.container_title,
               "publisher": n.publisher, "member": n.member, "type": n.type, "language": n.language,
               "volume": n.volume, "issue": n.issue, "page": n.page, "issns": n.issns, "issued_year": n.issued_year,
               "issued_month": n.issued_month, "issued_day": n.issued_day, "reference_count": n.reference_count,
               "cited_by_count": n.cited_by_count, "author_count": len(n.authors), "content_hash": n.content_hash,
               "source_updated_at": n.source_updated_at} for i, n in items]
    authors = [{"rec_idx": i, **a} for i, n in items for a in n.authors]
    refs = [{"rec_idx": i, **r} for i, n in items for r in n.references]
    p = pl.DataFrame(papers, schema=PAPER_SCHEMA).with_columns(
        pl.when(pl.col("issued_year").is_not_null())
        .then(pl.date(pl.col("issued_year"), pl.col("issued_month").fill_null(1), pl.col("issued_day").fill_null(1)))
        .alias("issued_date"),
        pl.when(pl.col("issued_day").is_not_null()).then(pl.lit("day"))
        .when(pl.col("issued_month").is_not_null()).then(pl.lit("month"))
        .when(pl.col("issued_year").is_not_null()).then(pl.lit("year")).alias("issued_precision"))
    return Frames(p, pl.DataFrame(authors, schema=AUTHOR_SCHEMA), pl.DataFrame(refs, schema=REF_SCHEMA))
