import csv
import io
import json
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from psycopg_pool import ConnectionPool

from paperforge.config.settings import get_settings
from paperforge.repositories.analytics import EXPORT_COLS, PgAnalyticsRepository

SortKey = Literal["year_desc", "year_asc", "cited_desc", "title_asc", "relevance"]


def create_app(dsn: str | None = None) -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = ConnectionPool(dsn or settings.pg_dsn, min_size=1, max_size=8, open=True, kwargs={"autocommit": True})
        app.state.repo = PgAnalyticsRepository(pool)
        app.state.settings = settings
        yield
        pool.close()

    app = FastAPI(title="PaperForge API", version="1.0.0", lifespan=lifespan,
                  description="Read-only data service over the PaperForge warehouse (ads/dws/dwd/meta).")
    app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], allow_methods=["GET"], allow_headers=["*"])

    def repo(request: Request) -> PgAnalyticsRepository:
        return request.app.state.repo

    R = Annotated[PgAnalyticsRepository, Depends(repo)]

    def filters(doi: str | None = None, arxiv_id: str | None = Query(None, description="arXiv id, e.g. 2210.03629"),
                category: str | None = Query(None, description="arXiv category the paper is listed in, e.g. cs.CL"), doi_prefix: str | None = None, q: str | None = Query(None, description="full-text over title, journal, authors"),
                title: str | None = None, journal: str | None = Query(None, description="journal title substring or ISSN-derived journal_key"),
                publisher: str | None = Query(None, description="publisher name substring or Crossref member id"),
                year: int | None = None, year_from: int | None = None, year_to: int | None = None) -> dict:
        return {k: v for k, v in dict(doi=doi, arxiv_id=arxiv_id, category=category, doi_prefix=doi_prefix, q=q, title=title, journal=journal, publisher=publisher,
                                      year=year, year_from=year_from, year_to=year_to).items() if v not in (None, "")}

    F = Annotated[dict, Depends(filters)]

    @app.get("/api/v1/health")
    def health(r: R):
        r._q("SELECT 1")
        return {"status": "ok"}

    @app.get("/api/v1/papers", summary="Search / filter / paginate papers")
    def papers(r: R, f: F, page: int = Query(1, ge=1), page_size: int = Query(25, ge=1, le=200), sort: SortKey = "year_desc"):
        return r.search_papers(f, page, page_size, sort)

    @app.get("/api/v1/papers/export", summary="Export filtered papers as CSV or JSONL (max 100,000 rows)")
    def export(r: R, f: F, format: Literal["csv", "jsonl"] = "csv", limit: int = Query(10000, ge=1, le=100000)):
        def gen():
            if format == "csv":
                buf = io.StringIO()
                w = csv.DictWriter(buf, fieldnames=EXPORT_COLS)
                w.writeheader()
                yield buf.getvalue()
                for row in r.export_cursor(f, limit):
                    buf = io.StringIO()
                    csv.DictWriter(buf, fieldnames=EXPORT_COLS).writerow(row)
                    yield buf.getvalue()
            else:
                for row in r.export_cursor(f, limit):
                    yield json.dumps(row, ensure_ascii=False, default=str) + "\n"
        media = "text/csv" if format == "csv" else "application/x-ndjson"
        return StreamingResponse(gen(), media_type=media, headers={"Content-Disposition": f'attachment; filename="papers.{format}"'})

    @app.get("/api/v1/papers/{paper_id}", summary="One paper: standardized fields, authors, references, identifiers, raw record")
    def paper(paper_id: str, r: R):
        try:
            import uuid
            uuid.UUID(paper_id)
        except ValueError:
            raise HTTPException(422, "paper_id must be a UUID")
        p = r.get_paper(paper_id)
        if not p:
            raise HTTPException(404, "paper not found")
        return p

    @app.get("/api/v1/papers/{paper_id}/versions", summary="Version history on the source_updated_at timeline")
    def versions(paper_id: str, r: R):
        try:
            import uuid
            uuid.UUID(paper_id)
        except ValueError:
            raise HTTPException(422, "paper_id must be a UUID")
        v = r.paper_versions(paper_id)
        if v is None:
            raise HTTPException(404, "paper not found")
        return {"paper_id": paper_id, "versions": v}

    @app.get("/api/v1/statistics/overview")
    def overview(r: R):
        return r.overview()

    @app.get("/api/v1/quality/metrics")
    def quality(r: R, quarantine_limit: int = Query(50, ge=0, le=500)):
        return r.quality(quarantine_limit)

    @app.get("/api/v1/quality/reconciliation", summary="DuckDB over the parquet archive vs PostgreSQL counts")
    def reconciliation(r: R, request: Request):
        return r.reconciliation(request.app.state.settings.data_dir)

    @app.get("/api/v1/pipelines/runs")
    def runs(r: R, limit: int = Query(20, ge=1, le=200)):
        return r.pipeline_runs(limit)

    @app.get("/api/v1/datasets/lineage", summary="Dataset-level lineage (whole graph, or up/down from one dataset)")
    def lineage(r: R, dataset: str | None = None, direction: Literal["up", "down"] = "up"):
        return r.lineage(dataset, direction)

    @app.get("/api/v1/datasets")
    def catalog(r: R):
        return r.catalog(None)

    @app.get("/api/v1/datasets/{dataset_id}")
    def dataset(dataset_id: str, r: R):
        d = r.catalog(dataset_id)
        if not d:
            raise HTTPException(404, "dataset not found")
        return d

    return app


app = create_app()
