# Stage 6 report: data service, console, end-to-end validation

2026-11-19. Figures are from real runs; see `docs/acceptance.md` for the final checklist and `docs/test_report.md` for tests.

## Delivered
- **FastAPI** (`src/paperforge/api/`, `repositories/analytics.py`): the 7 required endpoints — `GET /api/v1/papers` (DOI, DOI prefix, title, full text, journal/ISSN key, publisher/member id, year / range, sort, pagination), `/papers/{id}` (standardized fields, authors, references, identifiers, **raw record + normalized snapshot**), `/papers/{id}/versions` (timeline with changed fields), `/statistics/overview`, `/quality/metrics`, `/pipelines/runs`, `/datasets/lineage` — plus `/papers/export` (CSV/JSONL, streamed, ≤100,000 rows), `/datasets` + `/datasets/{id}` (catalog), `/quality/reconciliation` (DuckDB over the parquet archive vs PostgreSQL), `/health`. Inputs are validated (422), SQL is parameterised and sort keys are whitelisted. `AnalyticsRepository` interface with a PostgreSQL implementation.
- **Console** (`frontend/`, Vite + React 18 + TypeScript + ECharts): Overview · Pipeline · Data Explorer · Data Quality · Data Lineage · Data Catalog. Plain, compact styling; every number comes from the API.
- **Deployment**: `api` and `frontend` (nginx, proxies `/api`, streams exports) Compose services; Makefile rewritten (`setup`, `up`, `demo`, `ingest LIMIT=`, `transform`, `dbt`, `quality`, `replay`, `test`, `test-all`, `down`, …).
- **Docs generated from the live system**: `docs/er.md`, `docs/data_dictionary.md` (`scripts/gen_docs.py`), `docs/test_report.md` (`scripts/gen_test_report.py`), `docs/dbt-dag.md`.
- `QualityValidator` interface added so all 8 core interfaces of requirement §15 exist in code.

## Verified
- **API on real data (101,000 papers):** search 0.19 s, DOI lookup 6 ms, overview 0.25 s; export of one publisher = 12,602 rows = the search total.
- **`make demo`**: exit 0 from a running stack (ingest 1,000 → transform → Kafka publish → worker → dbt 73/73 → quality 0 Error failures → lineage → catalog); Kafka lag 0, no ODS row left outside DWD/quarantine.
- **Browser (real Chrome driven by Playwright) against the containerised console:** all six pages render with live data; search; detail drawer (standardized / raw-vs-standardized / versions); CSV export; lineage node selection; run expansion; catalog deep link; severity filter. No console errors, no failed API calls.
- **End-to-end tests** (`tests/e2e`, 14): real CLI commands through Kafka → worker → batch replay → dbt → quality → lineage → catalog → API, in an isolated database, Kafka topics, data dir and dbt target. They also prove the real warehouse contains no test DOI.
- **Full suite: 101/101 passed** (35 unit, 52 integration, 14 e2e).

## Follow-up: language switch and pipeline cards
- **EN / 中文 switch** in the sidebar (`frontend/src/i18n.tsx`): all UI text, chart labels, status badges, table headers, filter options and quality-dimension names are translated; the choice is stored in `localStorage` and, if none is stored, follows the browser language. Keys are the English strings, so a missing translation falls back to English instead of breaking. Values that come from the backend as data (e.g. rule descriptions, dataset descriptions) stay in English; technical terms (DOI, Parquet, layer names, dbt) are kept as is. Filters keep using the raw API value, so switching language never changes what a filter does.
- **Pipeline stage cards** now share one layout (title + status, aligned In / Out / Failed, two footer lines), identical height in both languages.
- Verified with real Chrome (`frontend/ui-check-i18n.mjs`: default language, toggle, persistence across reload, drawer tabs, badges, filters, switch back, Chinese-locale start, no console errors) and the original English checks (`frontend/ui-check.mjs`, now pinned to `en-US`).

## Problems found and fixed in this stage
- CSV/JSONL export returned only the header: a server-side cursor needs a transaction while the pool uses autocommit.
- Lineage graph was unreadable (overlapping labels, tiny); relaid out one column per layer with shortened labels.
- Raw-vs-standardized panes overflowed the drawer (long JSON lines widened the CSS grid); the standardized pane was off-screen.
- Pipeline page listed Airflow task names as separate stages and sorted them wrongly; stages are now keyed by the CLI command stored in `task_run.details.command` (existing rows back-filled once with a SQL update).
- A React missing-`key` warning and a missing favicon (404) in the console.
- My first browser checks asserted before the data had rendered (0 stages found) — fixed to wait, then re-run; the "passing" first run was not trustworthy.
- `DBT_TARGET` collided with a dbt environment variable; the setting is now `DBT_TARGET_PATH`.
- Playwright's own Chromium download was truncated by the proxy; the check uses the installed Google Chrome instead.

## Limitations
- The browser check (`frontend/ui-check.mjs`) needs Chrome + `playwright`; it is a script, not part of `make test`.
- No authentication on API/console (local use, ports bound to 127.0.0.1).
- The console is English-only; no dark mode; no pagination beyond the explorer table.
- The Docker images were built and run on one Apple-silicon machine.
