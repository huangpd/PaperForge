.PHONY: setup up down test test-all smoke ingest ingest-arxiv fetch-doi transform dbt quality publish worker replay status api demo frontend-dev airflow-up docs

LIMIT ?= 1000
UV := uv run python -m paperforge.cli

# --- environment -------------------------------------------------------------------------------------------------------
setup:                      ## python deps (uv, Python 3.12), frontend deps, .env
	uv sync --group dev --group dbt
	cd frontend && npm install --no-audit --no-fund
	@test -f .env || cp .env.example .env

up:                         ## PostgreSQL, Kafka (KRaft), worker, API, web console
	docker compose up -d --build --wait postgres kafka worker api frontend
	@echo "console http://localhost:$${FRONTEND_PORT:-5173}   api http://localhost:$${API_PORT:-8000}/docs"

airflow-up:                 ## optional: Airflow (single-node, LocalExecutor) on http://localhost:8080
	docker compose --profile airflow up -d --build airflow

down:
	docker compose --profile airflow down

# --- pipeline ----------------------------------------------------------------------------------------------------------
ingest:                     ## make ingest LIMIT=1000 | 10000 | 100000  (domain-stratified sample of the offline Crossref dump)
	$(UV) migrate
	$(UV) ingest --limit $(LIMIT)

ingest-arxiv:               ## make ingest-arxiv ARXIV=~/Code/arxiv/arxiv.zip [CATS="cs.AI,cs.CL,stat.ML"] [MATCH=any] [N=2000]  (then `make dbt`)
	$(UV) migrate
	$(UV) ingest-arxiv "$(ARXIV)" $(if $(CATS),--categories "$(CATS)",) $(if $(MATCH),--match $(MATCH),) $(if $(N),--limit $(N),)

fetch-doi:                  ## make fetch-doi DOI="10.1016/j.radphyschem.2017.05.027" (several: DOI="a b"; REFRESH=1 also runs dbt so search shows it)
	$(UV) migrate
	$(UV) events init-topics
	$(UV) fetch-doi $(DOI) $(if $(REFRESH),--refresh,)

transform:                  ## Batch Mode: raw JSONL -> DWD
	$(UV) migrate
	$(UV) transform

publish:                    ## announce ODS rows to Kafka (the worker container turns them into DWD)
	$(UV) events init-topics
	$(UV) events publish

worker:                     ## run the consumer on the host instead of the container
	$(UV) worker run

dbt:                        ## dbt build (models + tests), record results + lineage + catalog
	$(UV) migrate
	$(UV) dbt-build
	$(UV) quality load-dbt
	$(UV) lineage sync-dbt
	$(UV) catalog refresh

quality:                    ## run all quality rules, write the daily report (exit 1 if an Error rule fails)
	$(UV) quality run
	$(UV) quality report

replay:                     ## replay raw files as events; BATCH=<id> limits to one ingestion batch
	$(UV) events replay $(if $(BATCH),--batch $(BATCH),)

status:
	$(UV) events status

api:                        ## API on the host (port 8000)
	uv run uvicorn paperforge.api.app:app --host 127.0.0.1 --port 8000

frontend-dev:               ## Vite dev server with proxy to the host API
	cd frontend && npm run dev

# --- one command: end-to-end ---------------------------------------------------------------------------------------------
demo: up                    ## ingest -> Kafka -> worker -> dbt -> quality -> lineage -> console (idempotent; LIMIT=1000 by default)
	$(UV) migrate
	$(UV) ingest --limit $(LIMIT)
	$(UV) transform
	$(UV) events init-topics
	$(UV) events publish
	$(MAKE) dbt
	$(MAKE) quality
	$(UV) events status
	@echo ""
	@echo "Open the console:  http://localhost:$${FRONTEND_PORT:-5173}"
	@echo "API docs:          http://localhost:$${API_PORT:-8000}/docs"

# --- tests -------------------------------------------------------------------------------------------------------------
test:                       ## unit tests only (no services needed)
	uv run pytest tests/unit -q

test-all:                   ## unit + integration + e2e (needs `make up`; uses isolated databases/topics)
	uv run pytest tests -q

smoke:
	uv run pytest tests/integration/test_smoke.py -v
