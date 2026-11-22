.PHONY: venv check data data-funnel explore up down reset ps logs trino m1-demo

# Which Python to use: the project's .venv if it exists, otherwise whatever
# `python3` is active in your shell (e.g. an activated conda env).
# Override explicitly with: make data PYTHON=/path/to/python
PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

venv:            ## Create a local Python env with the helper tools
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt

check:           ## Check Docker/Colima resources and disk
	bash scripts/check_machine.sh

data:            ## Download the Olist dataset into data/raw
	$(PYTHON) scripts/download_olist.py

data-funnel:     ## Download the Olist marketing funnel dataset into data/raw
	$(PYTHON) scripts/download_marketing_funnel.py

explore:         ## Profile the raw CSVs with DuckDB
	$(PYTHON) scripts/explore_olist.py

# ---- M1: the lakehouse stack (Docker Compose) ----
up:              ## Start MinIO + Iceberg REST catalog + Trino
	docker compose up -d

down:            ## Stop the stack (data is kept in Docker volumes)
	docker compose down

reset:           ## Stop the stack AND delete all buckets/tables (asks first)
	@read -p "Delete ALL lakehouse data (MinIO buckets + catalog)? [y/N] " a; [ "$$a" = "y" ] && docker compose down -v || echo "cancelled"

ps:              ## Show service status
	docker compose ps -a

logs:            ## Follow logs of all services (Ctrl+C to stop)
	docker compose logs -f --tail 50

trino:           ## Open an interactive Trino SQL shell
	docker compose exec trino trino

m1-demo:         ## Create the M1 demo Iceberg table and inspect it
	docker compose exec -T trino trino --output-format=ALIGNED < sql/m1_first_table.sql
