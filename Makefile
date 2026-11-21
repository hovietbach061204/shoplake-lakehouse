.PHONY: venv check data explore

venv:            ## Create a local Python env with the helper tools
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt

check:           ## Check Docker/Colima resources and disk
	bash scripts/check_machine.sh

data:            ## Download the Olist dataset into data/raw
	.venv/bin/python scripts/download_olist.py

explore:         ## Profile the raw CSVs with DuckDB
	.venv/bin/python scripts/explore_olist.py
