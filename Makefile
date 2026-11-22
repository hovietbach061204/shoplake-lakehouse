.PHONY: venv check data explore

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

explore:         ## Profile the raw CSVs with DuckDB
	$(PYTHON) scripts/explore_olist.py
