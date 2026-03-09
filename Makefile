.PHONY: install init-db lint test eod-date eod-catchup

PYTHONPATH ?= src
PYTHON ?= python3

install:
	$(PYTHON) -m pip install -e '.[dev]'

init-db:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/init_db.py

lint:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m ruff check src tests

test:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m pytest -q

eod-date:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod run-date --date $(DATE)

eod-catchup:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod catchup --start-date $(START) --end-date $(END)
