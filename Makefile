.PHONY: install init-db lint test test-postgres-integration preflight preflight-lite pre-data-ready eod-date eod-catchup sync-corporate-actions

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

test-postgres-integration:
	PYTHONPATH=$(PYTHONPATH) TEST_POSTGRES_URL=$(TEST_POSTGRES_URL) $(PYTHON) -m pytest -q tests/test_postgres_corporate_action_integration.py

preflight:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod preflight

preflight-lite:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod preflight --skip-db

pre-data-ready: test preflight-lite

eod-date:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod run-date --date $(DATE)

eod-catchup:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod catchup --start-date $(START) --end-date $(END)

sync-corporate-actions:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m quant.cli.main eod sync-corporate-actions --start-date $(START) --end-date $(END)
