# Convenience targets (needs GNU make; on Windows without make, run the commands directly).
PY ?= python

.PHONY: install test test-fast lint synthetic data features train train-smoke mlflow-ui

install:
	$(PY) -m pip install -r requirements.txt
	$(PY) -m pip install -e . --no-deps

test:
	$(PY) -m pytest tests -q

test-fast:
	$(PY) -m pytest tests -q -m "not slow"

lint:
	$(PY) -m ruff check src tests

synthetic:
	$(PY) -m riskstream.cli generate-synthetic
	$(PY) -m riskstream.cli prepare-data --dataset synthetic
	$(PY) -m riskstream.cli build-features --dataset synthetic

data:
	$(PY) -m riskstream.cli prepare-data --dataset paysim

features:
	$(PY) -m riskstream.cli build-features --dataset paysim

train:
	$(PY) -m riskstream.cli train --dataset paysim

train-smoke:
	$(PY) -m riskstream.cli train --dataset synthetic --n-trials 1

mlflow-ui:
	$(PY) -m mlflow ui --backend-store-uri sqlite:///mlflow.db
