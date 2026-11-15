# RiskStream Makefile
# Convenient commands for development and operations

.PHONY: help install test lint format clean data demo

help:  ## Show this help message
	@echo "RiskStream - Real-Time Fraud Detection Platform"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install dependencies
	pip install -r requirements.txt

install-dev:  ## Install development dependencies
	pip install -r requirements.txt
	pip install -e .

test:  ## Run tests
	pytest tests/ -v

test-unit:  ## Run unit tests only
	pytest tests/unit/ -v

test-cov:  ## Run tests with coverage
	pytest tests/ -v --cov=src/riskstream --cov-report=html --cov-report=term

lint:  ## Run linting
	ruff check src/ tests/ scripts/

format:  ## Format code
	ruff format src/ tests/ scripts/

type-check:  ## Run type checking
	mypy src/

clean:  ## Clean generated files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf htmlcov/ .coverage

data:  ## Prepare PaySim dataset (load, validate, EDA, split)
	python scripts/prepare_data.py

data-force:  ## Force reload PaySim dataset from CSV
	python scripts/prepare_data.py --force-reload

demo:  ## Generate synthetic demo data
	python scripts/generate_demo_data.py --num-transactions 10000 --run-eda

# Phase 1 complete workflow
phase1: install data demo test  ## Complete Phase 1 setup and validation
