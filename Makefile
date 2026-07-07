.DEFAULT_GOAL := help
.PHONY: help install test lint demo bench plot clean

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-9s\033[0m %s\n", $$1, $$2}'

install: ## install package with dev extras (editable)
	pip install -e ".[dev]"

test: ## run the tests
	pytest

lint: ## static checks
	ruff check ballast tests benchmark

demo: ## fabricate a job having a bad day, find the problems (no infra)
	ballast demo

bench: ## measure parse + analyze throughput
	python -m benchmark.harness

plot: ## render the chart from the last bench run
	python -m benchmark.plot

clean: ## remove generated files
	rm -rf data .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
