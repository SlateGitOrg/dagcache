# One entry point for every gate. `make check` is what CI runs.
UV      ?= uv
PY_SRC  := src tests examples scripts bench

.PHONY: help install fmt format-check lint typecheck test cov check demo bench integration clean

help:
	@echo "make check        format-check + lint + typecheck + tests with coverage gate"
	@echo "make demo         offline end-to-end demo on the bundled examples"
	@echo "make bench        micro-benchmarks (hashing, overhead, parallelism)"
	@echo "make integration  S3 backend against a real S3 server (SeaweedFS) in Docker (skips without Docker)"

install:
	$(UV) sync --frozen

fmt:
	$(UV) run --frozen ruff format $(PY_SRC)
	$(UV) run --frozen ruff check --fix $(PY_SRC)

format-check:
	$(UV) run --frozen ruff format --check $(PY_SRC)

lint:
	$(UV) run --frozen ruff check $(PY_SRC)

typecheck:
	$(UV) run --frozen mypy

test:
	$(UV) run --frozen pytest -q

cov:
	$(UV) run --frozen pytest -q --cov --cov-report=term-missing:skip-covered

check: format-check lint typecheck cov

demo:
	$(UV) run --frozen python -u scripts/demo.py

bench:
	$(UV) run --frozen python -u bench/bench.py

integration:
	./scripts/integration.sh

clean:
	rm -rf build .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
