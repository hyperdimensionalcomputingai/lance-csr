UV ?= uv
PYTHON ?= 3.12
VENV ?= .venv
PYTEST_ARGS ?= -q
BIN := $(VENV)/bin

.PHONY: venv install test lint format wheel clean

# A virtual environment for development.
venv:
	$(UV) venv --python $(PYTHON) $(VENV)

# Debug build of the extension, installed with the test and lint tools.
install:
	$(UV) pip install --python $(BIN)/python 'maturin[patchelf]'
	VIRTUAL_ENV=$(abspath $(VENV)) $(BIN)/maturin develop --extras tests,dev

test:
	$(BIN)/pytest $(PYTEST_ARGS) python/tests

lint:
	$(BIN)/ruff check python
	$(BIN)/ruff format --check python
	$(BIN)/pyright

format:
	$(BIN)/ruff format python

# Release wheel and sdist for this platform, in dist/ (CI builds the published ones).
wheel:
	VIRTUAL_ENV=$(abspath $(VENV)) $(BIN)/maturin build --release --out dist --sdist

clean:
	rm -rf $(VENV) dist ../crates/lance-graph-python/target
