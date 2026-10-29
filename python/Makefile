UV ?= uv
PYTHON ?= 3.12
PYTEST_ARGS ?= -q
# Run tools in .venv as synced; a plain `uv run` would re-sync and drop the extension build.
RUN := $(UV) run --no-sync

.PHONY: sync install test lint format wheel clean

# Locked dependencies and dev tools (maturin, pytest, ruff, pyright) into .venv. The project
# itself is left out (uv would build it in release mode), and --inexact keeps an existing build.
sync:
	$(UV) sync --locked --no-install-project --inexact --python $(PYTHON)

# Debug build of the extension into .venv; re-run after changing Rust code.
install: sync
	$(RUN) maturin develop

test:
	$(RUN) pytest -n auto $(PYTEST_ARGS) python/tests --doctest-modules python/lance_csr

lint:
	$(RUN) ruff check python
	$(RUN) ruff format --check python
	$(RUN) pyright

format:
	$(RUN) ruff format python

# Release wheel and sdist for this platform, in dist/ (CI builds the published ones).
wheel:
	$(RUN) maturin build --release --out dist --sdist

clean:
	rm -rf .venv dist ../crates/lance-csr-python/target
