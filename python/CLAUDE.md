Never use `pip`; dependencies are managed with uv and locked in `uv.lock`. Use the Makefile for most actions (from `python/`):

* Set up and build: `make install` (`uv sync --locked --no-install-project`, then `uv run --no-sync maturin develop`)
* Rebuild after Rust changes: `make install`
* Test: `make test`
* Run single test: `uv run --no-sync pytest python/tests/<test_file>.py::<test_name>`
* Lint: `make lint`
* Format: `make format`
* Add a dependency: `uv add <pkg>` (development tools: `uv add --group dev <pkg>`)
