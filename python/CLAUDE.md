Use the Makefile for most actions (from `python/`):

* Set up: `make venv`, then `make install` (debug build with test and lint tools)
* Rebuild after Rust changes: `make install` or `maturin develop`
* Test: `make test`
* Run single test: `pytest python/tests/<test_file>.py::<test_name>`
* Lint: `make lint`
* Format: `make format`
