# Writing the lance-csr docs

These are the guidelines for anyone, human or agent, who edits the documentation site in this directory.
Read them before you change a page.

## What the site is for

A reader should be able to find out what lance-csr is, install it, run a query, and learn every user-facing
option without reading the source. Write for a Python developer who knows a little Cypher and nothing about
the internals. The CSR index is the project's distinguishing feature, so it gets the most care.

## How the site is built

- [Zensical](https://zensical.org/) renders the Markdown in `docs/`. `zensical.toml` at the repository root
  holds the navigation, theme and Markdown extensions. A page missing from `nav` doesn't appear in the menu.
- The tooling is a separate, locked uv project: `docs/pyproject.toml` and `docs/uv.lock`. Never use `pip`.
  Change the Zensical version with `uv add --project docs --group docs zensical==<version>`.
- Preview with `uv run --project docs --locked zensical serve`; check with
  `uv run --project docs --locked zensical build --strict`, which CI runs on every pull request. Fix every
  warning.
- Cloudflare Pages builds and hosts the site at https://lance-csr.pages.dev/ through its native GitHub
  integration. Pushes to `main` automatically deploy when automatic production deployments are enabled.
  Build from the repository root with the locked build command above and publish `site/`.
- Files that aren't pages (this file, `check_snippets.py`, `sync_benchmarks.py`, the uv project, `overrides/`, the
  brand README and previews) are listed under `exclude_docs` in `zensical.toml`. Add new non-page files there.
- Benchmark tables between `<!-- bench:... -->` markers in `benchmarks.md` and the root `README.md` are
  generated; never edit them by hand. The runners and results live in graph-benchmark-ldbc-complex and
  graph-benchmark-ldbc. To publish a new run, merge it there, pin its commit in `sync_benchmarks.py`, and run
  `uv run --project docs --locked python docs/sync_benchmarks.py`.
- Brand assets are in `assets/brand/`; their `README.md` gives colours and usage. Light and dark images use the
  `#only-light` and `#only-dark` URL suffixes. The header logo is in `overrides/partials/logo.html`; the brand
  colours are in `stylesheets/extra.css`.

## Accuracy comes first

- **Document what exists on `main`.** Read the code, tests and docstrings before writing. The Python API is
  defined in `crates/lance-graph-python/src/graph.rs`; the Cypher surface in `crates/lance-graph/src/`
  (`parser.rs`, `semantic.rs`, `logical_plan.rs`). Never describe planned features as shipped.
- **Every Python example runs.** `check_snippets.py` executes each page's `python` blocks in order, in one
  namespace, in a temporary directory. Run it against a local build before you commit:

    ```bash
    cd python && uv run --no-sync python ../docs/check_snippets.py
    ```

  Put the output in a trailing comment (`print(x)  # [{'name': 'Bob'}]`) and check it matches what the run
  printed. Use `ORDER BY` so results are deterministic, and don't show output that varies (timings, dict or
  label order) without saying so.
- A snippet that can't run on its own (it needs LanceDB, a remote store or an ANN index) gets a
  `<!-- skip-check -->` line directly above its fence. Use this sparingly.
- **Every user-facing knob is documented**: constructor arguments, keyword arguments, builder methods, enum
  members, defaults, return types and the exceptions raised. When you add or change one, update both the guide
  page that teaches it and `api-reference.md`, in the same pull request as the code.
- State limits plainly. Don't claim a property ("constant memory", "never materializes") unless code and
  benchmarks show it. Performance numbers come from `benchmarks.md` sources only, with their methodology.
- Keep scope in mind: lance-csr is single-machine. Don't document distributed execution or proprietary HDC
  Labs systems.

## Style

- Lead with what the reader wants to do, then show it. Mix short prose with code; a concept that takes more
  than two paragraphs probably needs an example or a diagram.
- Use Zensical components where they help the reader, not for decoration:
    - admonitions (`!!! tip`, `note`, `warning`, `danger`, `info`, `example`, `failure`, `bug`) for advice,
      caveats and known issues; `???` makes a collapsible one for long detail;
    - content tabs (`=== "Tab"`) for alternatives, such as install methods or input types;
    - code annotations (`# (1)!` plus a numbered list after the block) to explain specific lines;
    - Mermaid diagrams (```` ```mermaid ````) for flows: query pipelines, refresh decisions, snapshots;
    - tables for options and comparisons, definition lists for method references;
    - grid cards on the home page only.
- Plain, direct sentences. Use "you". Avoid filler and marketing adjectives. Spell out a term the first time
  (compressed sparse row) and link to the page that explains it.
- Use sentence-case headings, and keep page titles short; they appear in the navigation.
- Link between pages with relative Markdown links (`[CSR index](csr.md#inspecting-the-csr)`), which the
  strict build checks.
- Commands use `uv`, never `pip`. Shell examples go in `bash` fences, one command per line.
