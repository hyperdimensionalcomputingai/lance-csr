# Writing the lance-csr docs

These are the guidelines for anyone, human or agent, who edits the documentation site in this directory.
Read them before you change a page.

## What the site is for

A reader should be able to find out what lance-csr is, install it, run a query, and learn every user-facing
option without reading the source. Write for a Python developer who knows a little Cypher and nothing about
the internals; `rust.md` covers the same engine for Rust developers. The CSR index is the project's distinguishing feature, so it gets the most care.

The one exception is `internals/`, the collapsed "DataFusion internals" section under "How it works". It is for
contributors and the few readers who want the technical detail: how lance-csr extends DataFusion (extension nodes,
optimizer rules, graph rewrites, statistics) and how the indexes are maintained. Keep user-facing behaviour on the
guide pages and link to it from there rather than repeating it. Its plans are `explain()` output pasted as `text`
blocks; when an operator's display or the planner changes, re-run those queries against a local build and update
them.

## How the site is built

- [Zensical](https://zensical.org/) renders the Markdown in `docs/`. `zensical.toml` at the repository root
  holds the navigation, theme and Markdown extensions. A page missing from `nav` doesn't appear in the menu.
- The tooling is a separate, locked uv project: `docs/pyproject.toml` and `docs/uv.lock`. Never use `pip`.
  Change the Zensical version with `uv add --project docs --group docs zensical==<version>`.
- Preview with `uv run --project docs --locked zensical serve`; check with
  `uv run --project docs --locked zensical build --strict`, which CI runs on every pull request. Fix every
  warning.
- Cloudflare Pages builds and hosts the site at https://lance-csr.hdclabs.ai/ (a custom domain on the
  `lance-csr` Pages project) through its native GitHub integration. `site_url` in `zensical.toml` must match
  it, because it sets the canonical and social-preview URLs. Pushes to `main` automatically deploy when automatic production deployments are enabled.
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

- **Document what exists on `main`.** Read the code, tests and docstrings before writing. The public Python
  classes are in `python/python/lance_csr/`, wrapping the compiled bindings in
  `crates/lance-csr-python/src/graph.rs`, on top of the Rust `CypherEngine` in
  `crates/lance-csr/src/engine.rs`; the Cypher surface in `crates/lance-csr/src/`
  (`parser.rs`, `semantic.rs`, `logical_plan.rs`). Never describe planned features as shipped.
- **Every Python example runs.** `check_snippets.py` executes each page's `python` blocks in order, in one
  namespace, in a temporary directory. Run it against a local build before you commit:

    ```bash
    cd python && uv run --no-sync python ../docs/check_snippets.py
    ```

  Put the output in a trailing comment (`print(x)  # [{'name': 'Bob'}]`) and check it matches what the run
  printed. Use `ORDER BY` so results are deterministic, and don't show output that varies (timings, dict or
  label order) without saying so.
- **Every Rust example compiles and runs.** Rust code on a page is never typed into the Markdown: it lives in
  `crates/lance-csr/examples/embedded_query.rs` between `// --8<-- [start:name]` and `// --8<-- [end:name]`
  markers, and the page includes it with `--8<-- "crates/lance-csr/examples/embedded_query.rs:name"` inside a
  `rust` fence. CI builds and runs the example, and the strict build fails if a file or section is missing.
  Keep the output comments in the example in step with what it prints.
- A snippet that can't run on its own (it needs LanceDB, a remote store or an ANN index) gets a
  `<!-- skip-check -->` line directly above its fence. Use this sparingly.
- **Every user-facing knob is documented**: constructor arguments, keyword arguments, builder methods, enum
  members, defaults, return types and the exceptions raised. When you add or change one, update the guide page
  that teaches it and the docstring of the Python class in `python/python/lance_csr/`, in the same pull request
  as the code. `api-reference.md` renders those docstrings with mkdocstrings (NumPy style); its prose sections
  (type checking, datasets, exceptions, timeouts, path values) are still written by hand. Docstring examples
  run as doctests in `make test`.
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
