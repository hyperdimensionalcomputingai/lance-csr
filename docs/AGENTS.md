# Writing the lance-csr docs

These are the guidelines for anyone, human or agent, who edits the documentation site in this directory.
Read them before you change a page.

## What the site is for

A reader should be able to find out what lance-csr is, install it, run a query, and learn every user-facing
option without reading the source. Write for a Python developer who knows a little Cypher and nothing about
the internals; `rust.md` and `typescript.md` cover the same engine for Rust and Node.js developers. The CSR index
is the project's distinguishing feature, so it gets the most care.

The one exception is `internals/`, the collapsed "DataFusion internals" section under "How it works". It is for
contributors and readers who want the technical detail: how lance-csr extends DataFusion (extension nodes,
optimizer rules, graph rewrites, statistics) and how the indexes are maintained. Keep user-facing behaviour on the
guide pages and link to `internals/` rather than repeating implementation detail. Its plans are `explain()` output
pasted as `text` blocks, often trimmed to the lines that matter, so `check_snippets.py` can't check them.
`explain_plans.py` prints every one of them, along with the plans in `running-queries.md` and `csr.md`, from a
local build, each under the page and section it belongs to. Whenever you change an operator or its display, an
optimizer rule, a rewrite or the planner, or touch a page that shows a plan:

1. Rebuild the extension (`cd python && make install`).
2. Run the script. The 100,000-person plans need a checkout of
   [graph-benchmark](https://github.com/hyperdimensionalcomputingai/graph-benchmark) with its Lance datasets built
   (`lance_csr/build_graph.py`); without `--graph-benchmark` the script skips them and says so.

    ```bash
    cd python && uv run --no-sync python ../docs/explain_plans.py --graph-benchmark ../../graph-benchmark
    ```

3. Compare each section's plan with the page, line by line, and update the page where they differ, keeping its
   trimming (`...`, `, ...`). If the prose around a plan describes an operator or a line that changed, update it too.
4. When you add a plan to a page, add its query and graph to `explain_plans.py` under that page's name.

## Where new content belongs

Balance human readability with the technical completeness that agents and power users rely on. Put each detail in
one place, at the right layer:

| Kind of detail | Where it goes |
| --- | --- |
| User-facing concept, workflow, or common option | Main flow of the relevant guide page (`graph-schema.md`, `running-queries.md`, `csr.md`, `lance.md`, `vector-search.md`, `graph-algorithms.md`, `sql.md`) or `cypher.md` for language-agnostic Cypher features |
| Exhaustive edge cases, coercion rules, dialect quirks, or third-party library caveats | A collapsible `??? note` or `??? info` block on the guide or `cypher.md` page, so agents and search still index every detail without breaking reading flow |
| Engine internals: operator mechanics, adaptive thresholds (`1/256`, `1/8`, `256 MiB`), optimizer eligibility rules, internal struct fields (`__lance_csr_*`), or index layouts | `docs/internals/` (`operators.md`, `optimizer.md`, `indexes.md`, `execution-plumbing.md`), with at most a one-sentence summary and link from the user guide |
| Exact signatures, parameter defaults, types, and exception lists | Python docstrings (`python/python/lance_csr/`), TypeScript TSDoc (`nodejs/src/`, regenerating `nodejs/typedoc/api.md`), and Rust doc comments |

Keep page boundaries and structure clean as features land:

- **Integrate, do not just append.** When a PR extends an existing feature, revise the section as a coherent whole
  (update the table, example, or bullet list) instead of tacking another paragraph onto the top or bottom.
- **No changelog prose.** Document current behaviour only. Do not write *"Earlier versions did..."*, and do not
  repeat *"fork of lance-graph"* provenance notes outside `index.md` and `internals/index.md`.
- **Respect page ownership.** Engine-wide runtime features (streaming, deadlines, `explain()`, plan caching,
  `target_partitions`) belong in `running-queries.md`; language-agnostic Cypher syntax and semantics belong in
  `cypher.md`; Lance on-disk storage, versioning, LanceDB, and compaction belong in `lance.md`.
- **Keep the sidebar scannable (`zensical.toml`).** Core practitioner capabilities stay visible under `Guide`;
  deep reference or secondary language trees stay in nested collapsible directories (`Rust & TypeScript`,
  `API reference`, `DataFusion internals`).
- **Prefer clear practitioner terminology.** For combined graph and vector workflows, write *"hybrid graph and
  vector RAG"* (or *"hybrid graph and vector retrieval"*); keep *"GraphRAG"* to the single intro mention on
  `vector-search.md`.

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
- Files that aren't pages (this file, `check_snippets.py`, `explain_plans.py`, `sync_benchmarks.py`, the uv project, `overrides/`, the
  brand README and previews) are listed under `exclude_docs` in `zensical.toml`. Add new non-page files there.
- Benchmark tables between `<!-- bench:... -->` markers in `benchmarks.md` are
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
  run as doctests in `make test`. `typescript-api.md` includes `nodejs/typedoc/api.md`, which TypeDoc generates
  from the TSDoc comments in `nodejs/src/`: regenerate it with `npm run docs:api` from `nodejs/` and commit it.
- State limits plainly. Don't claim a property ("constant memory", "never materializes") unless code and
  benchmarks show it. Performance numbers come from `benchmarks.md` sources only, with their methodology.
- Keep scope in mind: lance-csr is single-machine. Don't document distributed execution or proprietary HDC
  Labs systems.

## Style

- Explain what something is and why it exists before how to use it. Don't open a page or section with an
  instruction ("Call it after…", "Use this when…"): the reader needs the what and the why to judge the how. Mix
  short prose with code; a concept that takes more than two paragraphs probably needs an example or a diagram.
- Don't assume the reader knows the background. Give the context a newcomer needs (for example, that Lance
  datasets gain new versions as they're written, so the engine has to be told about them), and define a term,
  such as the CSR's delta, before relying on it.
- Say each thing once. A section shouldn't restate what the page already said above it; keep sentences short and
  cut words that add no information.
- Use Zensical components where they help the reader, not for decoration:
    - admonitions (`!!! tip`, `note`, `warning`, `danger`, `info`, `example`, `failure`, `bug`) for advice,
      caveats and known issues; `???` makes a collapsible one for long detail;
    - content tabs (`=== "Tab"`) for alternatives, such as install methods or input types;
    - code annotations (`# (1)!` plus a numbered list after the block) to explain specific lines;
    - Mermaid diagrams (```` ```mermaid ````) for flows: query pipelines, `checkout_latest()` decisions, snapshots;
    - tables for options and comparisons, definition lists for method references;
    - grid cards on the home page only.
- Plain, direct sentences. Use "you". Avoid filler and marketing adjectives. Spell out a term the first time
  (compressed sparse row) and link to the page that explains it.
- **Avoid em dashes (`—`) and awkward hyphenations.** Use commas, colons, parentheses, or two shorter sentences
  instead of em dashes. Keep standard technical compounds (`read-only`, `in-memory`, `multi-hop`, `zero-copy`,
  `variable-length`), but avoid forced compound modifiers like `in-flight`, `filtered-out`, `one-edge-per-source`,
  or `capability-based` when plain wording reads more naturally.
- **Avoid colliding verbs and garden-path sentences.** Do not drop `that`/`which` when it puts two verbs back to
  back (*"a newer format than Lance knows fails"*, *"a version cleanup deletes fails"*, *"whose variable the query
  never reads runs"*). Rephrase so every sentence flows smoothly on the first read without forcing a double take.
- **No `github.io` links.** Always link to `https://lance.org/` for the Lance format and docs, and
  `https://lancedb.com/docs/` for LanceDB.
- Use sentence-case headings, and keep page titles short; they appear in the navigation.
- Link between pages with relative Markdown links (`[CSR index](csr.md#inspecting-the-csr)`), which the
  strict build checks.
- Commands use `uv`, never `pip`. Shell examples go in `bash` fences, one command per line.
