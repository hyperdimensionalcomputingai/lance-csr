# Examples for lance-csr

## How to run the examples

With the released package, from the repository root (uv creates a temporary environment with lance-csr and its
dependencies):

```bash
uv run --with lance-csr python examples/basic_cypher.py
uv run --with lance-csr python examples/kg_traversal.py
uv run --with lance-csr python examples/lance_snapshot_refresh.py
```

With a local build, so the examples use your changes, set up `python/.venv` as in
[`python/DEVELOPMENT.md`](../python/DEVELOPMENT.md) and run them from it:

```bash
cd python
uv sync --locked --no-install-project
uv run --no-sync maturin develop
uv run --no-sync python ../examples/basic_cypher.py
```

## Examples

- `basic_cypher.py`: simple node filter and projection.

```bash
uv run --with lance-csr python examples/basic_cypher.py
```

- `kg_traversal.py`: multi-hop traversal on a small synthetic knowledge graph.

```bash
uv run --with lance-csr python examples/kg_traversal.py
```

- `lance_snapshot_refresh.py`: lazy Lance datasets, snapshots and `refresh()`.

```bash
uv run --with lance-csr python examples/lance_snapshot_refresh.py
```
