# Vector search

Knowledge graphs often carry embeddings next to their structure. lance-csr gives you two ways to use them:
**vector functions** inside Cypher, and a **`VectorSearch`** builder that ranks rows by similarity, either
on its own or as a reranking step after a graph query (the common GraphRAG pattern).

```python
import pyarrow as pa
from lance_csr import CypherEngine, DistanceMetric, GraphConfig, VectorSearch

documents = pa.table({
    "id": [1, 2, 3],
    "title": ["Graph joins", "Image search", "CSR layouts"],
    "topic": ["graphs", "vision", "graphs"],
    "embedding": pa.array(
        [[1.0, 0.0], [0.0, 1.0], [0.7, 0.7]], type=pa.list_(pa.float32(), 2)
    ),
})
config = GraphConfig.builder().node_table("Document", "id").build()
engine = CypherEngine(config, {"Document": documents})
```

Store embeddings as a fixed-size list of `float32` (`pa.list_(pa.float32(), dim)`), as Lance and LanceDB do.

## Vector functions in Cypher

```cypher
vector_distance(a, b, metric)
vector_similarity(a, b, metric)
```

`metric` is a bare keyword (not a string): `cosine`, `l2` or `dot`. One argument must be a property; the
other can be a property or a vector literal.

```python
result = engine.execute("""
    MATCH (d:Document)
    WHERE vector_similarity(d.embedding, [1.0, 0.0], cosine) > 0.5
    RETURN d.title AS title, vector_distance(d.embedding, [1.0, 0.0], l2) AS dist
    ORDER BY dist
""")
print(result.to_pylist())
# [{'title': 'Graph joins', 'dist': 0.0}, {'title': 'CSR layouts', 'dist': 0.7615...}]
```

| Metric | `vector_distance` | `vector_similarity` |
| --- | --- | --- |
| `l2` | Euclidean distance | `1 / (1 + distance)` |
| `cosine` | `1 - cos(a, b)` | `cos(a, b)` |
| `dot` | `-(a · b)` | `a · b` |

Smaller distances and larger similarities mean closer vectors. Comparing vectors of different dimensions
gives the worst possible score rather than an error.

!!! warning "Write vector literals into the query"

    `CypherEngine` doesn't accept list parameters yet, so `$query_vector` bound to a list raises
    `ValueError`. Format the vector into the query text.

## `VectorSearch`

`VectorSearch` is an immutable builder: each method returns a new one.

| Method | Default | Meaning |
| --- | --- | --- |
| `VectorSearch(column)` | | The vector column to search. In reranking, `alias.column` (`"d.embedding"`) |
| `.query_vector(vector)` | required | The query vector, a list of floats |
| `.metric(metric)` | `DistanceMetric.L2` | `DistanceMetric.L2`, `.Cosine` or `.Dot` |
| `.top_k(k)` | `10` | Number of rows to return |
| `.include_distance(include)` | `True` | Add a distance column to the result |
| `.distance_column_name(name)` | `"_distance"` | Name of that column |
| `.use_lance_index(enabled)` | `False` | Use a Lance ANN index first (see below) |

### Searching a table directly

`.search(table)` runs a brute-force search over a `pyarrow.Table`:

```python
nearest = (
    VectorSearch("embedding")
    .query_vector([1.0, 0.0])
    .metric(DistanceMetric.Cosine)
    .top_k(2)
    .search(documents)
)
print(nearest.select(["title", "_distance"]).to_pylist())
# [{'title': 'Graph joins', '_distance': 0.0}, {'title': 'CSR layouts', '_distance': 0.2928...}]
```

### Reranking a graph query

`execute_with_vector_rerank` runs the Cypher query first, to find candidates by structure, then ranks those
candidates by similarity. The query must return the vector column:

```python
reranked = engine.execute_with_vector_rerank(
    "MATCH (d:Document) WHERE d.topic = 'graphs' RETURN d.title, d.embedding",
    VectorSearch("d.embedding")
    .query_vector([0.0, 1.0])
    .metric(DistanceMetric.Cosine)
    .top_k(1)
    .distance_column_name("score"),
)
print(reranked.select(["d.title", "score"]).to_pylist())
# [{'d.title': 'CSR layouts', 'score': 0.2928...}]
```

`CypherQuery.execute_with_vector_rerank(datasets, vector_search)` does the same without an engine.

### Using a Lance ANN index

When the datasets are Lance datasets with a vector index, `.use_lance_index(True)` changes the order of work:
the engine asks the Lance index for the top-k nearest rows first, then runs the Cypher query on just those.

<!-- skip-check -->
```python
search = (
    VectorSearch("d.embedding")
    .query_vector(query_embedding)
    .metric(DistanceMetric.Cosine)
    .top_k(10)
    .use_lance_index(True)
)
results = query.execute_with_vector_rerank({"Document": lance.dataset("docs.lance")}, search)
```

!!! danger "This can change results"

    With the index path, the Cypher query only sees the top-k vectors. If the query also filters (by topic,
    say), it may return fewer than k rows, or miss rows that rank lower overall but pass the filter. Use it
    when the vector condition is the main selector and speed on large datasets matters.
