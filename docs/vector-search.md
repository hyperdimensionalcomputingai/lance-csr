# Vector search

Knowledge graphs stored in Lance can hold embeddings right next to structured columns. lance-csr lets you combine
graph traversal and embedding similarity over that same data (hybrid graph and vector RAG, sometimes called
GraphRAG) in two ways: **vector functions** inside Cypher, and a **`VectorSearch`** builder that ranks rows by
similarity on its own or after a graph query.

The examples below model a small authoring graph where people know each other and author documents carrying 2D
`float32` embeddings:

```python
import pyarrow as pa
from lance_csr import CypherEngine, DistanceMetric, GraphConfig, VectorSearch

documents = pa.table({
    "id": [10, 20, 30, 40],
    "title": ["Graph joins", "Image search", "CSR layouts", "Lance versioning"],
    "embedding": pa.array(
        [[1.0, 0.0], [0.0, 1.0], [0.7, 0.7], [0.9, 0.4]],
        type=pa.list_(pa.float32(), 2),
    ),
})
datasets = {
    "Person": pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}),
    "Document": documents,
    "KNOWS": pa.table({"src": [1, 2], "dst": [2, 3]}),
    "AUTHORED": pa.table({"src": [1, 2, 2, 3], "dst": [10, 20, 30, 40]}),
}
config = (
    GraphConfig.builder()
    .node_table("Person", "id")
    .node_table("Document", "id")
    .rel_table("KNOWS", "Person", "Person", "src", "dst")
    .rel_table("AUTHORED", "Person", "Document", "src", "dst")
    .build()
)
engine = CypherEngine(config, datasets, use_csr=True)
```

Store embeddings as a fixed-size list of `float32` (`pa.list_(pa.float32(), dim)`), as Lance and LanceDB do.

## Vector functions in Cypher

```cypher
vector_distance(a, b, metric)
vector_similarity(a, b, metric)
```

`metric` is a bare keyword (not a string): `cosine`, `l2` or `dot`. One argument must be a property; the other
can be a property, a vector literal, or a `$parameter`. Whole-number query vectors such as `[1, 0]` also work and
are converted to `float32` inside the vector operation; elements must be non-null numbers.

Use these functions directly inside a Cypher traversal to narrow candidates by graph structure and filter or sort
by embedding similarity in one query. For example, find documents written by Alice's 1-hop and 2-hop connections
that are close to `[1.0, 0.0]`:

```python
result = engine.execute(
    """
    MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(author:Person)-[:AUTHORED]->(d:Document)
    WHERE vector_similarity(d.embedding, $q, cosine) > 0.5
    RETURN author.name AS author,
           d.title AS title,
           vector_distance(d.embedding, $q, l2) AS dist
    ORDER BY dist
    """,
    {"q": [1.0, 0.0]},
)
print(result.to_pylist())
# [{'author': 'Carol', 'title': 'Lance versioning', 'dist': 0.4123...},
#  {'author': 'Bob', 'title': 'CSR layouts', 'dist': 0.7615...}]
```

| Metric | `vector_distance` | `vector_similarity` |
| --- | --- | --- |
| `l2` | Euclidean distance | `1 / (1 + distance)` |
| `cosine` | `1 - cos(a, b)` | `cos(a, b)` |
| `dot` | `-(a · b)` | `a · b` |

Smaller distances and larger similarities mean closer vectors. Comparing vectors of different dimensions produces
the worst possible score rather than an error.

## `VectorSearch`

`VectorSearch` is an immutable builder: each method returns a new instance.

| Method | Default | Meaning |
| --- | --- | --- |
| `VectorSearch(column)` | | Vector column to search (for reranking, the returned column name such as `"d.embedding"` or its alias) |
| `.query_vector(vector)` | required | Query vector as a list of floats |
| `.metric(metric)` | `DistanceMetric.L2` | `DistanceMetric.L2`, `.Cosine` or `.Dot` |
| `.top_k(k)` | `10` | Maximum number of rows to return |
| `.include_distance(include)` | `True` | Append a distance column to the result |
| `.distance_column_name(name)` | `"_distance"` | Name of the appended distance column |
| `.use_lance_index(enabled)` | `False` | Search a Lance ANN index first (see below) |

### Hybrid graph and vector reranking

`execute_with_vector_rerank` runs a two-stage hybrid retrieval workflow: Cypher first traverses the graph to find
structurally relevant candidates, and `VectorSearch` then ranks those candidates by similarity and keeps the
closest `top_k`. The query must return the vector column:

```python
reranked = engine.execute_with_vector_rerank(
    """
    MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(author:Person)-[:AUTHORED]->(d:Document)
    RETURN author.name, d.title, d.embedding
    """,
    VectorSearch("d.embedding")
    .query_vector([0.0, 1.0])
    .metric(DistanceMetric.Cosine)
    .top_k(2)
    .distance_column_name("score"),
)
print(reranked.select(["author.name", "d.title", "score"]).to_pylist())
# [{'author.name': 'Bob', 'd.title': 'Image search', 'score': 0.0},
#  {'author.name': 'Bob', 'd.title': 'CSR layouts', 'score': 0.2928...}]
```

`CypherQuery.execute_with_vector_rerank(datasets, vector_search)` provides the same reranking without an engine.

### Searching a table directly

`.search(table)` runs an exact brute-force vector search over a standalone `pyarrow.Table`:

```python
nearest = (
    VectorSearch("embedding")
    .query_vector([1.0, 0.0])
    .metric(DistanceMetric.Cosine)
    .top_k(2)
    .search(documents)
)
print(nearest.select(["title", "_distance"]).to_pylist())
# [{'title': 'Graph joins', '_distance': 0.0}, {'title': 'Lance versioning', '_distance': 0.0861...}]
```

### Pre-filtering with a Lance ANN index

When your datasets are Lance datasets with a vector index, `.use_lance_index(True)` reverses the order of work
for `CypherQuery.execute_with_vector_rerank`: it queries the Lance ANN index for the `top_k` nearest rows first,
then runs the Cypher query on just those rows and returns them as produced (without an appended distance column
or distance sorting). `CypherEngine.execute_with_vector_rerank` ignores this flag and always runs exact reranking.
Queries containing a `WHERE` or `WITH` clause also take the exact path so filters see every row.

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

    With the ANN index path, the Cypher query only sees the `top_k` nearest vectors. If its pattern also filters
    (for example with an inline property map or a relationship match), it may return fewer than `k` rows or miss
    rows that rank lower by vector distance alone but satisfy the graph pattern. Use it when vector similarity is
    the primary selector and speed on large datasets matters most.
