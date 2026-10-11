# recommendx

`recommendx` stores item vectors in Redis hashes and retrieves ranked
recommendation candidates with KNN search and optional filters.

```mermaid
flowchart LR
    Items --> Hashes[Item hashes]
    Hashes --> Index[Search index]
    Query[User vector and filters] --> Index
    Index --> Results[Recommendations]
```

## Requirements

- Redis Search with vector support.
- One fixed vector size per recommender.
- `COSINE`, `L2`, or `IP` distance. Empty means `COSINE`.

## API

- `CreateIndex` creates the search index.
- `UpsertItem` stores a vector and declared fields.
- `Recommend` returns nearest items in score order.
- `DeleteItem` removes an item.

`Prefix`, `IndexName`, and positive `Dimensions` are required. Tag and numeric
fields must be declared in config before use. Unsupported search commands are
reported through `ErrCapability`.

See [USAGE.md](USAGE.md) for setup.
