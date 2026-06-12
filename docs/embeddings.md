# KnotDB embeddings MVP

KnotDB includes a manual embeddings subsystem for generating derived vectors from graph nodes and searching them semantically.

## Architecture

Embedding configuration is system metadata:

- built-in embedding provider/model catalog
- user-owned encrypted provider API keys
- user-owned embedding profiles

Generated vectors are space-scoped derived data stored outside `Node.Props` under the graph space directory:

```text
graphs/<space_id>/embeddings/embeddings.jsonl
```

The initial vector index is rebuilt from this append-only JSONL file and searched with brute-force cosine similarity. ANN indexes, compaction, background jobs, and automatic mutation triggers are intentionally deferred.

## Source modes

Manual generation supports two source modes:

- `self`: embed the selected node content, plus optional included props
- `subtree`: embed the selected node plus ordered descendants through `contains` edges

Sibling order comes from the `contains` edge `Props["order"]` value. The assembled source text is hashed; generation skips an existing matching embedding unless `--force` is provided.

## CLI examples

List the embedding catalog:

```sh
knotdb embeddings catalog -u USER -p PASSWORD
```

Add a provider key:

```sh
export OPENAI_API_KEY=...
knotdb embeddings keys add \
  --provider openai \
  --name personal \
  --api-key-env OPENAI_API_KEY \
  --default \
  -u USER -p PASSWORD
```

Add a profile:

```sh
knotdb embeddings profiles add \
  --name pkm-page-default \
  --provider openai \
  --model openai/text-embedding-3-small \
  --source subtree \
  --include-prop title \
  -u USER -p PASSWORD
```

Generate an embedding for a node:

```sh
knotdb embeddings generate \
  --space-id SPACE_ID \
  --node NODE_ID \
  --profile PROFILE_ID \
  -u USER -p PASSWORD
```

Regenerate even if the source hash has not changed:

```sh
knotdb embeddings generate \
  --space-id SPACE_ID \
  --node NODE_ID \
  --profile PROFILE_ID \
  --force \
  -u USER -p PASSWORD
```

Search:

```sh
knotdb embeddings search \
  --space-id SPACE_ID \
  --profile PROFILE_ID \
  --text "notes about graph storage" \
  --limit 10 \
  -u USER -p PASSWORD
```

## Deferred work

The MVP does not include automatic triggers, durable job queues, chunking, blob text extraction, ANN indexes, or PKM UI integration.
