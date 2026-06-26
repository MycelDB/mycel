# Current Embeddings MVP

MycelDB includes a manual embeddings subsystem for generating derived vectors from graph nodes and searching them semantically.

## Architecture

Embedding configuration is system metadata:

- built-in embedding provider/model catalog
- user-owned encrypted provider API keys
- user-owned embedding profiles

Generated vectors are space-scoped derived data stored outside `Node.Props` under the graph space directory. Storage details are documented in [../storage/semantic.md](../storage/semantic.md).

The current vector index is rebuilt from append-only `.kvec` segments and searched with brute-force cosine similarity. ANN indexes, compaction, background jobs, and automatic mutation triggers are intentionally deferred.

## Source modes

Manual generation supports two source modes:

- `self`: embed the selected node content, plus optional included props
- `subtree`: embed the selected node plus ordered descendants through `contains` edges

Sibling order comes from the `contains` edge `Props["order"]` value. The assembled source text is hashed; generation skips an existing matching embedding unless `--force` is provided.

## CLI commands

Current MVP embedding command syntax is documented in the CLI command reference:

- [embeddings catalog](../cli/commands/embeddings-catalog.md)
- [embeddings keys add](../cli/commands/embeddings-keys-add.md)
- [embeddings profiles add](../cli/commands/embeddings-profiles-add.md)
- [embeddings generate](../cli/commands/embeddings-generate.md)
- [embeddings search](../cli/commands/embeddings-search.md)

Batch generation requires at least one selector: `--node`, `--template-key`, or `--contains`. Existing current embeddings are skipped unless `--force` is set. Use `--continue-on-error` to keep processing when one selected node fails.

## Deferred work

The MVP does not include automatic triggers, durable job queues, chunking, blob text extraction, ANN indexes, or application UI integration.
