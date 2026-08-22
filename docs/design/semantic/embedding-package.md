# Embedding Package

## Status

Current daemon embedding utilities live under `internal/embedding` and are used
by semantic generation rules. Legacy embedded/session profile generation APIs and
the old `internal/embeddingstore` package are removed.

## Purpose

The embedding package owns low-level source assembly helpers and vector payload
normalization. It does not own rule selection, Intelligence Access resolution,
credential grants, policy decisions, semantic maintenance scheduling, or semantic
search planning.

Semantic generation rules orchestrate embedding generation:

```text
graph change -> trigger filter -> target selector -> source assembly ->
Intelligence Access profile resolution -> embedding provider call ->
vector record write -> physical search-index update
```

## Package boundaries

Semantic subsystem packages compose the embedding utilities:

```text
internal/embedding/       source assembly and embedding payload helpers
internal/semantic/model/  semantic generation rule model and validation
internal/semantic/backfill/ rule/binding backfill execution
internal/semantic/maintenance/ dirty analysis and work processing
internal/semantic/vectorstore/ vector records and physical search-index backend
internal/semantic/search/ binding-aware search planner
```

## Durable vector records

Durable vector records are attributed by rule, binding, and target:

```text
semantic_rule_id + embedding_binding_key + target_node_id
```

Records store enough provenance to explain generation and rebuild physical search
indexes:

- semantic rule ID
- embedding binding key
- target node ID
- source hash and source policy details
- vector-space/model provenance resolved through Intelligence Access
- policy decision and credential grant provenance where applicable
- tombstone state for deleted/empty sources

The physical search index is derived state. For `mycel-file`, latest-live search
records are maintained per rule/binding and can be rebuilt from durable vector
records after deletion, corruption, or startup.

## Dirty event analysis

Dirty graph events are not embedding work yet. The analyzer decides whether a
changed node affects embedding targets for each enabled semantic generation rule:

1. Load enabled rules for the relevant space/domain.
2. Apply the rule trigger filter (`changed` events and label filters).
3. Resolve targets using the rule's node-type selector or bounded selector GQL.
4. For each enabled embedding binding, upsert dirty work keyed by
   `semantic_rule_id + embedding_binding_key + target_node_id`.
5. Use `SemanticMaintenancePolicy.DirtyCooldown`, `EarliestRunAt`, and durable
   coalescing so repeated changes update the existing work item instead of
   creating unbounded duplicate work.

Deleted nodes tombstone latest vector records or enqueue affected target refresh
work according to the rule/source policy.

## Source assembly

Worker/backfill execution loads the rule and binding, then assembles source text
from the rule source policy:

- `self` for the target node's own properties;
- `subtree` for bounded descendant context;
- included/excluded property filters;
- minimum text length checks;
- optional future bounded context GQL.

Empty or too-short sources tombstone the latest vector record instead of calling
an embedding provider.

## Idempotency

Embedding calls are skipped when the idempotency key has already succeeded:

```text
semantic_rule_id + embedding_binding_key + target_node_id + source_mode + source_hash + vector_space_key
```

Forced backfill can intentionally refresh current embeddings, but normal dirty
processing should avoid provider calls when source and binding provenance are
unchanged.

## Search-index interaction

Vector upserts and tombstones update or invalidate the per-rule/per-binding
physical search index. Semantic search uses those physical indexes or bounded
rebuilds; it must not silently scan unbounded historical vector records.

Search-index status is surfaced to Admin and Console views so operators can see
record counts, rebuild timestamps, and degraded/error states.
