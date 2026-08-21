# Semantic Generation Rules

## Status

Design direction. This document supersedes the older mental model where a
`SemanticIndex` is mainly a static embedding-index configuration with advisory
source fields. The intended user-facing model is a **semantic generation rule**:
a constrained, system-owned automation that reacts to graph changes and
maintains embedding/vector records for selected graph targets.

Because the product is not released, this design does not preserve backward
compatibility with the older `SemanticIndex` API/storage shape. The next
semantic indexing tranche should replace incompatible fields and commands rather
than carry long-term compatibility aliases.

## Summary

Semantic indexing should be treated as a specialized graph automation:

```text
graph change -> trigger filter -> target selector -> source assembly ->
inference access resolution -> embedding generation -> vector/index write ->
usage accounting
```

Unlike graph automations, semantic generation rules do not run arbitrary scripts,
prompt templates, or mutation actions. Their action is fixed:

```text
maintain embeddings for selected graph targets
```

This gives semantic indexing the same operational shape as graph automation while
keeping it safer, simpler, and easier to optimize.

## Goals

- Make semantic indexing understandable as declarative graph-reactive behavior.
- Support simple node-type/label rules and bounded GQL target selectors.
- Reuse graph automation/inference access-control concepts:
  - service actor;
  - owner/on-behalf-of principal;
  - inference profiles;
  - credential grants;
  - inference policies;
  - usage accounting and denial diagnostics.
- Support one semantic rule maintaining one or more embedding bindings.
- Preserve deterministic derived-state behavior: vectors are generated artifacts
  that can be rebuilt from graph data plus rule configuration.
- Define storage and cache boundaries clearly, especially in raft mode.
- Remove older semantic-index fields/behaviors that no longer fit the model.

## Non-goals

- Semantic generation rules are not arbitrary automation workflows.
- Semantic generation rules do not mutate user graph nodes/edges directly.
- Semantic generation rules do not choose provider credentials directly.
- Semantic generation rules do not repair divergent cluster state.
- This design does not require external vector databases before the local
  `mycel-file` backend remains production-safe.

## User-facing concept

A semantic generation rule answers five questions:

1. **When should this rule be considered?**
   - graph events;
   - labels/types;
   - optional debounce/coalescing.
2. **Which graph targets should have embeddings?**
   - node type / labels;
   - bounded read-only GQL selector;
   - future explicit fan-out controls.
3. **What text should be embedded for each target?**
   - self properties;
   - subtree/context assembly;
   - included/excluded properties;
   - minimum text length.
4. **Which embeddings should be generated?**
   - one or more embedding bindings;
   - each binding references an inference profile and vector store.
5. **Who is allowed to do the work and who pays for it?**
   - owner/on-behalf-of principal;
   - standard inference grants/policies;
   - usage events attributed to the semantic rule and binding.

Example rule shape:

```json
{
  "key": "journal-entry-search",
  "name": "Journal entry semantic search",
  "enabled": true,
  "scope": {
    "spaceId": "sp_...",
    "domainId": "dom_..."
  },
  "trigger": {
    "events": ["node.created", "node.updated", "node.deleted", "edge.created", "edge.deleted"],
    "labels": ["JournalEntry"],
    "debounce": "120s"
  },
  "selector": {
    "mode": "node_type",
    "labels": ["JournalEntry"]
  },
  "source": {
    "mode": "self",
    "includeProperties": ["title", "body", "tags"],
    "minimumTextLength": 20
  },
  "embeddings": [
    {
      "key": "default-search",
      "profile": "journal-entry-embeddings",
      "vectorStore": "mycel-file",
      "purpose": "semantic_search"
    }
  ]
}
```

More complex selector:

```json
{
  "selector": {
    "mode": "gql",
    "query": "MATCH (j:Journal)-[:HAS_ENTRY]->(e:JournalEntry) RETURN e FETCH FIRST 1000 ROWS ONLY",
    "targetAlias": "e"
  },
  "source": {
    "mode": "subtree",
    "includeProperties": ["title", "body"],
    "maxDepth": 2
  }
}
```

## Relationship to graph automation

Semantic generation rules should reuse graph automation design patterns, but not
its full action model.

| Concern | Graph automation | Semantic generation rule |
| --- | --- | --- |
| Trigger | graph events | graph events |
| Target selection | condition/context GQL | node-type or bounded selector GQL |
| Action | arbitrary configured action(s) | fixed embedding maintenance |
| Inference | profile/model access | embedding profile access |
| Output | graph mutations or external effects | derived vector/index records |
| Idempotency | invocation/action dependent | source hash + binding key |
| Usage | automation/run scoped | semantic rule/binding scoped |
| Safety | user/application-defined | system-owned derived data |

The shared abstraction should be the front half of the pipeline:

```text
trigger -> target selector -> access resolution -> accounting
```

The execution backend remains separate:

```text
automation action runner
semantic embedding maintainer
```

## Rule model

Internally, replace the existing `SemanticIndex` model with a
`SemanticGenerationRule` model.

Recommended conceptual fields:

```go
type SemanticGenerationRule struct {
    ID        SemanticRuleID
    SpaceID   SpaceID
    DomainID  DomainID
    Key       string
    Name      string
    Enabled   bool

    Trigger      SemanticTriggerPolicy
    Selector     SemanticTargetSelector
    Source       SemanticSourceAssemblyPolicy
    Embeddings   []SemanticEmbeddingBinding
    Access       SemanticAccessPolicy
    Maintenance  SemanticMaintenancePolicy
    Storage      SemanticStoragePolicy

    CreatedByPrincipalID PrincipalID
    OwnerPrincipalID     PrincipalID
    CreatedAt            time.Time
    UpdatedAt            time.Time
}
```

### Trigger policy

The trigger policy is a cheap pre-filter for committed graph changes.

```go
type SemanticTriggerPolicy struct {
    Events   []GraphEventType
    Labels   []string
    Debounce time.Duration
}
```

Rules:

- If omitted, graph node/edge changes in the rule's domain may dirty the rule.
- Trigger matching must be cheap and should not execute GQL.
- Trigger matching only decides whether analysis is worth doing; selector
  matching remains authoritative.

### Target selector

The selector identifies embedding targets.

Supported modes:

1. `node_type`
   - match node labels/types;
   - best default for most use cases.
2. `gql`
   - bounded read-only GQL;
   - must return the target alias;
   - must be anchored to the changed node or constrained by domain/scope;
   - must include an explicit row limit.
3. `explicit_nodes`
   - useful for backfill/manual test operations;
   - not a normal automatic rule mode.

Selector validation should reuse the graph-context automation hardening:

- compile/inspect GQL instead of substring checks;
- read-only only;
- bounded with `FETCH FIRST`;
- relationship patterns must be labeled;
- reject unknown target aliases;
- enforce max limit at runtime;
- fail closed on ambiguous target resolution unless explicit fan-out is enabled.

### Source assembly policy

Source assembly decides the text used for each embedding target.

```go
type SemanticSourceAssemblyPolicy struct {
    Mode              string // self | subtree | context_query
    IncludeProperties []string
    ExcludeProperties []string
    MaxDepth          *int
    MinimumTextLength int
    ContextQuery      string // future, read-only bounded GQL
}
```

`self` embeds selected properties from the target node.

`subtree` embeds target plus contained descendants using labeled containment
edges.

`context_query` is a future option for graph-neighborhood source assembly. It
must follow the same bounded/read-only validation rules as selector GQL.

### Embedding bindings

A semantic rule may generate more than one embedding for the same target. This
is the answer to "the set of embeddings to use".

```go
type SemanticEmbeddingBinding struct {
    Key              string
    Purpose          SemanticRulePurpose
    InferenceProfile string
    InferenceProfileID uuid.UUID
    VectorStore      string
    VectorStoreID    uuid.UUID
    Enabled          bool
    Metadata         map[string]any
}
```

Each binding represents one independently maintained vector record stream.

Examples:

- `default-search` using a small embedding model;
- `local-private-search` using a local-only profile;
- `large-rerank-context` for future larger vector spaces.

Rules:

- Bindings reference inference profiles, not raw endpoints/models/credentials.
- Profile resolution determines endpoint/model/capability.
- Credential grants and policies decide whether the service actor may generate
  embeddings on behalf of the rule owner.
- Vector-store resolution must remain reference-safe.

### Access policy

Semantic generation should use the same access mechanism as graph automations.

Recommended runtime identity:

```text
actor: semantic-maintenance service actor
on_behalf_of: rule owner principal
scope: rule space/domain/semantic rule/binding
```

Access checks:

- rule management requires semantic manage capability on the relevant scope;
- graph reads require domain visibility/read access for the rule owner or an
  explicit system-owned semantic role;
- embedding calls resolve through inference profiles;
- credential grants must allow background/service use;
- inference policies can allow/deny/restrict by space, domain, semantic rule,
  node, model, endpoint, operation, privacy class, and data class.

Usage events must include at least:

- `space_id`;
- `domain_id`;
- `semantic_rule_id`;
- `semantic_binding_key` or binding ID;
- inference profile ID;
- endpoint/model/capability IDs;
- credential grant/policy decision IDs;
- actor and on-behalf-of principal IDs;
- token counts, latency, status, denial reason.

### Maintenance policy

```go
type SemanticMaintenancePolicy struct {
    DirtyCooldown     time.Duration
    MaxBatchSize      int
    WorkerConcurrency int
    RetryPolicy       RetryPolicy
}
```

Defaults should continue to come from daemon semantic maintenance config, with
rule-level overrides only where safe.

## Storage design

Semantic data falls into four categories with different durability rules.

### 1. Rule definitions

Rule definitions are authoritative metadata and must be raft/WAL-owned in raft
mode.

Recommended logical location:

```text
graphs/<space_id>/semantic/rules.json
```

or segmented:

```text
graphs/<space_id>/semantic/rules/<rule_id>/rule.json
```

The existing space semantic manager storage should be rewritten around rules.
Because there is no released compatibility contract, do not preserve
`SemanticIndex` as a compatibility view.

Required indexes/materialized views:

- by rule ID;
- by `(space_id, domain_id, key)`;
- by enabled state;
- by trigger labels/events;
- by binding vector store/profile refs for reference-safe delete checks.

### 2. Dirty event and work state

Dirty events and work items are operational state, but they are still durable and
must be raft/WAL-owned in raft mode.

Current layout remains valid:

```text
graphs/<space_id>/semantic/maintenance/
  dirty/graph-dirty-000001.ksem
  work/state.json
  checkpoints.json
```

Recommended evolution:

- dirty events remain graph-change oriented;
- work items become keyed by `(rule_id, binding_key, target_node_id)`;
- work items retain source transaction/revision provenance;
- checkpoints track rule/binding analysis progress;
- batch upsert remains preferred for high-volume imports.

### 3. Vector/index records

Vector records are derived data. They must be durable but rebuildable.

Recommended logical layout:

```text
graphs/<space_id>/semantic/indexes/<rule_id>/<binding_key>/
  manifest.ksem
  records/embeddings-000001.kvec
```

The `mycel-file` layout should be rewritten to use rule IDs and binding keys
directly.

Each record should include:

- rule ID;
- binding key or binding ID;
- target node ID;
- source hash;
- source mode;
- graph revision range or source revision;
- model/profile/capability binding IDs;
- credential grant and policy decision IDs;
- vector store ID;
- dimensions/vector-space key;
- tombstone fields.

Idempotency key:

```text
(rule_id, binding_key, target_node_id, source_mode, source_hash, vector_space_key)
```

The writer should skip generation when the latest non-tombstone record has the
same idempotency key unless force/backfill explicitly requests refresh.

### 4. Physical search indexes

Semantic generation rules must support fast semantic search. Append-only vector
records are the durable source of truth, but search must not require a full scan
of all historical records on every query.

For each searchable embedding binding, maintain a rebuildable physical search
index:

```text
graphs/<space_id>/semantic/search/<rule_id>/<binding_key>/
  manifest.ksem
  latest-records.kidx
  vectors.kidx
  graph.fidx          # optional future ANN graph/HNSW-style structure
```

Minimum first implementation:

- load/build a latest-live-record map per rule binding;
- keep normalized vectors in memory or mmap-backed arrays;
- filter by domain/rule/binding before scoring;
- score only latest non-tombstone records;
- update the loaded index on vector upsert/tombstone;
- rebuild the physical index from durable vector records at startup or after
  corruption detection.

Future larger-scale implementation:

- add an approximate nearest-neighbor structure per vector space/rule binding;
- compact old append-only vector segments into optimized search shards;
- support background index rebuilds with atomic manifest swaps;
- expose search-index health and rebuild status through Admin/Semantic status
  APIs.

Physical search indexes are derived state. They can be deleted and rebuilt from
vector records, but they are required for low-latency semantic search once record
counts grow beyond small development datasets.

### 5. Derived status and accounting views

Status is a materialized view and should be rebuildable from rule definitions,
work state, vector records, physical search indexes, and usage events.

Examples:

- rule state: active, building, stale, disabled, failed;
- dirty counts;
- record counts;
- skipped policy count;
- credential resolution failure count;
- last backfill/refresh/error;
- per-rule and per-binding usage summaries.

These views can be stored for fast UI reads, but they must not be the sole
source of truth.

## Caching design

Caching is important because semantic maintenance and search can be hot paths.
All caches must be safe to drop and rebuild.

### Rule cache

Cache compiled rule definitions by space/domain:

```text
(space_id, domain_id) -> enabled rules
```

Cache contents:

- normalized trigger policies;
- compiled/validated selector plans;
- compiled/validated source/context queries;
- embedding binding resolution hints.

Invalidation:

- rule create/update/delete;
- inference profile changes that affect a binding;
- vector store/model/endpoint/capability changes;
- WAL/raft applied command for semantic metadata.

### Target-resolution cache

Optional short-lived cache:

```text
(rule_id, graph_revision, changed_node_id) -> target_node_ids
```

Use only for analyzer batches. Never persist as authority.

Invalidation:

- end of analyzer pass;
- graph revision changes beyond the batch;
- rule changes.

### Source-hash cache

Cache assembled source hashes to skip embedding calls quickly:

```text
(rule_id, binding_key, target_node_id, graph_revision) -> source_hash
```

This cache is advisory. The authoritative skip check remains the latest vector
record/source hash.

### Latest-record cache

For `mycel-file`, maintain an in-memory latest-record map loaded from append-only
segments:

```text
(rule_id, binding_key, target_node_id, source_mode, vector_space_key) -> latest record
```

This avoids scanning all records for every backfill item. It can be rebuilt from
segments on startup. It should be scoped by space and invalidated/touched on
append/tombstone/purge.

### Vector search index cache

Semantic search should use a per-space/per-rule/per-binding search cache instead
of scanning vector record segments on every request.

Cache contents:

- latest live record metadata;
- normalized vectors or mmap handles;
- optional approximate nearest-neighbor graph/shards;
- record ID -> target node ID lookup;
- vector-space key and dimensions.

Invalidation/update:

- vector upsert appends update the cache entry for that target/binding;
- tombstones remove the target from the searchable live set;
- rule/binding disable removes it from query planning;
- purge deletes both durable records and physical search-index cache;
- daemon startup lazily loads or rebuilds search indexes before first query.

The search path must have a bounded fallback. If a physical index is missing, the
daemon may rebuild it synchronously for small indexes or return a clear
`FailedPrecondition`/degraded warning for large indexes rather than silently doing
an unbounded full scan.

### Search result cache

Semantic search may cache vector segment manifests, latest-record maps, and
physical search-index handles, but should not cache query embeddings by default
because access policy, profile resolution, and token accounting must happen per
request. A future short-lived query embedding cache is possible only if it
preserves per-request usage and policy semantics.

### Usage summary cache

Console/API usage summaries can use materialized rollups by:

```text
(space_id, domain_id, semantic_rule_id, binding_key, profile_id, model_id, time_bucket)
```

Rollups must be derived from usage events and rebuildable.

## Runtime pipeline

### Change ingestion

Graph commits continue to emit semantic-neutral graph change events.

Semantic dirty append should remain synchronous after commit but must not make
the graph commit fail on append failure. Instead, semantic maintenance is marked
degraded and operators can analyze/recover.

### Analysis

Analyzer steps:

1. Load enabled rules for changed space/domain from rule cache.
2. Cheaply apply trigger policy.
3. Resolve target nodes with selector.
4. For each target and enabled binding, upsert a dirty work item keyed by:

   ```text
   rule_id + binding_key + target_node_id
   ```

5. Apply dirty cooldown/debounce.
6. Save per-rule/binding checkpoint.

### Worker

Worker steps:

1. Claim ready work with lease.
2. Re-check rule and binding are still enabled.
3. Assemble source.
4. If source is empty/below minimum, tombstone latest record if needed.
5. Resolve inference profile, endpoint, model, capability, credential grant, and
   policy decision.
6. If latest record has same source hash/binding, complete as skipped.
7. Generate embedding.
8. Upsert vector record.
9. Emit usage event.
10. Complete or fail/retry work item.

### Search

Search steps:

1. Authorize domain read.
2. Select enabled bindings with search purpose.
3. Resolve inference profile/access for query embedding.
4. Generate query embedding and record usage.
5. Search the physical per-binding vector index for top-k record candidates.
6. Merge/rank candidates across selected bindings.
7. Load visible graph nodes.
8. Return results and warnings.

## Removing/replacing old mechanism parts

The following existing concepts should be removed or renamed because they no
longer fit the semantic-generation-rule model. Since the product is unreleased,
prefer direct replacement over compatibility layers.

### Remove direct endpoint/model ownership from user-facing semantic indexes

Current fields:

```go
ModelEndpointID
ModelID
ModelEndpointCapabilityID
```

Target behavior:

- user-facing rules reference inference profiles in embedding bindings;
- endpoint/model/capability are resolved at runtime through the profile/catalog;
- stored records keep resolved endpoint/model/capability for provenance.

Implementation direction:

- remove these fields from user-authored rule definitions;
- require profile-backed embedding bindings;
- keep resolved endpoint/model/capability only on generated vector records and
  usage events as provenance.

### Replace `Purpose` on the index with binding purpose

A rule can maintain multiple embeddings for different purposes. Purpose belongs
on the embedding binding, not the whole rule.

Implementation direction:

- remove index-level purpose from the rule model;
- require purpose on every embedding binding.

### Replace advisory `RecordTypes` with explicit selectors

Current `SemanticSourcePolicy.RecordTypes` is advisory and not enforced.

Target behavior:

- use `selector.mode = node_type` with labels/types;
- enforce during analysis and backfill;
- remove advisory-only semantics.

### Replace `RootQuery` with bounded selector/context GQL

Current `RootQuery` is too underspecified.

Target behavior:

- selector GQL returns target alias;
- context/source GQL, if used, assembles source text;
- both must be read-only, bounded, validated, and fail closed.

### Split source assembly from target selection

Current source policy mixes root selection and source assembly.

Target behavior:

- selector chooses targets;
- source policy assembles text for each target.

This matches graph automation's split between trigger/condition/context/action.

### Make work items binding-aware

Current dirty work is keyed around semantic index + target.

Target behavior:

- dirty work is keyed by rule + embedding binding + target;
- one target can produce multiple independent embeddings.

### Remove `SemanticIndex` as product/API terminology

User-facing UI/docs and new API names should use:

```text
Semantic generation rule
```

`SemanticIndex` should not remain as a public protobuf, CLI, or storage concept
for the new design. Keep "index" only for low-level vector-index implementation
details where it describes a physical/search structure rather than a user rule.

## API implications

New Admin API concepts should expose:

- list semantic generation rules;
- get semantic generation rule;
- validate semantic generation rule;
- create/update semantic generation rule;
- enable/disable rule;
- delete rule with reference/vector purge options;
- list rule status and work items;
- backfill rule or binding;
- summarize usage by rule/binding.

Existing semantic index RPCs/CLI commands should be replaced or renamed rather
than maintained as compatibility aliases.

## Console implications

The console `Intelligence / Semantic` page should become the canonical semantic
rule management surface.

It should support:

- list rules across spaces/domains;
- status/backlog/failure overview;
- token usage by rule and binding;
- create/edit structured rules;
- select node type/labels or bounded GQL selector;
- configure source assembly;
- configure embedding bindings from available inference profiles/vector stores;
- validate before save;
- explicit backfill/analyze/process actions.

Space pages should keep contextual shortcuts:

- create semantic rule for this domain;
- open global Semantic page filtered to this space/domain.

## Replacement strategy

1. Replace `SemanticIndex` model/storage with `SemanticGenerationRule` and
   binding-aware vector record metadata.
2. Delete or rewrite direct endpoint/model fields, advisory record types, and
   ambiguous root query behavior in the same tranche.
3. Update analyzer/work items to include rule ID and binding key/ID.
4. Update backfill/search to resolve embeddings exclusively through binding
   inference profiles.
5. Replace Admin/Client APIs and CLI docs with semantic rule terminology.
6. Update console to create/edit structured semantic generation rules.
7. Regenerate or delete old generated public API code only when explicitly
   approved under repository rules.

## Open questions

- Should selectors be limited to labels/node types in the first tranche, with GQL
  selectors enabled later after validation is shared with graph automations?
- Should semantic rule ownership default to the space owner, the creator, or an
  explicit service principal?
- Should multiple bindings share one source assembly result per target during a
  worker pass?
- Should vector records store binding key only, binding UUID only, or both?
- How much of rule status should be precomputed versus derived on demand for the
  console?

## Acceptance criteria for the target design

- A user can define semantic indexing with a node type or bounded GQL selector.
- A user can configure the set of embedding bindings without raw credentials.
- Semantic maintenance uses the same inference access-control path as graph
  automations.
- Work items and usage are attributable to rule and embedding binding.
- Vector records are idempotent by source hash and binding.
- Semantic search uses physical per-binding search indexes or bounded rebuilds,
  not unbounded full scans of historical vector records.
- Caches and physical search indexes are rebuildable and safely invalidated on
  graph/rule/inference/vector changes.
- Old semantic index terminology and storage fields are removed instead of
  preserved as compatibility aliases.
