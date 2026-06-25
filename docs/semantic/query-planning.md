# Semantic Query Planning

A semantic query may target one semantic index or many semantic indexes.

If selected indexes use different `vector_space_key` values, Mycel must generate multiple query embeddings and merge results. `vector_space_key` is an opaque authoritative string: equal keys are directly comparable, different keys are not directly comparable.

## Planning Direction

```text
query scope
  -> applicable semantic indexes
  -> policy filtering
  -> compatible vector-space groups
  -> credential resolution for model endpoint calls
  -> vector searches
  -> merge/rank
```

Credential grants do not define the search space. They only authorize model endpoint calls required by selected indexes.

## Planner Steps

Mycel should:

1. resolve requested space/domain/index/content scope
2. find semantic indexes covering that scope and purpose
3. evaluate inference policies and remove disallowed content/index/model endpoint combinations
4. group remaining indexes by `vector_space_key` and compatible endpoint/model requirements
5. resolve a compatible credential grant for each required query-embedding model endpoint call
6. generate one query embedding per compatible vector-space group
7. search each vector store/index
8. merge and rank results
9. return provenance and warnings

Returned provenance should include:

```text
semantic_index_id
model_endpoint_id
model_id
vector_store_id
credential_grant_id, if a model endpoint call was required
policy_decision_id, when applicable
record_id
node_id
score
```

## Multi-Index Example

A domain may contain:

```text
notes-openai:
  model: text-embedding-3-small
  model_endpoint: openai-public
  source: journals/pages

private-notes-local:
  model: nomic-embed-text
  model_endpoint: local-ollama
  source: private-tagged journals

tasks-azure:
  model: azure-embedding-v2
  model_endpoint: azure-openai
  source: app.task
```

A broad query over the domain may need:

```text
1 query embedding for OpenAI small
1 query embedding for local Nomic
1 query embedding for Azure embedding v2
```

Then Mycel searches each compatible index group and merges results.

## Policy Effects

Policy may exclude part of the graph or entire indexes.

Examples:

- a `local_only` subtree is excluded from third-party indexes
- a `no_inference` subtree is excluded from all semantic indexes
- a query may return partial results with warnings for skipped content/indexes

Warnings should be explicit:

```text
private subtree skipped for openai-notes due to local_only policy
index tasks-azure skipped because no credential grant was available
```

## Score Merging

Cosine scores from different `vector_space_key` groups should not be compared blindly.

Options:

- return grouped results by index/vector space
- normalize scores by index-specific calibration
- perform lexical/vector hybrid reranking
- call a reranker model over candidate result text

The MVP can start with conservative grouping or simple score normalization, but the architecture should not assume one universal vector space.

## Missing Credentials

If a query requires a model endpoint call but no credential grant resolves:

- skip the affected index/group
- return a warning
- do not silently fall back to an unauthorized credential

Example:

```text
index private-notes-local skipped: missing credential for local-ollama embeddings
```

## Stale and Latest Records

Search should use the latest logical embedding record for each semantic index/node/source identity.

Old records remain physically present in append-only storage until compaction, but should not remain semantically searchable after a newer record supersedes them.

## Current MVP

The current MVP performs a simpler flow:

```text
query text -> selected profile/provider/model -> one query embedding -> brute-force cosine search
```

Advanced planning generalizes this to multiple semantic indexes and vector spaces.
