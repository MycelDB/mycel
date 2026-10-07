# SGR7 Semantic Generation Rules Search Planner Plan

## Status

Implemented. This tranche follows SGR6 physical per-rule/per-binding search indexes.

## Goal

Rewrite semantic search so query planning is rule-native, binding-aware, and
fast-index-backed. The planner should select enabled semantic generation rule
bindings with semantic-search purpose, resolve query embeddings through
Intelligence Access, search the SGR6 physical indexes, merge/rank candidates,
and only then load visible graph nodes.

SGR7 should remove semantic search's dependency on legacy `SemanticIndex`
planner assumptions while preserving transitional API adapters until SGR8
replaces public Admin/Client semantic APIs.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Search planner, semantic service search input/result types, client adapter mapping/tests, vectorstore search integration tests. |
| `mycel-api` | Out of scope unless generated client API fields are explicitly approved for this tranche. |
| `mycel-console` | Out of scope. |
| `mycel-rust-sdk` | Out of scope. |

## Primary files

```text
internal/semantic/search/types.go
internal/semantic/search/planner.go
internal/semantic/search/*_test.go
internal/semantic/service/types.go
internal/semantic/service/module.go
internal/semantic/storage/interface.go
internal/semantic/model/semantic.go
internal/semantic/vectorstore/types.go
internal/daemon/api/client/semantic_service.go
internal/daemon/api/client/semantic_service*_test.go
```

Likely new files:

```text
internal/semantic/search/rule_selection.go
internal/semantic/search/rule_selection_test.go
internal/semantic/search/ranking.go
internal/semantic/search/ranking_test.go
```

## Current behavior

The current planner is transitional. It:

1. lists legacy semantic indexes;
2. resolves endpoint/model/vector-store directly from index fields;
3. embeds the query using index metadata for an inference profile;
4. searches the vector backend by `SemanticIndexID`;
5. returns index-oriented provenance.

This does not match the target SGR model because endpoint/model are not
user-authored rule fields, purpose is binding-scoped, and rule-native search
must use per-rule/per-binding physical indexes.

## Target behavior

Search planning should follow this pipeline:

```text
request -> visible domain authorization -> rule/binding selection ->
Intelligence Access query embedding -> physical index search -> candidate merge ->
visible graph node load -> response mapping
```

Planner inputs should support rule-native selection:

```go
type Input struct {
    SpaceID             domainspace.SpaceID
    DomainID            graph.DomainID
    SemanticRuleIDs     []domainsemantic.SemanticRuleID
    EmbeddingBindingKey string
    Purpose             domainsemantic.SemanticIndexPurpose // transitional type until renamed
    Text                string
    Limit               int
    MinScore            float64
    ActorPrincipalID    identity.PrincipalID
}
```

`SemanticIndexIDs` may remain as transitional adapter fields only where needed
for existing generated client APIs. Internally, rule/binding selection should be
primary.

## Rule and binding selection

Select from `SpaceManager.ListSemanticRules` rather than `ListSemanticIndexes`.

A binding is searchable when all of the following are true:

- rule is enabled;
- rule `SpaceID` and `DomainID` match the request;
- binding is enabled;
- binding purpose normalizes to semantic search;
- binding has an Intelligence Access profile reference or profile ID;
- binding references an enabled `mycel-file` vector store;
- optional request filters match:
  - selected rule IDs;
  - selected binding key;
  - transitional semantic-index ID filter, if present.

Selection output should preserve enough information for grouping and warnings:

```go
type selectedBinding struct {
    Rule    domainsemantic.SemanticGenerationRule
    Binding domainsemantic.SemanticEmbeddingBinding
    Store   domainsemantic.VectorStoreBackend
}
```

Warnings should be structured enough for API/console display, even if the
transitional public response still exposes strings:

```go
type Warning struct {
    Code                string
    Message             string
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string
    Retryable           bool
}
```

Initial codes:

```text
binding_disabled
profile_missing
profile_denied
vector_store_missing
vector_store_unsupported
search_index_missing
search_index_degraded
embedding_failed
```

## Query embedding resolution

For each compatible binding group, resolve the query embedding through the same
Intelligence Access path used by SGR5 generation:

- service actor;
- request actor as on-behalf-of principal where applicable;
- binding profile ref/ID;
- background/search operation attribution;
- policy and credential grant checks;
- usage event attribution to semantic rule ID and binding key.

The planner must not use user-authored endpoint/model IDs from semantic rules.
Endpoint/model/capability are resolved outputs from Intelligence Access and the
inference catalog.

Group query embedding calls by profile/vector-space when safe. Do not merge
bindings that require different profile/access decisions.

Recommended grouping key:

```text
profile_ref/profile_id + vector_store_id + vector_space_key + operation
```

Even if a query embedding result cache is added later, usage/accounting should
still record the search request according to Intelligence Access policy.

## Physical index search

For each selected binding, call the SGR6 rule-native vector backend path:

```go
vectorstore.SearchInput{
    SpaceID:             rule.SpaceID,
    DomainID:            rule.DomainID,
    SemanticRuleID:      rule.ID,
    EmbeddingBindingKey: binding.Key,
    VectorStoreID:       binding.VectorStoreID,
    VectorSpaceKey:      resolved.VectorSpaceKey,
    Query:               query.Vector,
    Limit:               perBindingLimit,
    MinScore:            in.MinScore,
}
```

Rules:

- Do not fall back to legacy historical vector scans for rule-native searches.
- Missing/degraded physical indexes should produce skipped-binding warnings.
- Search may continue when some bindings fail and at least one binding returns
  successfully.
- If every selected binding is skipped/failed, return a clear failed-precondition
  or planner error with warning details.

Use an over-fetch per binding so cross-binding ranking can produce stable top-N:

```text
per_binding_limit = max(request_limit, min(request_limit * 3, 100))
```

Keep this conservative and bounded.

## Candidate merge and ranking

Merge candidates from all searched bindings by:

1. score descending;
2. stable tie-break by target node ID;
3. stable tie-break by rule ID and binding key;
4. newest record timestamp if available.

Duplicate target nodes can appear from multiple bindings. Preserve provenance for
all matching records, but choose a primary result score as the best candidate
score for that node.

Recommended internal shape:

```go
type SearchResult struct {
    SemanticRuleID       domainsemantic.SemanticRuleID
    EmbeddingBindingKey  string
    SemanticIndexID      domainsemantic.SemanticIndexID // transitional response only
    NodeID               graph.NodeID
    TargetNodeID         graph.NodeID
    RecordID             domainsemantic.AdvancedEmbeddingRecordID
    Score                float64
    MatchedRecordIDs     []domainsemantic.AdvancedEmbeddingRecordID
    MatchedBindings      []MatchedBinding
    ModelEndpointID      domainsemantic.ModelEndpointID
    ModelID              domainsemantic.InferenceModelID
    VectorStoreID        domainsemantic.VectorStoreID
    CredentialGrantID    domainsemantic.CredentialGrantID
    VectorSpaceKey       string
    SourceHash           string
    SourceMode           string
}
```

If public generated APIs cannot yet expose rule/binding fields, keep the fields
internal and map only available data in `internal/daemon/api/client` until SGR8.

## Visible graph node loading

The planner should return candidate node IDs and provenance only. The client API
adapter remains responsible for loading visible graph nodes after candidate
selection, using the existing domain-read authorization path.

SGR7 should tighten this behavior:

- load each node once;
- preserve result order after node visibility filtering;
- append a warning when a candidate node is no longer visible or no longer
  exists;
- never load graph nodes before vector candidate selection.

## Transitional API behavior

Until SGR8 regenerates/replaces public APIs:

- `SemanticSearchRequest.semantic_index_id` may map to a rule ID for
  transitional compatibility if necessary;
- response `MatchedChunkIds` may continue to carry record IDs;
- warning strings may encode structured warning code/details;
- internal result structs should already carry rule/binding provenance.

Do not add long-term compatibility aliases or new public generated code unless
explicitly approved.

## Error handling

Recommended planner behavior:

- invalid input: hard error;
- no enabled searchable bindings: failed-precondition error;
- profile/access denial for one binding: warning and skip binding;
- vector store unsupported/missing: warning and skip binding;
- physical index missing/degraded: warning and skip binding;
- all selected bindings skipped: hard error containing warning summary;
- partial failures with at least one successful binding: return results plus
  warnings.

Access denials should be visible but sanitized. Do not leak credential material
or provider secrets.

## Tests

Add or update tests covering:

- selecting only enabled search-purpose bindings;
- non-search-purpose bindings are ignored;
- disabled rules and disabled bindings are ignored;
- missing profile/profile denial produces a warning;
- query embedding uses binding profile/access fields, not endpoint/model fields;
- vector backend receives `SemanticRuleID` and `EmbeddingBindingKey`;
- missing/degraded physical index skips one binding without scanning history;
- multiple bindings merge/rank deterministically;
- duplicate target nodes preserve best score and matched provenance;
- all bindings skipped returns a failed-precondition-style error;
- client adapter loads visible graph nodes after candidate selection and carries
  warnings forward.

## Implementation steps

1. Add rule-native search input/result fields in `internal/semantic/search` and
   `internal/semantic/service`.
2. Implement rule/binding selection from `ListSemanticRules`.
3. Add binding grouping and Intelligence Access query embedding calls.
4. Replace planner vector search calls with SGR6 physical index inputs.
5. Implement candidate merge/ranking and warning collection.
6. Update semantic service and client adapter mappings while keeping generated
   API compatibility for now.
7. Add planner and client adapter tests.
8. Run validation and `git diff --check`.

## Acceptance

- Semantic search is backed by SGR6 physical per-rule/per-binding indexes.
- Planner no longer depends on legacy `SemanticIndex` definitions for normal
  rule-native search.
- Query embeddings resolve through binding Intelligence Access profiles.
- Search results include internal rule/binding/record provenance.
- Skipped or degraded bindings are reported as structured warnings/errors.
- Candidate ranking is deterministic across multiple bindings.

## Validation

```sh
go test ./internal/semantic/search ./internal/daemon/api/client -count=1
go test ./internal/semantic/... -count=1
make docs-check
git diff --check
```

## Out of scope

- Public protobuf/API replacement; covered by SGR8.
- CLI replacement; covered by SGR9.
- Console rule authoring/search health UI; covered by SGR10.
- ANN/HNSW vector indexing; SGR6 intentionally keeps the first physical index
  latest-live and rebuildable.
- External vector database integration.
