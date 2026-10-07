# SGR5 Semantic Generation Rules Source Assembly and Embedding Generation Plan

## Status

Implemented for rule-native node-type/explicit-node backfill and worker paths.
GQL selector/source execution remains fail-closed until a safe graph transaction
execution context is wired.

## Goal

Rewrite semantic backfill and maintenance worker generation so embeddings are
created from `SemanticGenerationRule` + `SemanticEmbeddingBinding` rather than
from legacy `SemanticIndex` endpoint/model/vector-store fields.

SGR5 should make rule/binding work items executable: select targets, assemble
source text from the rule source policy, invoke embeddings through the binding's
Intelligence Access profile, and write vector records with rule/binding
attribution plus resolved provider/access provenance.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Semantic backfill, worker, connector input attribution, source assembly adapters, vector record write path, tests. |
| `mycel-api` | Out of scope unless SGR0 fields are found insufficient. |
| `mycel-console` | Out of scope. |
| `mycel-rust-sdk` | Out of scope. |

## Primary files

```text
internal/semantic/backfill/types.go
internal/semantic/backfill/runner.go
internal/semantic/maintenance/analyzer.go
internal/semantic/maintenance/worker_test.go
internal/semantic/connectors/types.go
internal/semantic/connectors/inference_adapter.go
internal/semantic/vectorstore/mycel_file.go
internal/semantic/vectorstore/interface.go
internal/semantic/model/semantic.go
internal/embedding/*
internal/inference/service/resolve.go
internal/inference/service/invoke.go
internal/inference/model/types.go
```

Likely tests:

```text
internal/semantic/backfill/*_test.go
internal/semantic/maintenance/*_test.go
internal/semantic/connectors/*_test.go
internal/semantic/vectorstore/*_test.go
internal/inference/service/*_test.go
```

## Current behavior

Current backfill path:

1. `backfill.Input` carries `SemanticIndexID`.
2. Runner loads `ListSemanticIndexes`.
3. Index directly owns:
   - `ModelEndpointID`;
   - `ModelID`;
   - `ModelEndpointCapabilityID`;
   - `VectorStoreID`;
   - `SemanticSourcePolicy`.
4. Source is assembled with legacy `SourceExtraction` and `IncludeProps`.
5. Connector input is attributed to `SemanticIndexID`.
6. Vector records are written as `AdvancedEmbeddingRecord` keyed by
   `SemanticIndexID` and provider IDs.

This is incompatible with rule/binding semantics because the user-authored rule
must not own endpoint/model/capability IDs.

## Target behavior

Target flow:

```text
work item / backfill request
  -> load semantic rule
  -> select enabled binding(s)
  -> select target nodes
  -> assemble source text from rule source policy
  -> resolve embedding access through Intelligence Access profile
  -> invoke embedding provider
  -> write vector record with rule + binding + provenance
  -> update rule/search state
```

## Data model/API shape

### Backfill input

Update `internal/semantic/backfill/types.go`:

```go
type Input struct {
    SpaceID             domainspace.SpaceID
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string

    // Transitional until API/CLI adapters are replaced.
    SemanticIndexID domainsemantic.SemanticIndexID

    NodeIDs         []graph.NodeID
    Force           bool
    Limit           int
    ContinueOnError bool
}
```

### Backfill result

Add rule/binding attribution while retaining transitional fields:

```go
type Result struct {
    SemanticRuleID      domainsemantic.SemanticRuleID `json:"semantic_rule_id"`
    EmbeddingBindingKey string                       `json:"embedding_binding_key,omitempty"`
    SemanticIndexID     domainsemantic.SemanticIndexID `json:"semantic_index_id,omitempty"`
    SelectedCount       int
    GeneratedCount      int
    SkippedCount        int
    FailedCount         int
    Records             []domainsemantic.AdvancedEmbeddingRecord
    Skipped             []Skipped
    Failures            []Failure
}
```

### Connector input

Update `connectors.EmbedInput`:

```go
type EmbedInput struct {
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string

    // Transitional mirror for current inference usage paths.
    SemanticIndexID domainsemantic.SemanticIndexID

    InferenceProfile   string
    InferenceProfileID domainsemantic.IntelligenceProfileID
    // resolved endpoint/model/capability fields remain optional provenance
}
```

The connector/inference adapter should resolve via profile first. Endpoint/model
fields may remain as overrides only for old semantic-index fallback paths and
must not be required for rule-native calls.

## Rule and binding selection

Backfill runner should:

1. load `ListSemanticRules`;
2. find `SemanticRuleID` or map transitional `SemanticIndexID` to rule ID when
   needed;
3. reject disabled rule;
4. select enabled bindings:
   - all enabled bindings when `EmbeddingBindingKey` is empty;
   - only matching binding when set;
5. reject missing binding with actionable error.

If no rules exist and `SemanticIndexID` is set, keep the old index path as a
transitional fallback until SGR8 removes old API adapters.

## Target selection

For SGR5, target selection may reuse SGR4 selector assumptions:

- explicit `NodeIDs` in backfill input narrow the target set;
- node-type selector filters `ListNodes` by labels;
- explicit-node selector selects listed node IDs;
- GQL selector execution can remain follow-up if SGR4 left it unimplemented, but
  fail with a clear error instead of silently scanning all nodes.

Do not use unbounded vector or graph scans beyond existing bounded domain node
listing. If rule selector cannot be evaluated safely, fail closed.

## Source assembly

Implement rule-native source assembly adapter:

```go
type RuleSourceInput struct {
    Rule   domainsemantic.SemanticGenerationRule
    Target graph.Node
    Nodes  []graph.Node
    Edges  []graph.Edge
}
```

Map source policy:

| Rule source mode | Behavior |
| --- | --- |
| `self` | Assemble text from target node only. |
| `subtree` | Assemble text from target plus contained descendants. |
| `context_query` | Future/fail-closed unless a bounded execution context is available. |

Apply:

- `IncludeProperties`;
- `ExcludeProperties`;
- `MaxDepth`;
- `MinimumTextLength`.

If `source.Text` is empty or shorter than `MinimumTextLength`:

- tombstone latest live record for the same rule/binding/target/source mode when
  present;
- return a skipped result with reason `source_below_minimum_text_length`.

## Intelligence Access resolution

Rule-native embedding invocation must use binding profile references:

```text
binding.IntelligenceProfile or binding.IntelligenceProfileID
```

The request should set:

- operation: embeddings;
- usage mode: semantic;
- space/domain/node;
- semantic rule ID;
- embedding binding key;
- actor principal: daemon/system semantic worker;
- on-behalf-of principal: rule owner when present;
- allow background use requirement via existing grant/policy resolution.

Current inference service types still use inference naming and `SemanticIndexID`.
SGR5 should update internal inference model/service minimally to carry:

- `SemanticRuleID`;
- `EmbeddingBindingKey`;
- `IntelligenceProfileID` alias/profile ID;
- usage event attribution fields.

If a full Intelligence Access package split is too large, add fields to existing
`internal/inference/model.Scope`, `ResolveRequest`, and `UsageEvent` as a
transitional implementation, with TODOs for the dedicated Intelligence Access
subsystem.

## Vector record write path

Write records with rule/binding fields populated:

```go
domainsemantic.AdvancedEmbeddingRecord{
    SemanticRuleID: rule.ID,
    EmbeddingBindingKey: binding.Key,
    SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), // transitional mirror
    TargetNodeID: target.ID,
    NodeID: target.ID,
    IntelligenceProfileID: binding.IntelligenceProfileID,
    ModelEndpointID: resp.EndpointID,
    ModelID: resp.ModelID,
    ModelEndpointCapabilityID: resp.CapabilityID,
    CredentialID: resp.CredentialID,
    CredentialGrantID: resp.CredentialGrantID,
    PolicyDecisionID: resp.PolicyDecisionID,
    VectorStoreID: binding.VectorStoreID,
    VectorSpaceKey: resolved model vector space,
    SourceMode: string(rule.Source.Mode),
    SourceHash: source.Hash,
}
```

Update `vectorstore` freshness/idempotency matching to include:

```text
semantic_rule_id + embedding_binding_key + target_node_id + source_mode + vector_space_key
```

For transitional compatibility, existing paths may continue to use
`SemanticIndexID` directories, but records must include rule/binding fields.
Physical layout rewrite and fast latest-live indexes remain SGR6.

## Idempotency and tombstones

Latest record lookup should match:

- rule ID;
- binding key;
- target node ID;
- source mode;
- vector store;
- vector space key;
- resolved model/capability if necessary for correctness.

Skip provider invocation when:

- latest non-tombstone record exists;
- `SourceHash` matches;
- `Force` is false.

Append tombstone when:

- source falls below minimum length;
- latest non-tombstone record exists for the same rule/binding/target.

## Worker integration

Update `Worker.runItem`:

- prefer `item.SemanticRuleID` and `item.EmbeddingBindingKey`;
- call backfill with rule/binding fields;
- keep `SemanticIndexID` fallback only for old work items;
- delete/cleanup should tombstone per rule/binding when available.

## State updates

Backfill should update or allow worker to update:

- `SemanticRuleState.LastBackfillAt` / `LastRefreshAt`;
- `SemanticRuleState.RecordCount` where available;
- `SemanticSearchIndexState` for binding status/backlog if practical.

If accurate counts require SGR6 latest-live indexes, store coarse status only and
leave count accuracy to SGR6.

## Tests

### Backfill tests

Cover:

1. rule + binding backfill invokes connector using Intelligence Access profile;
2. endpoint/model/capability are not required on the rule;
3. all enabled bindings run when no binding key is supplied;
4. binding key filter runs one binding;
5. disabled binding is skipped;
6. source `self` includes target text/properties;
7. source `subtree` respects max depth;
8. include/exclude property filters affect source hash;
9. minimum text length tombstones existing latest record;
10. unchanged source hash skips provider call unless force=true;
11. vector records include semantic rule ID and embedding binding key;
12. transitional `SemanticIndexID` fallback still passes existing tests.

### Worker tests

Cover:

- refresh work item with rule/binding calls backfill with rule/binding;
- backfill action with target equal to rule ID handles full-rule backfill if that
  convention is retained;
- delete work item tombstones rule/binding target records;
- old work item with only `SemanticIndexID` still works transitionally.

### Inference/access tests

Cover:

- usage events include semantic rule and binding attribution;
- policy denial is surfaced as a semantic failure/skip with sanitized reason;
- background credential grants are honored.

## Validation commands

Minimum:

```sh
go test ./internal/semantic/backfill ./internal/semantic/maintenance ./internal/semantic/connectors -count=1
go test ./internal/inference/... -count=1
git diff --check
```

Preferred:

```sh
go test ./internal/semantic/... ./internal/inference/... -count=1
make docs-check
```

Do not run destructive Compose/K3s cluster tests for SGR5.

## Implementation sequence

1. Extend backfill input/result and connector input with rule/binding fields.
2. Add rule/binding lookup helpers in backfill runner.
3. Implement rule-native target selection for node-type and explicit-node
   selectors.
4. Implement rule-native source assembly adapter for `self` and `subtree`.
5. Update connector/inference adapter to resolve embeddings through binding
   Intelligence Access profile fields.
6. Populate rule/binding/provenance fields on vector records and tombstones.
7. Update latest-record/idempotency matching to include rule and binding.
8. Update worker refresh/delete paths to prefer rule/binding fields.
9. Add tests and preserve transitional index-path tests.
10. Run validation and mark this plan implemented.

## Acceptance criteria

SGR5 is complete when:

- backfill can generate embeddings from semantic rules and binding keys;
- user-authored endpoint/model/capability IDs are not needed for rule-native
  generation;
- source text is assembled from rule source policy;
- empty/short source text tombstones latest matching records;
- unchanged source hashes skip provider calls unless force is set;
- vector records and usage include semantic rule and embedding binding
  attribution;
- worker processes rule/binding dirty work from SGR4;
- old semantic-index path remains only as a marked transitional fallback;
- tests pass for semantic backfill, maintenance worker, connectors, and
  inference attribution.

Implemented notes:

- Backfill input/result now carry `semantic_rule_id` and
  `embedding_binding_key`, while preserving transitional `semantic_index_id`.
- Rule-native backfill loads semantic rules, selects enabled bindings, filters
  node-type/explicit targets, assembles `self`/`subtree` sources, and invokes the
  connector through binding Intelligence Access profile references.
- Worker refresh/backfill paths now pass rule/binding identity to backfill.
- Vector records and tombstones persist rule ID, binding key, target node, and
  Intelligence Access profile provenance in record metadata.
- Internal inference resolution/usage now carries semantic rule and embedding
  binding attribution alongside transitional semantic-index fields.
- GQL selector/source execution still fails closed for generation; validation is
  already enforced by SGR3.

## Risks and follow-ups

- Current `internal/inference` types still use inference naming and
  `SemanticIndexID`; adding rule/binding fields may ripple through admin usage
  APIs. Keep changes internal and transitional if possible.
- GQL source/selector execution may require transaction context not available to
  backfill; fail closed for unsupported modes until a safe executor is wired.
- Physical latest-live indexes are SGR6; SGR5 may still rely on current record
  listing for idempotency, but must not introduce new unbounded search behavior
  beyond current transitional implementation.
- Full Intelligence Access package extraction remains a later refactor if SGR5
  only extends existing inference service internals.
