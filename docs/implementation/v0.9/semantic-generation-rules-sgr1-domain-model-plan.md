# SGR1 Semantic Generation Rules Domain Model Plan

## Status

Implemented. This tranche follows SGR0 source API work and keeps the daemon
functional by adding the rule-native domain model while retaining transitional
aliases for storage/runtime code that will be replaced in later SGR tranches.

## Goal

Replace the internal `SemanticIndex`-centered semantic model with a
`SemanticGenerationRule` model and introduce the supporting rule, binding,
record, work, and Intelligence Access attribution identifiers needed by later
storage, analyzer, generation, and search tranches.

SGR1 is a **domain-model tranche**, not a storage/runtime rewrite. It should make
new types available and adapt narrow call sites only as needed to keep tests
compiling.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Internal Go domain model, validation helpers, small compile adapters, model tests. |
| `mycel-api` | No additional source-proto changes expected unless SGR1 finds an SGR0 field gap. |
| `mycel-console` | Out of scope. |
| `mycel-rust-sdk` | Out of scope. |

## Current model issues

Current semantic model file:

```text
internal/semantic/model/semantic.go
```

It currently mixes four concepts:

1. inference catalog resources;
2. Intelligence Access resources, currently named as inference credentials,
   grants, policies, profile/usage state in adjacent packages;
3. semantic index definitions;
4. vector records, dirty work, index state, and policy decisions.

Problematic semantic-index-specific fields include:

- `SemanticIndexID`;
- `SemanticIndexPurpose`;
- `SemanticSourcePolicy.RootQuery`;
- `SemanticSourcePolicy.RecordTypes`;
- `SemanticIndex.ModelEndpointID`;
- `SemanticIndex.ModelID`;
- `SemanticIndex.ModelEndpointCapabilityID`;
- `SemanticIndex.VectorStoreID` as a single index-level binding;
- `AdvancedEmbeddingRecord.SemanticIndexID`;
- `SemanticDirtyWorkItem.SemanticIndexID`;
- `SemanticIndexState.SemanticIndexID`;
- `InferenceUsageEvent.SemanticIndexID`.

SGR1 should remove these from the new model surface. Transitional compatibility
aliases may exist only where required to keep untouched SGR2+ runtime compiling,
but new code and tests must use rule terminology.

## Design decisions

### Semantic model owns semantic rules only

`internal/semantic/model` should own:

- semantic rule IDs and structures;
- trigger/selector/source policies;
- embedding binding definitions;
- semantic vector record provenance;
- semantic dirty work and rule/search-index state.

It should not own user-authored provider endpoint/model/capability selection.
Rules reference Intelligence Access profiles and vector stores through bindings.
Resolved endpoint/model/capability IDs remain provenance on generated records and
usage events.

### Intelligence Access is its own concept

SGR1 should prepare the internal model boundary for Intelligence Access:

- semantic rules reference `IntelligenceProfileID` or profile key in bindings;
- semantic records/work/usage attribution carry `semantic_rule_id` and
  `embedding_binding_key`;
- credentials/grants/policies/decisions are not modeled as semantic-rule-owned
  objects.

If a new package is introduced, use:

```text
internal/intelligence/access/model
```

for Intelligence Access identifiers and structs. If that package causes too much
compile churn in SGR1, use semantic-local type aliases first and move package
ownership in a dedicated follow-up tranche before runtime rewrites.

### Preserve functional tranches

SGR1 should not rewrite storage layouts, WAL/raft application, analyzer target
selection, embedding generation, search planning, CLI, or daemon API adapters
except for minimal compile fixes.

## Proposed model shape

### IDs

Add/rename IDs:

```go
type (
    SemanticRuleID           = uuid.UUID
    SemanticBindingID        = uuid.UUID // optional/future; binding key remains canonical now
    SemanticRecordID         = uuid.UUID
    SemanticDirtyWorkItemID  = uuid.UUID
    SemanticRuleStateID      = uuid.UUID // optional if state remains keyed by rule ID
    SearchIndexID            = uuid.UUID // optional physical index state id
)
```

Use `embedding_binding_key string` as the primary stable binding key in records,
work, and search status. Avoid requiring a generated binding UUID unless a later
storage tranche needs it.

### Rule

```go
type SemanticGenerationRule struct {
    ID          SemanticRuleID
    SpaceID     domainspace.SpaceID
    DomainID    graph.DomainID
    Key         string
    DisplayName string
    Description string
    Enabled     bool

    Trigger    SemanticTriggerPolicy
    Selector   SemanticTargetSelector
    Source     SemanticSourceAssemblyPolicy
    Embeddings []SemanticEmbeddingBinding
    Maintenance SemanticMaintenancePolicy
    Storage     SemanticStoragePolicy

    OwnerPrincipalID     identity.PrincipalID
    CreatedByPrincipalID identity.PrincipalID
    CreatedAt            time.Time
    UpdatedAt            time.Time
}
```

### Trigger policy

```go
type SemanticTriggerPolicy struct {
    Events   []string
    Labels   []string
    Debounce time.Duration
}
```

Rules:

- default event should be graph `changed` unless explicitly overridden;
- labels are normalized and cheap prefilters only;
- omitted trigger should mean all relevant changes for the selector may be
  considered by later analyzer logic.

### Target selector

```go
type SemanticTargetSelectorMode string

const (
    SemanticTargetSelectorNodeType SemanticTargetSelectorMode = "node_type"
    SemanticTargetSelectorGQL      SemanticTargetSelectorMode = "gql"
    SemanticTargetSelectorExplicit SemanticTargetSelectorMode = "explicit_nodes"
)

type SemanticTargetSelector struct {
    Mode        SemanticTargetSelectorMode
    Labels      []string
    GQL         string
    TargetAlias string
    MaxResults  int
    NodeIDs     []graph.NodeID
}
```

Rules:

- node type/label selection replaces advisory `RecordTypes`;
- GQL selector must be explicit and later SGR3 will compile/validate it;
- `TargetAlias` is required for GQL selectors;
- `MaxResults` is a hard bound, not a hint.

### Source assembly

```go
type SemanticSourceAssemblyMode string

const (
    SemanticSourceSelf         SemanticSourceAssemblyMode = "self"
    SemanticSourceSubtree      SemanticSourceAssemblyMode = "subtree"
    SemanticSourceContextQuery SemanticSourceAssemblyMode = "context_query"
)

type SemanticSourceAssemblyPolicy struct {
    Mode              SemanticSourceAssemblyMode
    IncludeProperties []string
    ExcludeProperties []string
    MaxDepth          *int
    MinimumTextLength int
    ContextGQL        string
}
```

Rules:

- replaces `RootQuery` and old source policy ambiguity;
- target selection and source assembly are separate concepts;
- context query validation/compilation is SGR3/SGR5 work.

### Embedding binding

```go
type SemanticEmbeddingBinding struct {
    Key                    string
    Purpose                string
    IntelligenceProfile    string
    IntelligenceProfileID  uuid.UUID
    VectorStore            string
    VectorStoreID          uuid.UUID
    Enabled                bool
    Metadata               map[string]any
}
```

Rules:

- purpose is binding-scoped;
- user-authored bindings reference Intelligence Access profiles and vector
  stores only;
- endpoint/model/capability IDs must not be user-authored rule fields.

### Maintenance and storage policies

```go
type SemanticMaintenancePolicy struct {
    DirtyCooldown     time.Duration
    MaxBatchSize      int
    WorkerConcurrency int
}

type SemanticStoragePolicy struct {
    Searchable     bool
    PhysicalIndex  string // exact initially
}
```

### Records

Replace `AdvancedEmbeddingRecord` with a rule/binding-oriented record shape:

```go
type SemanticVectorRecord struct {
    ID                  SemanticRecordID
    SpaceID             domainspace.SpaceID
    DomainID            graph.DomainID
    SemanticRuleID      SemanticRuleID
    EmbeddingBindingKey string
    TargetNodeID        graph.NodeID
    SourceHash          string

    IntelligenceProfileID uuid.UUID
    ModelEndpointID       uuid.UUID
    ModelID               uuid.UUID
    CapabilityID          uuid.UUID
    CredentialID          uuid.UUID
    CredentialGrantID     uuid.UUID
    PolicyDecisionID      uuid.UUID
    VectorStoreID         uuid.UUID
    VectorSpaceKey        string

    SourceMode           string
    Dimensions           int
    Vector               []float64
    Tombstone            bool
    DeleteTargetRecordID SemanticRecordID
    DeleteReason         string
    CreatedAt            time.Time
}
```

SGR1 may keep the existing vector field serialization behavior and can alias old
record names only if required for compile stability.

### Dirty work

Update dirty work to be rule/binding aware:

```go
type SemanticDirtyWorkItem struct {
    ID                  SemanticDirtyWorkItemID
    SemanticRuleID      SemanticRuleID
    EmbeddingBindingKey string
    SpaceID             domainspace.SpaceID
    DomainID            graph.DomainID
    TargetNodeID        graph.NodeID
    SourceNodeID        graph.NodeID
    // existing revision, retry, status, lease, and timestamps remain
}
```

### State/status

Replace `SemanticIndexState` with:

```go
type SemanticRuleState struct {
    SemanticRuleID  SemanticRuleID
    State           string
    LastBackfillAt  *time.Time
    LastRefreshAt   *time.Time
    LastError       string
    DirtyCount      int
    RecordCount     int
    UpdatedAt       time.Time
}

type SemanticSearchIndexState struct {
    SemanticRuleID      SemanticRuleID
    EmbeddingBindingKey string
    State               string
    LiveRecordCount     int64
    LastRebuildAt       *time.Time
    LastError           string
    UpdatedAt           time.Time
}
```

## Implementation tasks

### 1. Add the new semantic rule model

Files:

```text
internal/semantic/model/semantic.go
```

Tasks:

- add new ID aliases and typed string enums;
- add rule, trigger, selector, source, binding, maintenance, and storage structs;
- add record/work/state structs with rule/binding fields;
- keep JSON tags aligned with SGR0 proto names.

### 2. Remove new-model dependency on user-authored endpoint/model fields

Tasks:

- ensure `SemanticGenerationRule` has no endpoint/model/capability fields;
- ensure only record provenance fields may carry resolved endpoint/model data;
- ensure binding purpose replaces index-level purpose.

### 3. Introduce normalization/default helpers

Recommended helpers:

```go
NormalizeSemanticRuleKey(string) string
NormalizeSemanticTriggerPolicy(SemanticTriggerPolicy) SemanticTriggerPolicy
NormalizeSemanticEmbeddingBinding(SemanticEmbeddingBinding) SemanticEmbeddingBinding
DefaultSemanticMaintenancePolicy() SemanticMaintenancePolicy
DefaultSemanticStoragePolicy() SemanticStoragePolicy
```

Avoid full validation in SGR1; SGR3 owns validation and selector compilation.
SGR1 helpers should only make model defaults deterministic.

### 4. Update service-facing DTOs minimally

Files likely affected:

```text
internal/semantic/service/types.go
internal/semantic/backfill/runner.go
internal/semantic/maintenance/*
internal/semantic/search/*
```

Tasks:

- update input/output types where simple rename is safe;
- otherwise leave old method names in storage/runtime and add TODO comments that
  SGR2+ will replace them;
- keep compile stability over completeness.

### 5. Model tests

Add or update tests under:

```text
internal/semantic/model
```

Coverage:

- default trigger uses `changed`;
- binding purpose is per-binding;
- semantic rule model has no user-authored endpoint/model/capability fields;
- dirty work identity includes `semantic_rule_id`, `embedding_binding_key`, and
  `target_node_id`;
- record provenance includes rule/binding and resolved access/provider IDs;
- JSON names match SGR0 API names.

### 6. Compatibility cleanup boundary

SGR1 should either:

- fully rename model types and fix all compile errors in affected packages; or
- add transitional type aliases clearly marked for removal in SGR2/SGR4.

Acceptable temporary aliases:

```go
type SemanticIndexID = SemanticRuleID
```

Avoid adding compatibility behavior in storage/API. This is only to keep
untouched code compiling during the tranche split.

## Out of scope

- protobuf regeneration;
- daemon API adapter replacement;
- storage file layout changes;
- raft/WAL mutation renames;
- analyzer selector compilation;
- source assembly execution;
- embedding generation rewrite;
- physical search-index implementation;
- CLI/console changes;
- Rust SDK changes.

## Suggested commit shape

1. `Add semantic generation rule domain types`
2. `Make semantic model records and work binding-aware`
3. `Add semantic rule model defaults tests`

If compile changes are small, one commit is fine. If compatibility aliases touch
many packages, keep model addition and compile-adapter cleanup separate.

## Validation

Minimum:

```sh
go test ./internal/semantic/model -count=1
go test ./internal/semantic/service -count=1
git diff --check
```

Preferred if compile churn reaches maintenance/search/backfill packages:

```sh
go test ./internal/semantic/... -count=1
make docs-check
```

Do not run destructive cluster/raft tests for SGR1.

## Acceptance criteria

SGR1 is complete when:

- `SemanticGenerationRule` and supporting policy/binding structs exist in the
  internal semantic model;
- new semantic rule structs use Intelligence Access profile terminology;
- user-authored semantic rule structs contain no direct endpoint/model/capability
  fields;
- vector records and dirty work have rule + embedding binding attribution;
- binding purpose replaces index-level purpose in the new model;
- model defaults/JSON naming are covered by tests;
- existing daemon packages still compile for the selected validation scope;
- follow-up TODOs are documented for SGR2+ where transitional aliases remain.

Implemented notes:

- Transitional aliases remain for `SemanticIndexID` and
  `AdvancedEmbeddingRecordID` so untouched storage, maintenance, search, and
  daemon adapters continue to compile.
- `SemanticVectorRecord`, rule/binding-aware dirty work fields, rule state, and
  search-index state are available for SGR2+ rewrites.
- Model tests cover defaults, JSON names, rule/binding attribution, and absence
  of direct endpoint/model/capability fields on user-authored rules.

## Risks and follow-ups

- A full rename may cascade through storage, maintenance, backfill, search, and
  daemon API adapters. Prefer transitional aliases if a complete rename would
  turn SGR1 into SGR2-SGR8.
- Existing `internal/inference/model` already contains profile/credential/grant
  types. SGR1 should avoid duplicating too much Intelligence Access code unless
  the dedicated package is introduced deliberately.
- Later tranches must remove any compatibility aliases before the public API and
  daemon adapters are regenerated.
