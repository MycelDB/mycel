# SGR2 Semantic Generation Rules Storage Plan

## Status

Implemented. This tranche follows SGR1 domain-model work and keeps the existing
runtime functional by adding rule-native storage and WAL/raft surfaces alongside
transitional semantic-index wrappers.

## Goal

Replace semantic-index-oriented storage interfaces, file layouts, raft/WAL mutation
names, and maintenance work keys with semantic-generation-rule and
embedding-binding terminology.

SGR2 should make rules durable and binding-aware while preserving the existing
runtime behavior enough for later analyzer, backfill, generation, search, API,
and CLI tranches to replace their logic incrementally.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Semantic storage interfaces/implementation, WAL/raft semantic mutations, storage tests, service compile adapters. |
| `mycel-api` | Out of scope unless a storage need exposes an SGR0 API gap. |
| `mycel-console` | Out of scope. |
| `mycel-rust-sdk` | Out of scope. |

## Current storage surfaces

Primary files:

```text
internal/semantic/storage/interface.go
internal/semantic/storage/file_store.go
internal/semantic/storage/file_store_test.go
internal/semantic/service/wal_space_methods.go
internal/semantic/service/wal_wrappers.go
internal/semantic/service/wal_canonical.go
internal/semantic/service/wal_resolve.go
internal/semantic/service/raft_read.go
internal/semantic/service/raft_snapshot.go
internal/semantic/service/raft_test.go
internal/semantic/service/raft_snapshot_test.go
internal/semantic/service/types.go
```

Current index-oriented methods to replace:

```go
UpsertSemanticIndex
ListSemanticIndexes
DeleteSemanticIndex
UpsertIndexState
ListIndexStates
```

Current index-oriented file names/layouts:

```text
graphs/<space_id>/semantic/indexes.json
graphs/<space_id>/semantic/index_state.json
graphs/<space_id>/semantic/maintenance/work/state.json
graphs/<space_id>/semantic/maintenance/work/work-000001.ksem
```

Target rule-oriented file names/layouts:

```text
graphs/<space_id>/semantic/rules.json
graphs/<space_id>/semantic/rule_state.json
graphs/<space_id>/semantic/search_index_state.json
graphs/<space_id>/semantic/maintenance/work/state.json
graphs/<space_id>/semantic/maintenance/work/work-000001.ksem
```

`state.json` and `work-000001.ksem` stay in place, but work item keys become:

```text
semantic_rule_id + embedding_binding_key + target_node_id
```

## Design decisions

### No old-format compatibility required

The product is unreleased. SGR2 should not add migration readers for old
`indexes.json` or `index_state.json` unless tests need temporary fixture support.
Prefer clean rule-native files.

### Keep Intelligence Access storage boundary explicit

SGR2 is about semantic rule storage. It should not fully split the existing
credential/grant/policy stores into a new Intelligence Access package unless that
is needed for compile stability. If touched, use Intelligence Access terminology
for new names and leave old inference-named methods as temporary wrappers only.

### Bindings are part of the rule

A semantic rule owns one or more `SemanticEmbeddingBinding` values. Storage must
preserve binding order and stable binding keys. Work/state records should refer
to binding keys, not binding array indexes.

### Physical search state is derived/rebuildable

`SemanticSearchIndexState` is durable operational state but remains
rebuildable/derived. It should be safe to delete and rebuild later. SGR2 only
stores the state shape; SGR6 implements physical index build/search behavior.

### Transitional wrappers are acceptable

Runtime code still uses index method names in analyzer/backfill/search/API. SGR2
may keep wrappers like `UpsertSemanticIndex` delegating through rule conversion,
but new interfaces/tests should use rule names. Any wrapper must be marked for
removal in SGR4-SGR8.

## Target storage interfaces

Update `internal/semantic/storage/interface.go`.

### SpaceManager

Add rule-native methods:

```go
type SpaceManager interface {
    Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error

    UpsertSemanticRule(ctx context.Context, rule domainsemantic.SemanticGenerationRule) (domainsemantic.SemanticGenerationRule, error)
    ListSemanticRules(ctx context.Context) ([]domainsemantic.SemanticGenerationRule, error)
    DeleteSemanticRule(ctx context.Context, id domainsemantic.SemanticRuleID, purgeDependents bool) error

    UpsertSemanticRuleState(ctx context.Context, state domainsemantic.SemanticRuleState) (domainsemantic.SemanticRuleState, error)
    ListSemanticRuleStates(ctx context.Context) ([]domainsemantic.SemanticRuleState, error)

    UpsertSearchIndexState(ctx context.Context, state domainsemantic.SemanticSearchIndexState) (domainsemantic.SemanticSearchIndexState, error)
    ListSearchIndexStates(ctx context.Context) ([]domainsemantic.SemanticSearchIndexState, error)

    // Existing Intelligence Access grant/policy/decision methods remain until
    // the dedicated Intelligence Access storage tranche.
}
```

Temporary compatibility methods may remain in the interface only if needed by
untouched code:

```go
UpsertSemanticIndex(...)
ListSemanticIndexes(...)
DeleteSemanticIndex(...)
UpsertIndexState(...)
ListIndexStates(...)
```

If kept, wrappers should convert between old transitional `SemanticIndex` values
and new `SemanticGenerationRule` values through explicit helper functions.

### MaintenanceManager

Keep method names initially, but make validation/keying rule-native:

```go
UpsertDirtyWorkItem(ctx, item domainsemantic.SemanticDirtyWorkItem)
UpsertDirtyWorkItems(ctx, items []domainsemantic.SemanticDirtyWorkItem)
ListDirtyWorkItems(ctx)
ClaimReadyWork(ctx, in ClaimReadyWorkInput)
```

Validation should require:

- `semantic_rule_id` or transitional `semantic_index_id`;
- `embedding_binding_key` for rule-native work except delete/cleanup cases where
  all bindings may be affected;
- matching `space_id`;
- `target_node_id`.

Coalescing key should use:

```go
item.EffectiveSemanticRuleID(), item.EmbeddingBindingKey, item.TargetNodeID
```

not `semantic_index_id + target_node_id`.

## Target file-store changes

### 1. Rename state containers

In `internal/semantic/storage/file_store.go`:

```go
type semanticRulesState struct {
    Rules []domainsemantic.SemanticGenerationRule `json:"rules"`
}

type semanticRuleStatesState struct {
    States []domainsemantic.SemanticRuleState `json:"states"`
}

type semanticSearchIndexStatesState struct {
    States []domainsemantic.SemanticSearchIndexState `json:"states"`
}
```

Replace `spaceManager.indexes` with `spaceManager.rules` and add loaded indexes:

```go
ruleByID  map[domainsemantic.SemanticRuleID]int
ruleByKey map[string]int // space_id + domain_id + normalized key
```

Optional but recommended for SGR4/SGR5 readiness:

```go
rulesByDomain        map[graph.DomainID][]int
rulesByTriggerEvent  map[string][]int
rulesByTriggerLabel  map[string][]int
rulesByProfileRef    map[string][]int
rulesByVectorStoreID map[domainsemantic.VectorStoreID][]int
```

### 2. Add rule validation for storage invariants

Storage validation is not full SGR3 validation. It should enforce only durable
identity and shape invariants:

- matching non-empty `space_id`;
- non-empty `domain_id`;
- normalized/non-empty rule key;
- unique `(space_id, domain_id, key)`;
- at least one embedding binding;
- unique non-empty binding keys;
- no endpoint/model/capability fields on rule because the struct should not have
  them;
- created/updated timestamps defaulted.

Full selector/GQL validation remains SGR3.

### 3. Rule upsert semantics

`UpsertSemanticRule` should:

- normalize `rule.Key`;
- normalize each binding key;
- assign `rule.ID` if missing;
- default `Trigger` with `changed`;
- default `Maintenance` and `Storage` where omitted;
- preserve existing ID and `CreatedAt` on key-based upsert;
- update by ID when ID exists;
- reject an ID/key conflict with actionable errors;
- persist to `rules.json`.

### 4. Rule delete semantics

`DeleteSemanticRule` should:

- remove the rule by ID;
- when `purgeDependents` is true, remove:
  - matching Intelligence Access grant/policy/decision scopes by
    `semantic_rule_id` and transitional `semantic_index_id`;
  - matching rule states;
  - matching search-index states;
  - matching pending/running maintenance work where practical in this tranche;
- persist all changed state files.

Do not purge vector records in SGR2 unless existing `PurgeVectorIndex` can safely
be called through transitional adapters. Vector record purge belongs to SGR6/SGR8
if it requires new physical index behavior.

### 5. Rule state methods

Add:

```go
UpsertSemanticRuleState
ListSemanticRuleStates
UpsertSearchIndexState
ListSearchIndexStates
```

`UpsertSearchIndexState` should be keyed by:

```text
semantic_rule_id + embedding_binding_key
```

### 6. Rule-native maintenance work keys

Update `dirtyWorkKey`:

```go
type dirtyWorkKey struct {
    semanticRuleID      domainsemantic.SemanticRuleID
    embeddingBindingKey string
    targetNodeID        graphmodel.NodeID
}
```

Rebuild, validation, and upsert should normalize transitional fields:

```go
func normalizeDirtyWorkItem(item domainsemantic.SemanticDirtyWorkItem) domainsemantic.SemanticDirtyWorkItem {
    if item.SemanticRuleID == uuid.Nil {
        item.SemanticRuleID = domainsemantic.SemanticRuleID(item.SemanticIndexID)
    }
    if item.SemanticIndexID == uuid.Nil {
        item.SemanticIndexID = domainsemantic.SemanticIndexID(item.SemanticRuleID)
    }
    item.EmbeddingBindingKey = strings.TrimSpace(item.EmbeddingBindingKey)
    return item
}
```

This lets old analyzer code keep working while new tests assert binding-aware
coalescing.

## WAL/raft changes

Files:

```text
internal/semantic/service/wal_space_methods.go
internal/semantic/service/wal_wrappers.go
internal/semantic/service/wal_canonical.go
internal/semantic/service/wal_resolve.go
internal/semantic/service/raft_read.go
internal/semantic/service/raft_snapshot.go
```

### New mutation kinds

Add rule-native mutation kinds:

```text
semantic_rule.upsert
semantic_rule.delete
semantic_rule_state.upsert
semantic_search_index_state.upsert
```

Keep old mutation kind handling only as transitional replay support inside the
current repo branch, not as a public compatibility promise.

### Read forwarding

Replace or supplement read op:

```text
list_indexes -> list_rules
```

Return payload:

```go
type raftSemanticRulesResponse struct {
    Rules []domainsemantic.SemanticGenerationRule `json:"rules"`
}
```

### Snapshot payload

Replace snapshot fields:

```go
Rules             []domainsemantic.SemanticGenerationRule
RuleStates        []domainsemantic.SemanticRuleState
SearchIndexStates []domainsemantic.SemanticSearchIndexState
```

Keep old fields only if compile/runtime restoration needs them temporarily.
Snapshot restore should normalize work item rule IDs and binding keys.

## Service-facing adapters

Update `internal/semantic/service/types.go` to add rule-native manager methods:

```go
ListRules(ctx, spaceID, domainID) ([]domainsemantic.SemanticGenerationRule, error)
```

Defer public API adapter replacement to SGR8. Existing `ListIndexes`,
`BackfillIndex`, and search inputs can remain until their tranches, but should be
clearly marked transitional.

## Test plan

### Storage tests

Update/add tests in:

```text
internal/semantic/storage/file_store_test.go
```

Coverage:

1. `UpsertSemanticRule` creates `rules.json` with normalized key and binding
   keys.
2. key-based upsert preserves ID and created timestamp.
3. duplicate binding keys fail with actionable error.
4. `ListSemanticRules` returns durable rules after reload.
5. `DeleteSemanticRule(..., purgeDependents=true)` removes rule state,
   search-index state, scoped decisions/grants/policies where represented.
6. dirty work coalesces by `(rule_id, binding_key, target_node_id)` so two
   bindings for one target produce distinct work items.
7. transitional old work items without `semantic_rule_id` are normalized from
   `semantic_index_id` during load/upsert.
8. search-index state upsert is keyed by `(rule_id, binding_key)`.

### Service/WAL/raft tests

Update focused tests in:

```text
internal/semantic/service/raft_test.go
internal/semantic/service/raft_snapshot_test.go
```

Coverage:

- rule upsert/delete applies through WAL in non-raft mode;
- rule upsert/delete/read forwarding works in raft mode where current tests
  already cover semantic indexes;
- snapshot captures/restores rules, rule states, search-index states, and
  binding-aware maintenance work.

## Validation

Minimum:

```sh
go test ./internal/semantic/storage -count=1
go test ./internal/semantic/service -count=1
git diff --check
```

Preferred:

```sh
go test ./internal/semantic/... -count=1
make docs-check
```

Do not run destructive Compose/K3s cluster tests for SGR2 unless explicitly
requested.

## Implementation sequence

1. Update storage interfaces with rule-native methods and temporary wrappers.
2. Add `semanticRulesState`, rule state, and search-index state containers.
3. Implement `UpsertSemanticRule`, `ListSemanticRules`, and
   `DeleteSemanticRule` in the file store.
4. Add rule state and search-index state upsert/list methods.
5. Change dirty work keying to include rule ID and embedding binding key.
6. Add transitional conversion helpers between `SemanticIndex` and
   `SemanticGenerationRule` only where existing runtime code still needs old
   method names.
7. Update WAL method wrappers and mutation replay for rule-native kinds.
8. Update raft read/snapshot payloads.
9. Update storage and service tests.
10. Run validation and document any remaining transitional aliases.

## Acceptance criteria

SGR2 is complete when:

- semantic rules are durably stored/listed/deleted through rule-native storage
  methods;
- rule state and search-index state are durably stored with rule/binding keys;
- maintenance work coalesces by rule ID, binding key, and target node ID;
- WAL/raft mutation/read/snapshot paths have rule-native operations;
- existing semantic packages still compile;
- tests prove binding-aware work and durable rule reload behavior;
- old index method names are either removed from the storage interface or marked
  as temporary wrappers with follow-up removal notes.

Implemented notes:

- `SpaceManager` now has rule-native methods for semantic rules, rule state, and
  search-index state.
- File storage persists rule-native metadata to `rules.json`, `rule_state.json`,
  and `search_index_state.json`.
- Maintenance work coalesces by effective rule ID, embedding binding key, and
  target node ID, while normalizing legacy `semantic_index_id` fields into
  `semantic_rule_id` for transitional runtime compatibility.
- WAL/raft wrappers and local read forwarding support rule-native mutation/read
  names while retaining old semantic-index mutation replay paths.
- Existing analyzer, backfill, search, and daemon API adapters still use
  transitional index methods and are scheduled for later SGR tranches.

## Risks and follow-ups

- WAL/raft rename can cascade into many tests. Keep compatibility replay for old
  mutation names during this branch if needed to keep the tranche reviewable.
- Existing analyzer/backfill/search still assume one index equals one model/vector
  binding. SGR2 should not rewrite that behavior; conversion wrappers can expose
  one transitional synthetic index per first/default binding until SGR4-SGR7.
- Intelligence Access storage split is only partially represented here. A later
  tranche should move credential/grant/policy/usage storage into a dedicated
  Intelligence Access subsystem.
- Physical vector/search record layout is still later SGR6 work; SGR2 only makes
  durable rule and work metadata binding-aware.
