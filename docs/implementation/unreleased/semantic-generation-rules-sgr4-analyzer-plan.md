# SGR4 Semantic Generation Rules Analyzer Plan

## Status

Implemented for rule-native node-type and explicit-node analyzer paths. Bounded
GQL selectors are validated in SGR3 but execution remains a documented follow-up
because the current analyzer does not own a graph transaction/session execution
context.

## Goal

Rewrite semantic dirty-event analysis to use semantic generation rules instead of
semantic indexes. The analyzer should apply rule triggers, resolve selected graph
targets, and enqueue one binding-aware maintenance work item per affected
rule/binding/target.

SGR4 should not rewrite source assembly, provider invocation, vector records,
search planning, public APIs, CLI, or console UX. Those are later SGR5-SGR10
tranches.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Semantic maintenance analyzer, analyzer tests, service analyze inputs, checkpoint naming, dirty work enqueue logic. |
| `mycel-api` | Out of scope unless maintenance request fields need an SGR0 correction. |
| `mycel-console` | Out of scope. |
| `mycel-rust-sdk` | Out of scope. |

## Primary files

```text
internal/semantic/maintenance/analyzer.go
internal/semantic/maintenance/analyzer_test.go
internal/semantic/service/types.go
internal/semantic/service/module.go
internal/semantic/storage/interface.go
internal/semantic/storage/file_store.go
internal/semantic/storage/file_store_test.go
internal/semantic/model/semantic.go
```

Likely helper/test fixtures:

```text
internal/query/gql/execution/*
internal/automation/service/context_input.go
```

## Current analyzer behavior

The current analyzer:

- loads `ListSemanticIndexes`;
- filters by enabled index and optional `SemanticIndexID`;
- uses `SemanticSourcePolicy.RecordTypes` as an advisory label filter;
- treats one index as one embedding/vector binding;
- enqueues `SemanticDirtyWorkItem{SemanticIndexID, TargetNodeID}`;
- checkpoints per semantic index;
- updates `SemanticIndexState`.

## Target analyzer behavior

The SGR4 analyzer should:

1. load enabled `SemanticGenerationRule` values through `ListSemanticRules`;
2. filter by optional `SemanticRuleID` and trigger policy;
3. resolve targets via rule selector:
   - `node_type` / labels;
   - `explicit_nodes` for targeted backfill/test paths;
   - bounded GQL selector if SGR3 validation/compiler support exists;
4. for each target and enabled embedding binding, enqueue work keyed by:

   ```text
   semantic_rule_id + embedding_binding_key + target_node_id
   ```

5. apply rule/binding-specific debounce/cooldown;
6. checkpoint per rule and binding;
7. update `SemanticRuleState` and `SemanticSearchIndexState` as appropriate;
8. keep old index analyzer path only as a transitional wrapper if needed for
   later runtime tranches.

## API and type changes

Update analyzer input:

```go
type AnalyzeInput struct {
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string
    Limit               int
    Now                 time.Time

    // transitional until maintenance API adapters are regenerated
    SemanticIndexID domainsemantic.SemanticIndexID
}
```

Update service input similarly:

```go
type AnalyzeInput struct {
    SpaceID             domainspace.SpaceID
    SemanticRuleID      domainsemantic.SemanticRuleID
    EmbeddingBindingKey string
    Limit               int

    // transitional
    SemanticIndexID domainsemantic.SemanticIndexID
}
```

Keep public maintenance API adapters untouched until SGR8 unless regenerated
stubs require earlier changes.

## Trigger filtering

Add cheap trigger filtering before selector evaluation:

- default/empty trigger means `changed`;
- `changed` matches any dirty event that touches the rule domain;
- create/update/delete events match respective dirty event node ID lists;
- edge events match `ChangedEdges` where applicable;
- trigger labels should match changed node/edge labels before expensive selector
  work when data is available;
- if label data is unavailable for a dirty event, fail safe by evaluating the
  selector rather than dropping work incorrectly.

Suggested helper:

```go
func eventMatchesRuleTrigger(event GraphDirtyEvent, rule SemanticGenerationRule) bool
```

## Target resolution

### Node-type selector

For `SemanticTargetSelectorNodeType`:

- inspect created/updated/deleted node IDs from the dirty event;
- load nodes as needed through `GraphReader.GetNode`;
- match selector labels against graph node labels;
- for subtree source mode, preserve existing parent/root behavior where it maps
  child changes to source/target roots, but use the rule source policy instead of
  old `SourceExtraction`.

### Explicit-node selector

For `SemanticTargetSelectorExplicit`:

- enqueue explicit nodes when a relevant event touches the domain;
- if this is too noisy for normal dirty analysis, restrict explicit-node selector
  analysis to explicit backfill paths and document that analyzer ignores it until
  SGR5 backfill rewrite.

### GQL selector

For `SemanticTargetSelectorGQL`:

- use the SGR3 compiled/validated selector;
- bind a `changed` alias for the changed node when available;
- execute with a timeout and row limit equivalent to automation context queries;
- extract target IDs from `TargetAlias`;
- fail closed on invalid/missing target alias;
- do not allow unbounded execution.

If execution integration is too large, SGR4 may implement node-type selectors
first and mark GQL selector evaluation as SGR4 follow-up, but validation must
already reject unsafe GQL from SGR3.

## Binding-aware enqueue

For each resolved target:

```go
for _, binding := range rule.Embeddings {
    if !binding.Enabled { continue }
    if filterBindingKey != "" && binding.Key != filterBindingKey { continue }
    work := SemanticDirtyWorkItem{
        SemanticRuleID: rule.ID,
        EmbeddingBindingKey: binding.Key,
        SpaceID: rule.SpaceID,
        DomainID: rule.DomainID,
        TargetNodeID: targetID,
        SourceNodeID: sourceNodeID,
        Action: action,
        Reason: reason,
        Status: SemanticDirtyWorkStatusPending,
    }
}
```

Do not set `SemanticIndexID` in new analyzer-created work except possibly as a
transitional alias for old worker/backfill code. If set, keep it equal to
`SemanticIndexID(rule.ID)`.

## Debounce and cooldown

- Start with rule-level `SemanticMaintenancePolicy.DirtyCooldown`.
- Add a binding-specific callback only if needed by existing schema cooldown
  logic.
- Existing `DirtyCooldownForTarget` callback can remain as a transitional
  adapter but should accept rules in new code.
- Coalescing is handled by SGR2 storage keying.

## Checkpoints

Checkpoint consumers should include rule and binding key:

```text
semantic-analyzer/<semantic_rule_id>/<embedding_binding_key>
```

This ensures one slow/broken binding does not block another binding on the same
rule.

For events skipped because they do not match the trigger/selector, checkpoint the
rule/binding so the analyzer does not repeatedly inspect the same event.

## State updates

Use rule-native state:

- `SemanticRuleState` for rule-level analyzer status, dirty count, last error,
  and refresh/checkpoint metadata if available;
- `SemanticSearchIndexState` for per-binding search readiness/backlog status
  where useful.

Existing `SemanticIndexState` updates may remain as transitional mirrors only if
worker/API code still reads them.

## Tests

Update/add tests in `internal/semantic/maintenance/analyzer_test.go`:

1. enabled node-type rule enqueues work for matching changed node;
2. non-matching labels do not enqueue work;
3. disabled rule is skipped;
4. create/update/delete trigger filters work;
5. one rule with two enabled bindings enqueues two work items for one target;
6. disabled binding is skipped;
7. binding filter only enqueues matching binding;
8. repeated events coalesce through storage by rule/binding/target;
9. checkpoints are per rule/binding;
10. deleted node produces delete/tombstone work for affected bindings;
11. optional GQL selector resolves target alias under row limits;
12. invalid GQL selector never executes because SGR3 validation blocks storage.

Service tests should cover `Module.AnalyzeDirtyWork` passing rule/binding fields
to the analyzer.

## Validation commands

Minimum:

```sh
go test ./internal/semantic/maintenance -count=1
go test ./internal/semantic/service -count=1
git diff --check
```

Preferred:

```sh
go test ./internal/semantic/... -count=1
make docs-check
```

Do not run destructive Compose/K3s cluster tests for SGR4.

## Acceptance criteria

SGR4 is complete when:

- analyzer loads semantic rules instead of semantic indexes for new work;
- graph changes enqueue one work item per affected rule/binding/target;
- trigger filters and node-type selectors are enforced;
- binding filters are supported for targeted analysis/backfill preparation;
- checkpoints are rule/binding aware;
- dirty counts/state updates are rule-native or clearly mirrored transitionally;
- existing semantic package tests pass;
- remaining old index analyzer assumptions are documented for SGR5-SGR8 removal.

Implemented notes:

- Analyzer prefers rule-native `ListSemanticRules` when rules exist and falls
  back to transitional semantic-index analysis only for old runtime paths.
- Node-type selectors enforce graph labels and enqueue one work item per enabled
  binding.
- Dirty work carries `semantic_rule_id`, `embedding_binding_key`, and a
  transitional `semantic_index_id` mirror for old worker/backfill code.
- Trigger filtering supports `changed`, node create/update/delete, and edge
  change events.
- Checkpoints are per rule/binding via
  `semantic-analyzer/<rule_id>/<binding_key>`.
- Rule state is updated through `SemanticRuleState`; old index state remains only
  in fallback index analysis.

## Risks and follow-ups

- GQL selector execution may require a graph transaction/session context that the
  current analyzer does not own. If so, implement node-type selectors first and
  leave GQL execution behind a clear interface for a follow-up.
- Existing worker/backfill still expects `SemanticIndexID`; transitional aliasing
  may be required until SGR5.
- Deleted-node subtree behavior is subtle; preserve existing conservative
  behavior and add tests before broadening it.
