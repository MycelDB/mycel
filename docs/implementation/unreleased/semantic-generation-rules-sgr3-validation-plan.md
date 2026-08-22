# SGR3 Semantic Generation Rules Validation Plan

## Status

Implemented. This tranche follows SGR2 rule-native storage work and adds
structured semantic rule validation plus bounded/read-only GQL selector checks.

## Goal

Add validation and selector-compilation support for semantic generation rules so
invalid rules fail before analyzer, maintenance, or embedding work can enqueue
background jobs.

SGR3 should validate rule shape, trigger policy, node-type selectors, bounded GQL
selectors, and optional source context queries. Runtime analyzer execution of
selectors belongs to SGR4.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | Semantic rule validator, selector compilation helpers, validation tests, storage/service validation call sites. |
| `mycel-api` | Out of scope unless diagnostics need a field missing from SGR0. |
| `mycel-console` | Out of scope; diagnostics should be console-friendly for SGR10. |
| `mycel-rust-sdk` | Out of scope. |

## Primary files

```text
internal/semantic/model/semantic.go
internal/semantic/model/semantic_rule_test.go
internal/semantic/storage/file_store.go
internal/semantic/storage/file_store_test.go
internal/query/gql/*
internal/automation/model/validate.go
internal/automation/service/context_input.go
```

Recommended new files:

```text
internal/semantic/model/validate.go
internal/semantic/model/validate_test.go
internal/semantic/model/selector.go
internal/semantic/model/selector_test.go
```

## Design principles

1. **Storage validation stays shallow.** Storage should enforce identity and
   durable shape invariants; semantic rule validation should own semantic
   correctness.
2. **Diagnostics are structured.** Validation should return path, severity, and
   message so admin APIs and console can display useful errors.
3. **GQL must be bounded and read-only.** Reuse graph automation hardening rules:
   compile/inspect query, reject read-write plans, require explicit `FETCH FIRST`,
   cap row limits, require labeled relationship patterns, and require a target
   alias.
4. **Selectors are compiled, not executed.** SGR3 may compile selectors and
   produce metadata; actual target evaluation belongs to SGR4.
5. **Default behavior is deterministic.** Omitted trigger events normalize to
   `changed`; omitted storage policy normalizes to searchable `exact`.

## Validator API

Add:

```go
type ValidationSeverity string

const (
    ValidationSeverityError   ValidationSeverity = "error"
    ValidationSeverityWarning ValidationSeverity = "warning"
    ValidationSeverityInfo    ValidationSeverity = "info"
)

type ValidationDiagnostic struct {
    Severity ValidationSeverity `json:"severity"`
    Path     string             `json:"path"`
    Message  string             `json:"message"`
}

type ValidationResult struct {
    Valid       bool                   `json:"valid"`
    Diagnostics []ValidationDiagnostic `json:"diagnostics,omitempty"`
    Rule        SemanticGenerationRule `json:"normalized_rule"`
}

func ValidateSemanticGenerationRule(rule SemanticGenerationRule) ValidationResult
func ValidateSemanticGenerationRuleForStorage(spaceID domainspace.SpaceID, rule SemanticGenerationRule) ValidationResult
```

`ValidateSemanticGenerationRule` should normalize a copy of the rule and return
all actionable diagnostics instead of failing fast. Storage/API adapters can turn
`Valid=false` into an error message.

## Trigger validation

Validate:

- supported event names initially include `changed`, `node_created`,
  `node_updated`, `node_deleted`, and edge change events if already represented
  by dirty events;
- empty trigger normalizes to `changed`;
- event names are lower-case/trimmed;
- debounce is non-negative;
- labels are trimmed and non-empty when present.

## Node-type selector validation

For `SemanticTargetSelectorNodeType`:

- labels are required unless a future explicit `all_nodes` field exists;
- labels are normalized consistently with graph labels;
- `GQL`, `TargetAlias`, and `NodeIDs` must be empty;
- `MaxResults` may be zero or positive; negative is invalid.

## Explicit-node selector validation

For `SemanticTargetSelectorExplicit`:

- at least one node ID is required;
- node IDs must not be nil;
- `Labels`, `GQL`, and `TargetAlias` must be empty;
- `MaxResults` may be zero or positive; negative is invalid.

This is mainly useful for backfill and tests; general authoring may remain
secondary to node-type/GQL selectors.

## GQL selector validation

For `SemanticTargetSelectorGQL`:

- `GQL` is required;
- `TargetAlias` is required;
- compile query with `gql.Compile` initially; schema-aware compilation can be
  added when a schema context is available;
- `plan.AccessMode` must not be `analysis.ReadWrite`;
- every graph read operation must have a positive limit from explicit
  `FETCH FIRST`;
- maximum plan limit must be <= `semanticSelectorMaxRows` (recommend 500);
- relationship/path patterns must include labels;
- plan must reference/produce the target alias;
- `MaxResults` must be non-negative and <= plan limit when set;
- `Labels` and `NodeIDs` must be empty for GQL mode.

Create semantic-local helpers equivalent to automation helpers:

```go
semanticPlanLimit(planmodel.Plan) int64
semanticPlanReferencesAlias(planmodel.Plan, string) bool
semanticPlanPatternsAreLabelBounded(planmodel.Plan) bool
```

Prefer moving these helpers to a shared query validation package only if the diff
stays small. Otherwise, duplicate in SGR3 and refactor later.

## Source assembly validation

For `SemanticSourceAssemblyPolicy`:

- mode defaults to `self`;
- supported modes: `self`, `subtree`, `context_query`;
- `MaxDepth` must be non-negative when set;
- `MinimumTextLength` must be non-negative;
- `IncludeProperties` and `ExcludeProperties` entries must be trimmed/non-empty;
- `context_query` source requires `ContextGQL` and must use the same read-only,
  bounded, labeled relationship validation as selector GQL;
- non-`context_query` modes must not set `ContextGQL`.

If context-query compilation becomes too large, SGR3 may validate only the shape
and explicitly reject `context_query` with a future diagnostic.

## Binding validation

Validate:

- at least one embedding binding;
- binding keys are required, normalized, and unique;
- binding purpose is binding-scoped and non-empty for enabled bindings;
- each enabled binding references an Intelligence Access profile by key or ID;
- each enabled binding references a vector store by key or ID;
- endpoint/model/capability fields are impossible on the rule model and should
  remain absent.

Do not resolve profiles/vector stores in SGR3; existence checks can be added in
service/API validation when managers are available.

## Storage/service integration

Update `internal/semantic/storage/file_store.go`:

- replace bespoke `validateSemanticRule` with model validator for shape checks;
- keep store-specific checks for matching `space_id` and key conflicts;
- persist normalized rule from validation result.

Add `Module.ValidateRule` or equivalent service method only if needed by tests.
Full admin API wiring belongs to SGR8.

## Tests

Add model tests for:

- valid node-type selector;
- missing node-type labels;
- valid bounded GQL selector;
- GQL selector rejects write/read-write statements;
- GQL selector rejects missing `FETCH FIRST`;
- GQL selector rejects unlabeled relationships;
- GQL selector rejects missing target alias;
- duplicate binding keys;
- enabled binding without Intelligence Access profile;
- negative debounce/max depth/minimum text length;
- context-query source shape validation.

Update storage tests to assert invalid rules fail through storage.

## Validation commands

Minimum:

```sh
go test ./internal/semantic/model ./internal/semantic/storage -count=1
git diff --check
```

Preferred:

```sh
go test ./internal/semantic/... ./internal/automation/... -count=1
make docs-check
```

Do not run destructive Compose/K3s cluster tests for SGR3.

## Acceptance criteria

SGR3 is complete when:

- semantic rule validation returns structured diagnostics;
- invalid rules cannot be persisted through rule-native storage;
- node-type, explicit-node, and bounded GQL selectors are validated;
- selector/source GQL validation is read-only, bounded, alias-explicit, and
  relationship-label bounded;
- binding validation enforces Intelligence Access profile references and vector
  store references;
- tests cover successful and failing validation cases;
- analyzer/backfill/search behavior is unchanged except that invalid rules cannot
  enter storage.

Implemented notes:

- `ValidateSemanticGenerationRule` and `ValidateSemanticGenerationRuleForStorage`
  return normalized rules plus structured diagnostics.
- Node-type, explicit-node, and GQL selectors are validated.
- GQL validation rejects read-write, unbounded, unlabeled relationship, and
  missing target-alias queries.
- Storage now uses the model validator before persisting rule-native definitions.
- Context query source validation uses the same bounded/read-only GQL checks.

## Risks and follow-ups

- GQL parser/plan APIs may not expose every alias detail needed. If target alias
  detection is incomplete, fail closed and document the missing planner hook.
- Schema-aware validation may require service context unavailable in model tests;
  keep SGR3 schema-neutral unless small.
- SGR4 must consume compiled/validated selector assumptions when evaluating
  targets.
