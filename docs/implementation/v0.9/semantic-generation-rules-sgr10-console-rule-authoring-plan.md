# SGR10 Semantic Generation Rules Console Authoring Plan

## Status

Planned. This tranche follows SGR9 rule-native CLI replacement.

## Goal

Update `mycel-console` so the `Intelligence / Semantic` page manages semantic
generation rules instead of legacy semantic indexes. Operators should be able to
list, inspect, validate, create, edit, enable/disable, delete, backfill, and
monitor semantic rules and their embedding bindings without opening raw JSON.

SGR10 is a Console UX tranche. It should consume the rule-native daemon APIs
from SGR8 and align terminology with SGR9. The product is unreleased, so do not
preserve legacy semantic-index UI labels or command names except as temporary
internal migration helpers removed before acceptance.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel-console` | Primary implementation: Tauri commands, TypeScript service/types, Intelligence/Semantic page, tests, build validation. |
| `mycel` | Plan and any small docs notes only. No daemon behavior changes expected unless Console reveals a missing SGR8 API field. |
| `mycel-api` | Out of scope unless a missing API field is found; source protobufs should already be rule-native. |
| `mycel-rust-sdk` | Out of scope unless explicitly approved to regenerate/commit public SDK code. |

## Primary files

Console Rust/Tauri command surface:

```text
../mycel-console/src-tauri/src/commands/semantic.rs
../mycel-console/src-tauri/src/commands/semantic_maintenance.rs
../mycel-console/src-tauri/src/commands/inference.rs
../mycel-console/src-tauri/src/lib.rs
../mycel-console/src-tauri/src/state.rs
```

Console TypeScript service/types:

```text
../mycel-console/src/services/adminService.ts
../mycel-console/src/services/adminService.test.ts
../mycel-console/src/types/semantic.ts
../mycel-console/src/types/semanticMaintenance.ts
../mycel-console/src/types/inference.ts
```

Console page/components:

```text
../mycel-console/src/features/intelligence/semantic/pages/SemanticPage.tsx
../mycel-console/src/features/intelligence/semantic/pages/SemanticPage.test.tsx
../mycel-console/src/features/spaces/pages/SpaceDetailPage.tsx
../mycel-console/src/features/console/navigation.ts
../mycel-console/src/components/typography/*.tsx
```

Generated protobuf consumers:

```text
../mycel-console/src-tauri/Cargo.toml
../mycel-rust-sdk/crates/mycel-proto/build.rs
../mycel-rust-sdk/crates/mycel-proto/src/lib.rs
```

## Current state after SGR9

- `mycel` daemon Admin/Client semantic APIs and CLI are rule-native.
- `mycel-console` navigation already groups Semantic under `INTELLIGENCE`.
- The Console Semantic page still uses legacy semantic-index types and commands:
  - `listSemanticIndexes`;
  - `BackfillSemanticIndex`;
  - `semantic_index_id` fields in maintenance and inference scope types;
  - page cards/labels for indexes rather than rules/bindings.
- The Tauri layer still calls legacy command names such as
  `admin_list_semantic_indexes` and `admin_backfill_semantic_index`.
- Console test fixtures still use `SemanticIndexInfo` and index states.

## Target UX

### Page layout

`Intelligence / Semantic` should present:

1. **Header and filters**
   - selected space;
   - optional domain filter;
   - enabled/disabled filter;
   - state/search-index health filters;
   - refresh action.
2. **Rule list**
   - rule key/display name;
   - enabled state;
   - rule state;
   - target selector summary;
   - source mode;
   - embedding binding count;
   - backlog/error badges;
   - physical search-index health.
3. **Rule details panel**
   - immutable IDs and scope;
   - trigger policy;
   - selector;
   - source assembly;
   - bindings table;
   - maintenance status/work summary;
   - recent errors;
   - usage summary if available.
4. **Rule editor drawer/modal**
   - create/edit modes;
   - validation preview before save;
   - explicit save/cancel;
   - delete/disable/backfill confirmations.

### Rule editor fields

Minimum editor fields:

- scope:
  - space;
  - domain;
  - key;
  - display name;
  - description;
  - enabled.
- trigger:
  - events, default `changed`;
  - labels;
  - debounce/dirty cooldown.
- selector:
  - `node_type` with labels;
  - `gql` with bounded query, target alias, max results;
  - `explicit_nodes` if already supported by SGR8 APIs.
- source:
  - `self`;
  - `subtree`;
  - `context_query`;
  - include/exclude properties;
  - max depth;
  - minimum text length;
  - context GQL when relevant.
- embedding bindings:
  - binding key;
  - purpose;
  - Intelligence Access profile key/ID;
  - vector store key/ID;
  - enabled;
  - metadata display if present.
- storage:
  - searchable;
  - physical index, initially `exact`.

### Rule and binding operations

Support the following operator actions:

- validate draft rule;
- create rule;
- update rule;
- enable/disable rule;
- delete rule with explicit confirmation;
- delete with `purge_vectors` only after a separate explicit confirmation;
- backfill a selected rule binding;
- analyze dirty work filtered by rule/binding;
- process pending maintenance work;
- retry/cancel individual maintenance work items.

Risky actions should be explicit and confirmed. Avoid automatic purge, repair,
merge, or rebalance behavior.

## Target Tauri/API shape

Rename Console-facing commands from index terminology to rule terminology:

```text
admin_list_semantic_rules
admin_get_semantic_rule
admin_validate_semantic_rule
admin_create_semantic_rule
admin_update_semantic_rule
admin_set_semantic_rule_enabled
admin_delete_semantic_rule
admin_backfill_semantic_rule
admin_analyze_semantic_dirty_work
admin_list_semantic_maintenance_work
```

TypeScript service functions should mirror this naming:

```ts
listSemanticRules(input)
getSemanticRule(input)
validateSemanticRule(input)
createSemanticRule(input)
updateSemanticRule(input)
setSemanticRuleEnabled(input)
deleteSemanticRule(input)
backfillSemanticRule(input)
analyzeSemanticDirtyWork(input)
listSemanticMaintenanceWork(input)
```

Type names should be rule-native:

```ts
SemanticGenerationRule
SemanticGenerationRuleSummary
SemanticEmbeddingBinding
SemanticEmbeddingBindingSummary
SemanticRuleValidationDiagnostic
SemanticRuleStatus
SearchIndexStatus
BackfillSemanticRuleInput
BackfillSemanticRuleResponse
```

Maintenance and inference scope fields should use:

```text
semanticRuleId
embeddingBindingKey
```

not `semanticIndexId`.

## Implementation phases

### SGR10.1 — API inventory and generated Rust alignment

Tasks:

- Confirm `../mycel-api` source protos expose all SGR8 Admin/Client semantic
  rule APIs needed by Console.
- Run Console/Rust checks with `MYCEL_API_ROOT` pointed at sibling `mycel-api` so
  stale generated SDK artifacts are not used.
- Inventory all Console references to:
  - `SemanticIndex`;
  - `semantic_index_id`;
  - `listSemanticIndexes`;
  - `backfillSemanticIndex`;
  - index state labels.
- Decide whether any Rust SDK generated code must be refreshed locally for
  tests/build. Do not commit generated public SDK/API code unless explicitly
  approved.

Acceptance:

- A concrete replacement list exists before editing UI code.
- No API gaps are hidden by frontend-only mocks.

### SGR10.2 — Tauri rule command replacement

Tasks:

- Replace `src-tauri/src/commands/semantic.rs` index list command with rule
  lifecycle commands.
- Replace maintenance backfill/analyze/list request fields with
  `semantic_rule_id` and `embedding_binding_key`.
- Update inference scope structs in `commands/inference.rs` from
  `semantic_index_id` to `semantic_rule_id` where Console surfaces scopes.
- Register new commands in `src-tauri/src/lib.rs`.
- Keep Rust structs serde-friendly for TypeScript camelCase payloads.

Acceptance:

- Tauri command names and Rust structs use semantic rule terminology.
- Rust compile catches no remaining command references to removed proto fields.

### SGR10.3 — TypeScript services and domain types

Tasks:

- Replace `src/types/semantic.ts` with rule-native summary and full-rule types.
- Replace `src/types/semanticMaintenance.ts` backfill/analyze/list fields with
  rule/binding names.
- Update `src/services/adminService.ts` function names and invoke command names.
- Update service tests for the new invoke payloads.
- Keep TypeScript response types close to the daemon response shape so raw JSON
  debugging remains possible.

Acceptance:

- No exported TypeScript semantic service is named `Index` unless documenting a
  transitional internal compatibility field.
- `adminService.test.ts` validates rule-native command names and payloads.

### SGR10.4 — Rule list and health dashboard

Tasks:

- Update `SemanticPage.tsx` to call `listSemanticRules`.
- Render rule rows/cards with:
  - rule ID/key/display name;
  - enabled state;
  - rule state;
  - binding keys;
  - search-index status per binding;
  - maintenance backlog/error summary.
- Add domain/space filters and keep loading/error/empty states.
- Update `SpaceDetailPage` semantic shortcut to route into filtered
  `Intelligence / Semantic` view if routing/query state supports it.

Acceptance:

- Operators can identify degraded rules and binding search-index health without
  opening raw JSON.
- Empty states guide users toward creating a rule.

### SGR10.5 — Rule editor and validation preview

Tasks:

- Add a structured create/edit form or drawer.
- Support common node-type rule creation without requiring JSON paste.
- Support GQL selector fields with clear bounded-query warnings.
- Add dynamic binding rows for Intelligence Access profile/vector store refs.
- Call `validateSemanticRule` before create/update and show diagnostics.
- Block save on validation errors unless the daemon marks the rule valid.

Acceptance:

- Console can create, edit, and validate semantic generation rules.
- Validation diagnostics are visible with severity/path/message.

### SGR10.6 — Rule actions, maintenance, and usage

Tasks:

- Add enable/disable action buttons.
- Add delete confirmation, with a distinct `purge vectors` confirmation.
- Add binding-level backfill action requiring a selected binding key.
- Add analyze/process maintenance controls with rule/binding filters.
- Show work item list with retry/cancel actions.
- Surface usage summary if SGR8 Admin usage APIs are available through Console;
  otherwise add a documented placeholder and avoid fake data.

Acceptance:

- Risky actions are explicit and confirmed.
- Backfill targets a rule/binding, not an unqualified semantic resource.

### SGR10.7 — Tests and docs

Tasks:

- Update `SemanticPage.test.tsx` for:
  - rule list rendering;
  - validation diagnostics;
  - create/update happy path;
  - enable/disable/delete confirmations;
  - backfill with binding key;
  - maintenance work retry/cancel.
- Update `adminService.test.ts` and any Tauri command tests/mocks.
- Update Console-facing docs or README snippets if present.
- Ensure no user-facing Console text says semantic index.

Acceptance:

- Console semantic tests are rule-native.
- Grep confirms no user-facing legacy semantic-index terminology remains in the
  Semantic page, service names, or Tauri command names.

## Out of scope

- Daemon API redesign; SGR8 owns that.
- CLI behavior; SGR9 owns that.
- Broad Console visual redesign outside `Intelligence / Semantic`.
- Public SDK code commits unless explicitly approved.
- Automatic vector repair, merge, rebalance, or destructive cluster behavior.

## Validation

Minimum validation from `../mycel-console`:

```sh
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm test -- --runInBand
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run build
```

Recommended targeted checks:

```sh
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm test -- SemanticPage --runInBand
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm test -- adminService --runInBand
```

From `mycel` after docs changes:

```sh
make docs-check
git diff --check
```

## Completion checklist

- [ ] Tauri semantic commands are rule-native.
- [ ] TypeScript semantic service/types are rule-native.
- [ ] Semantic page lists rules and binding health.
- [ ] Structured editor supports create/edit/validate.
- [ ] Enable/disable/delete/backfill actions are implemented with confirmations.
- [ ] Maintenance work filters/actions use rule and binding terminology.
- [ ] Tests cover rule lifecycle, validation, health display, and risky actions.
- [ ] Console build and tests pass with `MYCEL_API_ROOT` pointing to sibling
      `mycel-api`.
