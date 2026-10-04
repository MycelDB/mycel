# Semantic Generation Rules Implementation Plan

## Status

Planned. This plan implements the replacement design in
[Semantic Generation Rules](../../design/semantic/semantic-generation-rules.md).

The product is not released, so this plan does **not** preserve backward
compatibility with the older semantic index API, CLI, storage shape, or public
terminology. Prefer direct replacement over aliases, migration shims, or dual
models.

## Goal

Replace the current `SemanticIndex`-centered implementation with semantic
generation rules: constrained, system-owned graph-reactive rules that maintain
embedding/vector records for selected graph targets.

Target model:

```text
graph change -> trigger filter -> target selector -> source assembly ->
Intelligence Access resolution -> embedding generation -> physical search index ->
usage accounting
```

The action is fixed: maintain embeddings. Rules do not execute arbitrary scripts
or mutate user graph content.

## Non-goals

- No compatibility aliases for old semantic index APIs/CLI/storage.
- No arbitrary semantic action runner.
- No automatic divergent PVC/vector repair behavior.
- No external vector backend requirement before `mycel-file` remains fast and
  safe enough.
- No generated public SDK/API commits unless explicitly approved.

## Existing implementation areas

Relevant current files/packages:

```text
internal/semantic/model/semantic.go
internal/semantic/service/*
internal/semantic/storage/*
internal/semantic/maintenance/*
internal/semantic/backfill/*
internal/semantic/search/*
internal/semantic/vectorstore/*
internal/daemon/api/admin/semantic_service.go
internal/daemon/api/admin/semantic_maintenance_service.go
internal/daemon/api/client/semantic_service.go
internal/cli/cmd/semantic.go
internal/gen/mycel/admin/v1/*semantic*.pb.go
internal/gen/mycel/client/v1/*semantic*.pb.go
docs/design/semantic/semantic-generation-rules.md
docs/design/admin/semantic.md
docs/design/admin/semantic-maintenance.md
docs/operations/cli/semantic.md
```

## Phase SGR0 — Finalize replacement surface and protobuf plan

Decisions:

- Replace public concept `SemanticIndex` with `SemanticGenerationRule`.
- Replace index-level endpoint/model fields with embedding bindings referencing
  Intelligence Access profiles and vector stores.
- Replace `RecordTypes` with explicit node-type/label selector.
- Replace `RootQuery` with bounded selector/source GQL fields.
- Replace index-level purpose with binding-level purpose.
- Make work items binding-aware.
- Add physical search-index status concepts.
- Split shared profile, credential, grant, policy, decision, and usage APIs into
  an explicit Intelligence Access surface.

Tasks:

- Update source protobufs in `github.com/myceldb/mycel-api` if API changes are
  approved for this tranche.
- Regenerate daemon stubs under `internal/gen/` only after source protobufs are
  updated.
- Keep daemon-local generated stubs deferred until semantic model/storage/runtime
  replacement begins, unless this tranche is expanded to include compile fixes.
- Keep public SDK regeneration as a later handoff unless a downstream compile
  check requires it.

Acceptance:

- API shape is agreed before implementation touches generated files.
- No compatibility aliases are introduced.

Validation:

```sh
git diff --check
```

## Phase SGR1 — Replace semantic domain model

Tasks:

- Replace or rename `SemanticIndex` model with `SemanticGenerationRule`.
- Add:
  - `SemanticTriggerPolicy`;
  - `SemanticTargetSelector`;
  - `SemanticSourceAssemblyPolicy`;
  - `SemanticEmbeddingBinding`;
  - owner/on-behalf-of attribution fields that resolve through Intelligence Access;
  - `SemanticMaintenancePolicy`;
  - `SemanticStoragePolicy`.
- Remove/demote old fields:
  - `ModelEndpointID`;
  - `ModelID`;
  - `ModelEndpointCapabilityID` from user-authored rules;
  - index-level `Purpose`;
  - advisory `RecordTypes`;
  - ambiguous `RootQuery`.
- Add binding-aware record/work identifiers:
  - rule ID;
  - binding key or binding ID;
  - target node ID.
- Update semantic model tests.

Acceptance:

- Internal code no longer treats endpoint/model as user-authored semantic rule
  fields.
- Purpose is binding-scoped.
- Rule/binding IDs are available everywhere work and records need attribution.

Validation:

```sh
go test ./internal/semantic/model ./internal/semantic/service -count=1
```

## Phase SGR2 — Rewrite storage managers around rules and bindings

Tasks:

- Replace space semantic storage methods:
  - `UpsertSemanticIndex` -> `UpsertSemanticRule`;
  - `ListSemanticIndexes` -> `ListSemanticRules`;
  - `DeleteSemanticIndex` -> `DeleteSemanticRule`.
- Store rule definitions under rule-oriented layout:

  ```text
  graphs/<space_id>/semantic/rules/<rule_id>/rule.json
  ```

- Maintain loaded-state indexes:
  - by rule ID;
  - by `(space_id, domain_id, key)`;
  - by enabled state;
  - by trigger labels/events;
  - by profile/vector-store references.
- Make maintenance work state keyed by:

  ```text
  rule_id + binding_key + target_node_id
  ```

- Update WAL/raft semantic space and maintenance mutations for rule/binding
  names and payloads.
- Remove old semantic-index storage files/formats instead of preserving read
  compatibility.

Acceptance:

- A rule can be created, listed, updated, deleted, and raft/WAL-applied.
- Work items are binding-aware.
- Storage tests cover loaded-state indexes and delete/purge behavior.

Validation:

```sh
go test ./internal/semantic/storage ./internal/semantic/service -count=1
```

## Phase SGR3 — Rule validation and selector compilation

Tasks:

- Add rule validator.
- Validate trigger policy:
  - known events;
  - normalized labels;
  - non-negative debounce.
- Implement `node_type` selector:
  - labels/types required unless rule explicitly accepts all nodes;
  - label matching consistent with graph model.
- Implement bounded selector GQL validation using graph-context automation
  hardening patterns:
  - compile/inspect query;
  - read-only;
  - explicit `FETCH FIRST`;
  - target alias required;
  - relationship patterns labeled;
  - max row bound enforced.
- Keep `context_query` source assembly optional/future if too large for first
  implementation; if included, use same validation rules.
- Add validation diagnostics suitable for console display.

Acceptance:

- Invalid rules fail before maintenance/inference work is enqueued.
- Selector GQL cannot run unbounded writes or ambiguous target selection.
- Tests cover node-type selector and GQL validation failures.

Validation:

```sh
go test ./internal/semantic/... ./internal/automation/... -count=1
```

## Phase SGR4 — Analyzer rewrite: trigger and target selection

Tasks:

- Update graph dirty event analyzer to load enabled semantic rules.
- Apply cheap trigger filter before selector evaluation.
- Resolve targets via:
  - node-type/label selector;
  - bounded selector GQL if enabled.
- For every target and enabled embedding binding, upsert dirty work keyed by:

  ```text
  rule_id + binding_key + target_node_id
  ```

- Preserve debounce/coalescing behavior using rule/binding-specific cooldown.
- Save checkpoints per rule/binding.
- Remove `SemanticIndex` assumptions from analyzer tests.

Acceptance:

- Graph changes enqueue one work item per affected rule/binding/target.
- Multiple bindings for one rule produce independent work.
- Repeated dirty events coalesce by rule/binding/target.
- Deleted nodes tombstone binding records or refresh affected subtree roots.

Validation:

```sh
go test ./internal/semantic/maintenance -count=1
```

## Phase SGR5 — Source assembly and embedding generation rewrite

Tasks:

- Update backfill/worker input from semantic index to rule + binding.
- Assemble source from rule source policy:
  - `self`;
  - `subtree`;
  - included/excluded properties;
  - max depth;
  - minimum text length.
- Resolve embedding through binding Intelligence Access profile only.
- Reuse Intelligence Access path used by graph automations:
  - service actor;
  - on-behalf-of rule owner;
  - profile resolution;
  - credential grants allowing background use;
  - Intelligence Access policies;
  - denial diagnostics.
- Store resolved endpoint/model/capability/credential/policy details only as
  vector-record and usage provenance.
- Update idempotency key:

  ```text
  rule_id + binding_key + target_node_id + source_mode + source_hash + vector_space_key
  ```

- Tombstone latest record when source is empty/below minimum.

Acceptance:

- Embedding generation never directly uses user-authored endpoint/model IDs.
- Same source hash/binding skips provider calls unless force/backfill requests
  refresh.
- Usage and failures are attributable to rule and binding.

Validation:

```sh
go test ./internal/semantic/backfill ./internal/semantic/maintenance ./internal/inference/... -count=1
```

## Phase SGR6 — Physical fast search index for semantic search

Tasks:

- Add per-rule/per-binding physical search-index abstraction.
- Minimum implementation:
  - latest live record map;
  - normalized vector array or mmap-friendly equivalent;
  - record ID -> target node ID lookup;
  - dimensions/vector-space metadata;
  - rebuild from durable vector records.
- Update `mycel-file` backend so search does not scan all historical records on
  every query.
- Update vector upsert/tombstone/purge to update/invalidate loaded physical
  search indexes.
- Add search-index status:
  - ready/building/degraded;
  - record count;
  - last rebuild time;
  - last error.
- Add bounded missing-index behavior:
  - small indexes may rebuild synchronously;
  - large missing/corrupt indexes return clear degraded/fail-closed status rather
    than silently doing unbounded full scan.
- Leave ANN/HNSW-style implementation as future unless needed for first
  performance target.

Acceptance:

- Search path uses physical per-binding indexes.
- Tests prove historical tombstoned records are not searched.
- Tests prove search does not require unbounded full segment scan after index is
  built.
- Physical search index is rebuildable after deletion/corruption.

Validation:

```sh
go test ./internal/semantic/vectorstore ./internal/semantic/search -count=1
```

## Phase SGR7 — Search planner rewrite

Tasks:

- Select enabled rule bindings with search purpose.
- Resolve query embedding through the binding profile/access-control path.
- Search physical per-binding vector indexes.
- Merge/rank candidates across selected bindings.
- Load visible graph nodes after candidate selection.
- Include warnings for skipped/degraded bindings.
- Ensure query embedding usage is still recorded per request even if result cache
  or search-index cache is used.

Acceptance:

- Semantic search is fast-index-backed and binding-aware.
- Access and policy denial are visible as structured warnings/errors.
- Search result provenance includes rule/binding/record IDs.

Validation:

```sh
go test ./internal/semantic/search ./internal/daemon/api/client -count=1
```

## Phase SGR8 — Admin and client API replacement

Tasks:

- Replace Admin semantic API around rules:
  - list rules;
  - get rule;
  - validate rule;
  - create/update rule;
  - enable/disable rule;
  - delete rule with explicit vector purge option;
  - backfill rule or binding;
  - list rule work/status;
  - summarize rule/binding usage.
- Replace Client semantic APIs to list searchable rules/bindings and run search.
- Remove old semantic index API terminology from daemon adapters.
- Update authorization:
  - rule management requires semantic manage;
  - search/list requires domain visibility and semantic search;
  - maintenance requires semantic manage/maintenance capability.
- Update API mapping tests.

Acceptance:

- Daemon API no longer exposes semantic-index-as-primary-resource terminology.
- API responses include rule/binding status and usage-ready identifiers.
- Delete/purge is explicit and reference-safe.

Validation:

```sh
go test ./internal/daemon/api/admin ./internal/daemon/api/client -count=1
```

## Phase SGR9 — CLI replacement

Tasks:

- Replace `semantic index` commands with `semantic rule` commands:
  - `semantic rule list`;
  - `semantic rule get`;
  - `semantic rule validate`;
  - `semantic rule create`;
  - `semantic rule update`;
  - `semantic rule enable`;
  - `semantic rule disable`;
  - `semantic rule delete`;
  - `semantic rule backfill`.
- Keep maintenance commands but rename/help text to rule terminology where
  appropriate.
- Remove old semantic index command help/docs instead of preserving aliases.
- Add JSON/YAML file input for structured rule definitions.

Acceptance:

- CLI uses semantic generation rule terminology end-to-end.
- Commands are explicit and safe by default.
- Backfill/process operations require explicit scope/rule/binding arguments.

Validation:

```sh
go test ./internal/cli/cmd -run Semantic -count=1
```

## Phase SGR10 — Console semantic rule authoring

Tasks:

- Update `mycel-console` `Intelligence / Semantic` page to manage rules, not
  legacy indexes.
- Add structured rule editor:
  - scope;
  - trigger events/labels/debounce;
  - node-type selector;
  - bounded GQL selector when enabled;
  - source assembly;
  - embedding bindings from Intelligence Access profiles/vector stores;
  - validation preview.
- Show per-rule and per-binding:
  - status;
  - backlog;
  - latest errors;
  - physical search-index status;
  - token usage;
  - backfill/process controls.
- Keep space page contextual shortcut into filtered global Semantic page.

Acceptance:

- Console can create/edit/validate/delete semantic generation rules.
- Operators can see usage and search-index health without opening raw JSON.
- Risky actions are explicit and confirmed.

Validation:

```sh
cd ../mycel-console
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm test -- --runInBand
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run build
```

## Phase SGR11 — Documentation cleanup

Tasks:

- Update design docs:
  - `docs/design/semantic/README.md`;
  - `docs/design/admin/semantic.md`;
  - `docs/design/admin/semantic-maintenance.md`;
  - `docs/design/api/semantic.md`.
- Update operations docs:
  - `docs/operations/cli/semantic.md`;
  - console docs if present.
- Remove or archive stale semantic index terminology where it is no longer true.
- Update examples for semantic rule JSON.

Acceptance:

- Operator docs describe semantic generation rules, not old semantic indexes.
- Search-index storage/caching behavior is documented.
- No docs imply compatibility aliases exist.

Validation:

```sh
make docs-check
git diff --check
```

## Phase SGR12 — End-to-end validation

Add/refresh tests covering:

- rule create/update/delete/list;
- node-type target selector;
- bounded GQL selector validation;
- dirty event -> rule/binding work item;
- debounce/coalescing by rule/binding/target;
- source hash idempotency;
- multi-binding generation;
- access denial before provider call;
- usage event attribution by rule/binding;
- vector tombstones;
- physical search-index rebuild;
- semantic search over fast index;
- raft/WAL replication of rules/work/search metadata where applicable.

Normal validation:

```sh
make test
make docs-check
git diff --check
```

Raft-sensitive validation when semantic raft/WAL changes are substantial:

```sh
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
```

Destructive/system tests remain explicit only and are not part of `make test`.

## Rollout notes

Because the product is unreleased:

- do not add compatibility shims unless they simplify implementation internally;
- if old test fixtures break, rewrite them to semantic generation rules;
- if old docs become misleading, delete or replace the terminology;
- if existing local developer data becomes incompatible, document how to reset
  semantic metadata/vector directories.

## Open questions

- Should first implementation allow GQL selectors, or ship node-type selectors
  first and add GQL after shared automation validation is extracted?
- Should binding identity be a stable UUID, a stable key, or both?
- What threshold defines a "small" physical search index eligible for synchronous
  rebuild on first query?
- Should source assembly be shared once per target across all bindings in a
  worker pass?
- Should semantic rules have explicit owner principal, creator principal, or both
  as first-class fields?
