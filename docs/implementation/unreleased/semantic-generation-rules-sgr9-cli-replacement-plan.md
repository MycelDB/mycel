# SGR9 Semantic Generation Rules CLI Replacement Plan

## Status

Implemented locally. This tranche follows SGR8 rule-native Admin and Client daemon APIs.

## Goal

Replace the `mycel semantic index` CLI surface with semantic generation rule
commands that call the SGR8 rule-native Admin, Client, and maintenance APIs.
After SGR9, CLI users should no longer see `SemanticIndex` as the public
semantic resource. Semantic search, maintenance, and backfill commands should
use rule and embedding binding terminology end-to-end.

SGR9 is a breaking CLI cleanup. The product is unreleased, so do not preserve
old `semantic index` command aliases or legacy help text unless a later tranche
explicitly asks for temporary compatibility.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel` | CLI command replacement, CLI tests, operation docs for semantic commands. |
| `mycel-api` | Out of scope unless SGR8 reveals missing API fields. Source protobufs should already be rule-native. |
| `mycel-console` | Out of scope; console rule authoring is SGR10. |
| `mycel-rust-sdk` | Out of scope unless explicitly approved to regenerate public SDK code. |

## Primary files

```text
internal/cli/cmd/semantic.go
internal/cli/cmd/semantic_daemon_test.go
internal/cli/cmd/admin_semantic_test.go
internal/cli/cmd/admin_inference_test.go
internal/cli/cmd/root.go
internal/cli/cmd/admin.go
docs/operations/cli/semantic.md
docs/operations/cli/inference.md
```

Related daemon/API files that should not need large changes:

```text
internal/daemon/api/admin/semantic_service.go
internal/daemon/api/admin/semantic_maintenance_service.go
internal/daemon/api/client/semantic_service.go
internal/semantic/model/semantic.go
internal/semantic/service/types.go
```

## Current state after SGR8

- Daemon Admin and Client semantic APIs are rule-native.
- Internal search and vector maintenance are rule/binding aware.
- CLI code still contains legacy `semantic index` commands and tests.
- Some CLI compatibility shims may exist to keep older tests compiling during
  the SGR8 transition; SGR9 should remove or minimize these rather than deepen
  them.
- Current full CLI tests fail where they still assert legacy semantic-index
  list/delete behavior.

## Target CLI shape

### Command tree

Replace:

```text
mycel semantic index ...
```

with:

```text
mycel semantic rule list
mycel semantic rule get RULE
mycel semantic rule validate [--file FILE]
mycel semantic rule create --file FILE
mycel semantic rule update RULE --file FILE
mycel semantic rule enable RULE
mycel semantic rule disable RULE
mycel semantic rule delete RULE [--purge-vectors]
mycel semantic rule backfill RULE --binding BINDING [--node NODE ...] [--force] [--limit N]
```

Keep semantic search, but make it rule-native:

```text
mycel semantic search --space-id SPACE --domain DOMAIN --text TEXT \
  [--rule RULE ...] [--binding BINDING] [--limit N] [--min-score SCORE]
```

Keep semantic maintenance, but rename help, flags, and output fields to rule
terminology:

```text
mycel semantic maintenance status
mycel semantic maintenance list [--rule RULE] [--binding BINDING] [--status STATUS]
mycel semantic maintenance analyze [--rule RULE] [--binding BINDING] [--limit N]
mycel semantic maintenance process [--limit N]
mycel semantic maintenance retry WORK_ITEM_ID
mycel semantic maintenance cancel WORK_ITEM_ID
```

Do not keep `semantic index` as a hidden alias in this tranche unless needed
for a very short-lived internal test bridge that is removed before acceptance.

### Rule file input

Add JSON/YAML file input for structured rules. The CLI should accept the SGR8
Admin proto shape as closely as practical, with small CLI conveniences allowed
only if they map deterministically to the proto request.

Recommended minimal file shape:

```yaml
space_id: "..."
domain_id: "..."
key: notes-search
display_name: Notes Search
enabled: true
trigger:
  events: [changed]
  labels: [Note]
  dirty_cooldown: 30s
selector:
  mode: node_type
  labels: [Note]
source:
  mode: subtree
  include_properties: [title, body]
embeddings:
  - key: search
    purpose: search
    intelligence_profile: embedding-default
    vector_store: mycel-file
    enabled: true
storage:
  searchable: true
  physical_index: exact
```

Implementation notes:

- Support `--file -` for stdin.
- Infer JSON vs YAML from extension when possible, and fall back to YAML parser
  for extensionless/stdin input if existing dependencies support it.
- Resolve human-friendly refs where CLI already has resolvers:
  - `--space-id` or file `space_id`;
  - domain key/ID to domain ID;
  - intelligence profile key/ID;
  - vector store key/ID.
- Fail closed on ambiguous or missing refs.
- Print validation diagnostics without persisting for `validate`.

## Implementation phases

### SGR9.1 — Inventory and remove transitional CLI assumptions

Tasks:

- Inventory all `SemanticIndex`, `semantic index`, `--index`, and
  `semantic_index` references under `internal/cli/cmd` and semantic CLI docs.
- Remove generated compatibility aliases that only exist for CLI tests if they
  are no longer needed by daemon code.
- Decide whether `--index` in `semantic search` is immediately removed or
  replaced by `--rule` with no alias. The preferred SGR9 target is no alias.

Acceptance:

- A grep for user-facing CLI help strings does not expose `semantic index` as a
  supported command surface.

### SGR9.2 — Rule lifecycle commands

Tasks:

- Implement `semantic rule list/get/validate/create/update/enable/disable/delete`.
- Map create/update/validate to SGR8 Admin semantic rule requests.
- Resolve domain refs before create/update when a file or flag supplies a domain
  key.
- Make delete safe by default:
  - no vector purge unless `--purge-vectors` is set;
  - no reference/policy/grant purge flags unless such SGR8 behavior exists;
  - actionable error messages for not-found and validation failures.

Acceptance:

- Rule lifecycle commands call `AdminSemanticService` rule-native RPCs only.
- JSON output is the raw rule-native response or summary, not a legacy index
  projection.

### SGR9.3 — Search command replacement

Tasks:

- Replace `--index` with `--rule` and optional `--binding`.
- Resolve rule keys to semantic rule IDs using `Client SemanticService` rule
  listing or Admin APIs when operator-only data is required.
- Populate `SemanticSearchRequest.semantic_rule_ids` and
  `embedding_binding_key`.
- Print rule/binding provenance in text output when available.
- Preserve structured warnings and string warnings in JSON output.

Acceptance:

- Search no longer accepts or emits `semantic_index_id` in CLI-owned text/help.
- Search fails closed if no searchable rule/binding exists, matching SGR7/SGR8
  behavior.

### SGR9.4 — Maintenance and backfill commands

Tasks:

- Move backfill from `semantic index backfill` to `semantic rule backfill`.
- Require explicit rule ID/key and embedding binding key for backfill unless the
  rule has exactly one enabled embedding binding and the command documents that
  default.
- Update maintenance `analyze` and `list` filters from index to rule/binding.
- Ensure output includes `semantic_rule_id`, `embedding_binding_key`, status,
  attempts, and actionable error text.

Acceptance:

- Backfill/process operations require explicit scope/rule/binding arguments or
  a documented single-binding default.
- Maintenance commands do not print `semantic_index_id` except when inspecting
  transitional internal data for debugging, which should not be part of normal
  output.

### SGR9.5 — Tests and docs

Tasks:

- Replace legacy semantic-index CLI tests with rule-native tests:
  - create/list/get/validate/update/enable/disable/delete;
  - search by rule key and binding;
  - backfill rule/binding;
  - maintenance analyze/list filters.
- Update `admin_inference_test.go` references that created semantic indexes for
  Intelligence Access grant/policy tests to create semantic rules instead.
- Update `docs/operations/cli/semantic.md` to rule terminology.
- Update `docs/operations/cli/inference.md` if examples still mention semantic
  indexes in Intelligence Access scopes.

Acceptance:

- `go test ./internal/cli/cmd -run Semantic -count=1` passes.
- Full `go test ./internal/cli/cmd -count=1` passes or any remaining failures
  are unrelated to semantic CLI replacement and documented.
- `make docs-check` passes.

## Data and access-control expectations

- CLI rule creation must reuse Intelligence Access profile/credential/grant/
  policy terminology.
- Grant/policy scope flags should use `--semantic-rule` and optional
  `--embedding-binding`, not `--semantic-index`.
- Rule search/backfill should not bypass daemon-side authorization or
  Intelligence Access resolution.
- Dangerous maintenance operations remain explicit and operator-only.

## Out of scope

- Console semantic rule authoring; covered by SGR10.
- Public SDK regeneration; only do this with explicit approval.
- Storage cleanup removing all internal `SemanticIndex` compatibility; later
  cleanup tranche.
- Changing SGR8 protobuf source unless implementation reveals a missing field.
- Adding destructive vector repair, merge, rebalance, or automatic PVC repair
  behavior.

## Validation

Minimum validation:

```sh
go test ./internal/cli/cmd -run Semantic -count=1
go test ./internal/daemon/api/admin ./internal/daemon/api/client -count=1
go test ./internal/semantic/... ./internal/inference/... -count=1
make docs-check
git diff --check
```

Recommended broader validation before merging:

```sh
go test ./internal/cli/cmd -count=1
make test
```

Do not run destructive Compose/K3s validation for SGR9 unless explicitly
requested.

## Completion checklist

- [ ] No supported CLI command path is named `semantic index`.
- [ ] Rule lifecycle commands are implemented and tested.
- [ ] Rule file create/update/validate supports JSON/YAML and stdin.
- [ ] Search uses `--rule`/`--binding` and exposes rule/binding provenance.
- [ ] Maintenance/backfill use rule/binding terminology and safe defaults.
- [ ] Intelligence Access scope flags use semantic rule terminology.
- [ ] Semantic CLI operation docs are updated.
- [ ] SGR8 compatibility shims that were only for CLI tests are removed or
      clearly marked for later generated-code cleanup.
