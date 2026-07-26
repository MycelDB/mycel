# GWL schema management implementation plan

## Goal

Move Mycel schema management to a GWL-first interface while keeping an internal compiled validation representation. Migrate Knot PKM to author and provision schemas through embedded GWL, convert its record types to `pkm.*`, remove the old template/JSON-schema assumptions, and ensure Mycel validates PKM graph mutations against the active domain schema.

No backwards compatibility with the old schema JSON API or Mycel template-era behavior is required.

## Scope

This plan covers two repositories:

- `myceldb/mycel`
- `knot_pkm/knot_pkm_server`

Out of scope for the first implementation unless explicitly added later:

- Admin UI schema editor
- multi-file GWL module/import syntax
- long-term schema migration framework
- backwards-compatible JSON schema public API

## Design reference

See:

```text
myceldb/mycel/docs/design/schema-management.md
knot_pkm/knot_pkm_server/docs/pkm-data-types.md
knot_pkm/knot_pkm_server/internal/pkmschema/knot-pkm.gwl
```

## Phase 1: Extend Mycel GWL model and parser

### Goals

Support the generic field types needed by Knot PKM and prepare GWL as the public schema source format.

### Work

- Add GWL/schema field types:
  - `date` for `YYYY-MM-DD` calendar dates
  - `object` or `map` for structured property bags
  - optionally `json` if arbitrary structured JSON should be distinct from `object`
- Update schema model field type constants.
- Update GWL parser to accept the new types.
- Update schema self-validation.
- Update runtime field validation:
  - `date` validates `YYYY-MM-DD`
  - `object`/`map` validates JSON object/map values
  - `json`, if added, validates any JSON-compatible scalar/array/object
- Add parser/model/validator tests.
- Update schema docs to describe extension types.

### Acceptance

- `mycel schema compile` accepts GWL containing `date` and `object`/`map` fields.
- Strict schema validation rejects invalid dates and non-object map fields.

## Phase 2: Replace schema storage record with GWL source records

### Goals

Make GWL the persisted source of truth for domain schemas.

### Work

- Replace or refactor current file-backed schema storage:
  - from `<domain-id>.json` canonical schema files
  - to records containing GWL source and metadata.
- Define persisted schema record fields:
  - `domain_id`
  - `version`
  - `mode`
  - `source_gwl`
  - `source_hash`
  - timestamps
- Remove public/storage reliance on JSON-authored schema records.
- On load, compile GWL into normalized schema model.
- Fail fast if a persisted GWL schema cannot compile.
- Add storage tests.

### Acceptance

- Schema storage persists GWL source.
- Restarting `myceld` reloads GWL and rebuilds usable schema state.
- JSON-authored schema storage is no longer the primary path.

## Phase 3: Add compiled schema and validation cache

### Goals

Add a hot-path validation cache so graph writes do not repeatedly scan/compile schema source.

### Work

- Add package, likely:

```text
internal/schema/compile
internal/schema/validation
```

- Define `CompiledSchema` with indexes such as:
  - node types by name
  - node types by label
  - node types by `record_type`
  - edge types by label
- Define compiled field validators for:
  - required fields
  - allowed fields
  - scalar/array type checks
  - enums
  - payload/meta fields
- Add validation cache:

```text
domain_id -> schema_hash -> compiled_schema
```

- Refactor schema service to update cache on put/delete/load.
- Refactor graph validation to use compiled cache instead of scanning raw schema model.
- Add tests for deduplication and validation correctness.

### Acceptance

- Multiple domains with the same GWL source can share a compiled schema entry by hash.
- Graph node/edge validation uses the compiled cache.
- Existing schema validation tests pass through the new compiled path.

## Phase 4: Make Mycel schema APIs GWL-first

### Goals

Make the API/interface layer accept and return GWL as the primary schema format.

### Work

- Update client/admin schema proto/API to use GWL source fields, for example:
  - `PutSchema(domain_id, gwl)`
  - `GetSchema(domain_id) -> gwl`
- Remove JSON schema as the primary public request/response shape.
- Regenerate Mycel protobuf code.
- Update Go SDK and Rust SDK schema clients if needed.
- Update CLI:
  - `mycel schema put schema.gwl`
  - `mycel schema get --domain ...` returns GWL by default
  - `mycel schema validate schema.gwl`
  - `mycel schema compile schema.gwl` may remain as a developer/debug command
- Remove or demote JSON schema CLI behavior.
- Update API/CLI tests.

### Acceptance

- Public schema put/get round-trips GWL source.
- Existing JSON schema public flow is removed or debug-only.
- CLI docs describe GWL as the source format.

## Phase 5: Add schema WAL/cluster logical operations

### Goals

Replicate schema changes consistently in clustered Mycel deployments.

### Work

- Add WAL record types:
  - schema put
- Schema delete is not currently exposed by the public schema API or storage interface. Do not add a delete WAL record unless schema deletion is reintroduced as a supported lifecycle operation.
- Register schema WAL appliers in the schema service module.
- On schema put, append a logical WAL record before applying local durable state when WAL is enabled.
- WAL apply path should:
  - persist GWL source record
  - compile GWL
  - update validation cache
- Add cluster/raft command integration if schema changes must route through raft groups.
- Add recovery tests.

### Acceptance

- Schema put survives restart through WAL-backed state.
- WAL replay rebuilds schema storage and validation cache.
- Cluster mode does not leave schema state local-only.

## Phase 6: Convert Knot PKM schema source to real GWL

### Goals

Create the real Knot PKM GWL schema and remove the old lightweight JSON schema source as the source of truth.

### Work

- Expand:

```text
knot_pkm/knot_pkm_server/internal/pkmschema/knot-pkm.gwl
```

- Define all selected `pkm.*` record types:
  - `pkm.journal`
  - `pkm.journal_entry`
  - `pkm.page`
  - `pkm.page_entry`
  - `pkm.task`
  - `pkm.blob`
  - `pkm.ai_conversation`
  - `pkm.ai_message`
  - `pkm.user_settings_root`
  - `pkm.feature_preferences`
  - `pkm.ai_processing_preferences`
  - `pkm.onboarding_profile`
  - `pkm.consent_record`
  - `pkm.ai_provider_key`
  - `pkm.ai_provider_setting`
  - `pkm.registration_info`
  - `pkm.registration_token`
  - `pkm.signup_audit_event`
  - `pkm.browser_session`
  - `pkm.steward_record`
  - `pkm.steward_interview_session`
  - `pkm.steward_suggestion`
  - `pkm.steward_profile_snapshot`
  - `pkm.prompt_document`
  - `pkm.manual_document`
  - `pkm.manual_section`
- Define edge types:
  - `contains`
  - `references`
  - `annotates`
  - `derived_from`
- Define hierarchy endpoint rules for `contains`.
- Update data type documentation as each type is finalized.
- Remove or replace `internal/pkmschema/schema.json` as the source of truth.
- Update code generation to read/compile GWL or derive constants from GWL.
- Regenerate constants with `pkm.*` record types.

### Acceptance

- Knot PKM has one authoritative embedded GWL schema.
- Generated constants use `pkm.*` names.
- Old `logseq.*` and `app.task` schema constants are removed unless explicitly retained as migration-only comments/docs.

## Phase 7: Provision embedded GWL into Mycel domains

### Goals

Knot PKM should submit embedded GWL to Mycel when creating/ensuring domains.

### Work

- Embed GWL source with Go `embed`.
- Refactor schema provisioning code in Knot PKM, especially around:

```text
internal/mycelclient/schema_daemon.go
```

- On registration/content/settings domain ensure:
  - load embedded GWL for the domain
  - call Mycel schema API with GWL source
  - fail startup/onboarding if schema put fails
- Decide whether to use one GWL for all PKM domains or separate domain-specific GWL sources.
- Remove provisioning of lightweight JSON schema documents.
- Add tests with myceld fixture proving active domain schema is installed.

### Acceptance

- User content domain receives GWL-authored PKM schema during provisioning.
- Mycel validates writes into the domain against the installed schema.
- No JSON schema provisioning path remains in Knot PKM.

## Phase 8: Refactor Knot PKM runtime record types to `pkm.*`

### Goals

Move all Knot PKM runtime writes/classification from legacy names to `pkm.*` names.

### Work

Replace runtime record types:

| Old | New |
|---|---|
| `logseq.journal` | `pkm.journal` |
| `logseq.journal_entry` | `pkm.journal_entry` |
| `logseq.page` | `pkm.page` |
| `logseq.page_entry` | `pkm.page_entry` |
| `app.task` / `pkm.task` mixed usage | `pkm.task` |
| `blob` | `pkm.blob` |

- Update server handlers and helpers:
  - journal creation/list/detail
  - page creation/list/detail
  - slash command support paths
  - task list/update paths
  - blob upload paths
  - references and search
  - chat/steward tool paths
- Update importer if it writes legacy names.
- Update client assumptions if UI checks legacy kinds.
- Update tests and fixtures.
- Remove `template_key` as a runtime classification source where possible.
- Remove old template compatibility helpers that are no longer needed.

### Acceptance

- New data is written with `pkm.*` record types only.
- Server/client tests pass without legacy type names.
- No runtime path requires Mycel templates for PKM content classification.

## Phase 9: Validate graph mutation paths against schema

### Goals

Ensure Knot PKM graph mutations produce schema-valid records and edges.

### Work

- Audit each PKM write path:
  - create journal
  - create journal entry
  - create page
  - create page entry
  - create task
  - create blob
  - create/update AI conversations/messages
  - create/update settings records
  - registration/auth records
  - steward records
  - prompt/manual records
- Ensure required properties are set before writes.
- Ensure edge labels/properties match schema, especially `contains.order`.
- Ensure update paths preserve required properties.
- Add integration tests that run with strict schema mode enabled.
- Prefer Mycel schema validation errors over duplicated PKM-side schema checks, while keeping user-friendly request checks.

### Acceptance

- Strict schema mode accepts normal PKM operations.
- Invalid PKM graph mutations fail reliably.
- Integration tests prove schema validation is active in the daemon-backed runtime.

## Phase 10: Cleanup old schema/template system assumptions

### Goals

Remove obsolete paths now that GWL + `pkm.*` is the source of truth.

### Work

- Remove old JSON schema source files if replaced by GWL.
- Remove old Mycel template provisioning code from Knot PKM.
- Remove `ensureAppTaskTemplate` and related compatibility functions where possible.
- Remove code that resolves PKM type from `TemplateID` before `record_type`.
- Remove old docs that instruct users to use templates or JSON schema as the primary flow.
- Update README/configuration/docs for GWL schema provisioning.

### Acceptance

- No PKM content flow depends on Mycel templates.
- Docs describe GWL/domain schemas accurately.
- Project-wide search has no remaining active `logseq.*` or `app.task` runtime writes except migration notes/tests intentionally marked for removal.

## Validation commands

Expected validation during implementation:

```sh
cd myceldb/mycel && go test ./...
cd myceldb/mycel-api && make test
cd myceldb/mycel-go-sdk && go test ./...
cd myceldb/mycel-rust-sdk && MYCEL_API_ROOT=/Users/martinbeauvais/Projects/knotbase/Knotbase/myceldb/mycel-api cargo test
cd knot_pkm/knot_pkm_server && go test ./...
```

If client-visible record type names change:

```sh
cd knot_pkm/knot_pkm_client && npm test -- --runInBand
```

Compose smoke after domain provisioning changes:

```sh
cd knot_pkm/knot_pkm_server
make compose-reset
make compose-up
make compose-load-dev-assets
make compose-smoke
```

## Open decisions

- Whether the public Mycel schema API should remove JSON fields entirely or leave a debug-only compiled-schema export.
- Whether schema source hash should be exact GWL hash, semantic normalized hash, or both.
- Whether Knot PKM should use one GWL source for all domains or separate GWL sources per domain.
- Whether `record_type` should remain a required enum property or eventually map to graph labels directly.
- Exact GWL syntax for `object`/`map` and `date` extension types.
