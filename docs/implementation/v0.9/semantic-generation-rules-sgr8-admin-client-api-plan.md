# SGR8 Semantic Generation Rules Admin and Client API Plan

## Status

Planned. This tranche follows SGR7 rule-native, binding-aware semantic search
planning.

## Goal

Replace daemon Admin and Client semantic API adapters so semantic generation
rules are the primary API resource. Public semantic search/listing should expose
rule and binding terminology, while Admin APIs should support rule lifecycle,
validation, backfill, work/status inspection, explicit purge behavior, and usage
summaries.

SGR8 is the API boundary cleanup tranche. After SGR8, daemon adapters should no
longer present `SemanticIndex` as the primary semantic resource, although
transitional internal wrappers may remain until later cleanup if they are still
needed by storage/runtime tests.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel-api` | Source protobuf updates for Admin/Client semantic rule APIs, if not already complete from SGR0 or if fields need adjustment. |
| `mycel` | Regenerate daemon-local stubs only when source protobufs are ready; replace Admin/Client adapters and mapping tests. |
| `mycel-console` | Out of scope except downstream compile notes; console UI comes in SGR10. |
| `mycel-rust-sdk` | Out of scope unless explicitly approved to regenerate public SDK code. |

## Primary files

Source API repository:

```text
../mycel-api/api/proto/mycel/admin/v1/semantic.proto
../mycel-api/api/proto/mycel/admin/v1/semantic_maintenance.proto
../mycel-api/api/proto/mycel/client/v1/semantic.proto
../mycel-api/api/proto/mycel/client/v1/query.proto
../mycel-api/api/proto/mycel/common/v1/intelligence_access.proto
```

Daemon generated/API files:

```text
internal/gen/mycel/admin/v1/*semantic*.pb.go
internal/gen/mycel/client/v1/*semantic*.pb.go
internal/daemon/api/admin/semantic_service.go
internal/daemon/api/admin/semantic_maintenance_service.go
internal/daemon/api/client/semantic_service.go
internal/daemon/api/client/query_service.go
internal/daemon/api/admin/*semantic*_test.go
internal/daemon/api/client/*semantic*_test.go
internal/semantic/service/types.go
internal/semantic/service/module.go
internal/semantic/model/semantic.go
```

Likely new/renamed adapter helpers:

```text
internal/daemon/api/admin/semantic_rule_mapping.go
internal/daemon/api/admin/semantic_rule_mapping_test.go
internal/daemon/api/client/semantic_rule_mapping.go
internal/daemon/api/client/semantic_rule_mapping_test.go
```

## Current behavior

After SGR7, internal search is rule-native, but public daemon adapters remain
transitional:

- Client semantic APIs still use `semantic_index_id` request/response concepts.
- Admin semantic APIs still expose index-oriented create/list/update behavior.
- Maintenance/backfill APIs still carry transitional `SemanticIndexID` fields in
  some request/response paths.
- Search result provenance has rule/binding data internally, but public response
  fields cannot expose it cleanly until generated APIs are replaced.

## Target API shape

### Admin semantic rule lifecycle

Admin APIs should support:

- list rules;
- get rule;
- validate rule without persisting;
- create rule;
- update rule;
- enable/disable rule;
- delete rule;
- delete rule with explicit vector purge option;
- backfill rule;
- backfill one binding;
- list rule/binding work and status;
- list rule/binding physical search-index status;
- summarize rule/binding usage.

Recommended Admin request names:

```text
ListSemanticRulesRequest
GetSemanticRuleRequest
ValidateSemanticRuleRequest
CreateSemanticRuleRequest
UpdateSemanticRuleRequest
SetSemanticRuleEnabledRequest
DeleteSemanticRuleRequest
BackfillSemanticRuleRequest
ListSemanticRuleWorkRequest
GetSemanticRuleStatusRequest
ListSemanticRuleUsageRequest
```

Recommended resource fields:

```text
semantic_rule_id
space_id
domain_id
key
display_name
description
enabled
trigger
selector
source
embeddings[]
maintenance
storage
owner_principal_id
created_by_principal_id
created_at
updated_at
metadata
```

Embedding binding fields:

```text
key
purpose
intelligence_profile
intelligence_profile_id
vector_store
vector_store_id
enabled
metadata
status
usage_summary
search_index_status
```

### Client semantic APIs

Client APIs should support safe read/search behavior:

- list searchable semantic rules/bindings visible in a domain;
- run semantic search using optional rule and binding filters;
- return rule/binding/record provenance with each result;
- return structured warnings from SGR7.

Recommended Client request names:

```text
ListSearchableSemanticRulesRequest
SemanticSearchRequest
```

`SemanticSearchRequest` should use rule terminology:

```text
space_id
domain_id
query
semantic_rule_ids[]
embedding_binding_keys[]
limit
min_score
```

`SemanticSearchResult` should include:

```text
node_id
node
score
semantic_rule_id
embedding_binding_key
record_id
matched_records[]
source_hash
source_mode
vector_space_key
model_label/provider provenance where safe
```

Warnings should be structured, not only strings:

```text
code
message
semantic_rule_id
embedding_binding_key
retryable
```

## Protobuf and generation policy

`mycel-api` owns protobuf source. Do not hand-edit generated protobuf files.

Implementation order:

1. Confirm SGR0 source protobufs already contain the required SGR8 messages and
   services.
2. If source protobufs need changes, update `../mycel-api` first and run its
   validation.
3. Regenerate daemon-local stubs under `internal/gen/` only from source
   protobufs.
4. Do not regenerate/commit public Rust SDK or console generated API code unless
   explicitly approved.

If generated daemon stubs are too large for the first SGR8 commit, split SGR8
into:

- SGR8a: source protobuf/API contract;
- SGR8b: daemon-local generation and adapters;
- SGR8c: adapter tests and old semantic-index adapter removal.

## Admin adapter mapping

Map API messages directly to `domainsemantic.SemanticGenerationRule`:

- normalize keys and binding keys with existing model helpers;
- run `ValidateSemanticGenerationRule` for validation requests;
- run `ValidateSemanticGenerationRuleForStorage` before persistence;
- use `UpsertSemanticRule`, `ListSemanticRules`, and `DeleteSemanticRule`;
- use rule/binding IDs for backfill and maintenance requests;
- keep delete/purge explicit and default to no vector purge.

Delete behavior:

```text
DeleteSemanticRule(rule_id, purge_vectors=false)
```

Rules:

- `purge_vectors=false` deletes rule metadata and leaves derived vectors for
  explicit future cleanup or forensic inspection according to storage behavior;
- `purge_vectors=true` must be explicit, authorized, and reference-safe;
- no automatic cross-node repair, merge, overwrite, or divergent PVC behavior.

## Client adapter mapping

Update `internal/daemon/api/client/semantic_service.go` so public client APIs no
longer resolve `semantic_index_id` as the primary search filter.

Client search should pass rule-native inputs to semantic service:

```go
daemonsemantic.SearchInput{
    SpaceID:             spaceID,
    DomainID:            domainID,
    SemanticRuleIDs:     ruleIDs,
    EmbeddingBindingKey: bindingKey,
    Text:                query,
    Limit:               limit,
    MinScore:            minScore,
    ActorPrincipalID:    actorID,
}
```

Response mapping should preserve SGR7 result ordering after visibility filtering
and expose:

- rule ID;
- binding key;
- record ID;
- matched record IDs/bindings when available;
- structured warnings.

## Authorization

Use existing domain visibility and semantic gates as the minimum baseline.

Rules:

- Admin rule lifecycle requires semantic management permission/capability.
- Backfill/process/retry/cancel requires semantic maintenance permission.
- Client list/search requires visible domain and semantic search permission.
- Domain-level exclusion from semantic search/indexing remains fail-closed.
- Intelligence Access denials remain separate from API authorization and should
  surface as sanitized warnings/errors from the search/generation path.

If capability names are still transitional, keep implementation centralized in
small helper functions so SGR9/SGR10 can reuse them.

## Maintenance and status APIs

Rule status responses should include:

- rule enabled state;
- binding enabled state;
- pending/running/failed work counts by binding;
- last successful generation timestamp when available;
- latest sanitized error;
- physical search-index state:
  - ready/building/degraded/missing/error;
  - live record count;
  - last rebuild time;
  - last error.

Use SGR2 storage state methods:

```go
ListSemanticRuleStates(ctx)
ListSearchIndexStates(ctx)
ListDirtyWorkItems(ctx)
```

Do not create automatic repair or rebuild behavior in status/list APIs.

## Usage summary API

Summaries should be grouped by:

```text
space_id + domain_id + semantic_rule_id + embedding_binding_key
```

Include safe aggregate fields only:

- request count;
- success/failure count;
- input/output/total tokens;
- last usage timestamp;
- endpoint/model labels where safe;
- denied count if available.

Do not expose credential secrets, bearer tokens, raw provider request bodies, or
plaintext credentials.

## Transitional removal policy

SGR8 should remove old semantic-index terminology from daemon API adapters as the
primary path. Internal transitional wrappers can remain only where needed by
runtime/storage tests and should be marked for SGR11 cleanup.

Do not add public compatibility aliases unless explicitly approved. The product
is unreleased, so prefer direct replacement.

## Tests

Add/update tests for:

- Admin create/update/list/get semantic rules;
- Admin validate returns structured diagnostics without persisting;
- Admin enable/disable updates rule state safely;
- Admin delete defaults to no vector purge;
- Admin delete with purge requires explicit flag and calls purge path;
- Admin backfill rule and binding maps to rule-native service input;
- status APIs aggregate work/state/search-index state by rule/binding;
- client list returns searchable rules/bindings only;
- client search passes rule/binding filters to semantic service;
- client search response includes rule/binding/record provenance;
- structured warnings are mapped from SGR7 warnings;
- domain semantic exclusion fails closed;
- unauthorized rule management/search fails with the expected gRPC status.

## Implementation steps

1. Inventory SGR0 source protobufs against this SGR8 target shape.
2. Update `mycel-api` protobuf source only if fields/services are missing.
3. Run `cd ../mycel-api && make test`.
4. Regenerate daemon-local protobuf stubs under `internal/gen/` from the updated
   source API.
5. Replace Admin semantic adapter mapping with rule-native lifecycle methods.
6. Replace Client semantic adapter mapping with searchable-rule listing and
   rule-native search request/response mapping.
7. Update semantic maintenance/backfill adapter request mapping to rule/binding
   fields.
8. Remove old semantic-index adapter tests or rewrite them to rule terminology.
9. Add API authorization/status/usage tests.
10. Run validation.

## Acceptance

- Daemon Admin semantic API exposes semantic generation rules as the primary
  resource.
- Daemon Client semantic API lists/searches semantic rules and bindings.
- Search responses expose rule/binding/record provenance and structured warnings.
- Rule validation diagnostics are available through Admin API.
- Delete/purge behavior is explicit and safe by default.
- API adapters no longer require legacy `SemanticIndex` resources for normal
  semantic rule/search workflows.

## Validation

```sh
cd ../mycel-api && make test
go test ./internal/daemon/api/admin ./internal/daemon/api/client -count=1
go test ./internal/semantic/... ./internal/inference/... -count=1
make docs-check
git diff --check
```

If daemon-local generation is included, also run the repository's relevant proto
generation/check target and include the exact command in the commit notes.

## Out of scope

- CLI replacement; covered by SGR9.
- Console semantic rule authoring; covered by SGR10.
- Broad documentation cleanup; covered by SGR11.
- Public Rust SDK generation or console API client generation unless explicitly
  approved.
- Automatic vector repair/rebalance/merge behavior.
