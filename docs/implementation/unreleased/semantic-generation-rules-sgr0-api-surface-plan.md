# SGR0 Semantic Generation Rules API Surface Plan

## Status

Implemented for the source protobuf/API surface in `mycel-api`. Daemon-local
stub regeneration and runtime adapter changes are intentionally deferred to later
SGR tranches so the current daemon remains functional until model/storage/runtime
replacement work begins. This is the tranche-specific plan for SGR0 from
[Semantic generation rules implementation plan](semantic-generation-rules-implementation-plan.md).

The product is not released, so this tranche should define a clean replacement
surface rather than preserving old semantic-index compatibility.

## Goal

Finalize the replacement API/model surface for semantic generation rules before
runtime implementation begins.

SGR0 should produce an agreed source-of-truth API shape in `mycel-api` and a
clear generation/implementation handoff for `mycel`, `mycel-console`, and
`mycel-rust-sdk`.

## Repositories in scope

| Repo | Scope |
| --- | --- |
| `mycel-api` | Source protobuf API changes. |
| `mycel` | Review generated-stub impact and plan daemon adapter changes; generate daemon stubs only if approved for this tranche. |
| `mycel-console` | No implementation yet; record API needs for later SGR10. |
| `mycel-rust-sdk` | No implementation yet unless explicitly included after API surface is finalized. |

## Current API surfaces to replace

Current semantic API source files:

```text
../mycel-api/api/proto/mycel/admin/v1/semantic.proto
../mycel-api/api/proto/mycel/admin/v1/semantic_maintenance.proto
../mycel-api/api/proto/mycel/client/v1/semantic.proto
../mycel-api/api/proto/mycel/common/v1/inference.proto
../mycel-api/api/proto/mycel/common/v1/intelligence_access.proto
../mycel-api/api/proto/mycel/admin/v1/intelligence_access.proto
```

Current terms to remove from public API:

- `SemanticIndex`
- `semantic_index_id`
- `ListSemanticIndexes`
- `UpsertSemanticIndex`
- `DeleteSemanticIndex`
- `BackfillSemanticIndex`
- `SemanticIndexState`

Replacement terms:

- `SemanticGenerationRule`
- `semantic_rule_id`
- `embedding_binding_key` / `semantic_binding_id`
- `ListSemanticRules`
- `GetSemanticRule`
- `ValidateSemanticRule`
- `CreateSemanticRule`
- `UpdateSemanticRule`
- `SetSemanticRuleEnabled`
- `DeleteSemanticRule`
- `BackfillSemanticRule`
- `SemanticRuleState`
- `SearchIndexState`

## API design principles

1. **Rules are user-authored.**
   - Rules contain trigger, selector, source assembly, embedding bindings,
     maintenance, and storage policy.
2. **Bindings choose embeddings.**
   - Embedding bindings reference Intelligence Access profiles and vector stores.
   - Rules do not contain raw endpoint/model/capability IDs.
3. **Resolved model/provider data is provenance only.**
   - Endpoint/model/capability/credential/policy IDs belong on vector records and
     usage events, not user-authored rule definitions.
4. **Selectors are explicit.**
   - Node-type/label selector is first-class.
   - GQL selector is explicit, bounded, and target-alias based.
5. **Source assembly is separate from target selection.**
6. **Maintenance and search status are observable.**
   - API should expose rule state, binding state, physical search-index state,
     work/backlog counts, and usage-ready IDs.
7. **No compatibility aliases.**
   - Replace old RPCs/messages instead of preserving semantic-index wrappers.

## Proposed client API

File:

```text
../mycel-api/api/proto/mycel/client/v1/semantic.proto
```

Service:

```proto
service SemanticService {
  rpc ListSemanticRules(ListSemanticRulesRequest) returns (ListSemanticRulesResponse);
  rpc SemanticSearch(SemanticSearchRequest) returns (SemanticSearchResponse);
}
```

### `ListSemanticRules`

Purpose: list searchable rules visible to a standard user for a space/domain.

```proto
message ListSemanticRulesRequest {
  string space_id = 1;
  string domain_id = 2;
  int32 page_size = 3;
  string page_token = 4;
  bool include_disabled = 5;
}

message ListSemanticRulesResponse {
  repeated SemanticGenerationRuleSummary rules = 1;
  string next_page_token = 2;
}
```

### `SemanticSearch`

```proto
message SemanticSearchRequest {
  string space_id = 1;
  string domain_id = 2;
  optional string semantic_rule_id = 3;
  optional string embedding_binding_key = 4;
  string query = 5;
  int32 limit = 6;
  optional double min_score = 7;
}
```

Notes:

- If `semantic_rule_id` is omitted, search uses all enabled searchable bindings
  in the domain that the caller can read.
- If `embedding_binding_key` is set, `semantic_rule_id` must also be set.
- Search response should include rule/binding provenance.

```proto
message SemanticSearchResult {
  string semantic_rule_id = 1;
  string embedding_binding_key = 2;
  string record_id = 3;
  string node_id = 4;
  double score = 5;
  Node node = 6;
  repeated string matched_chunk_ids = 7;
  string snippet = 8;
}
```

## Proposed shared semantic messages

The current client semantic proto can own the display-safe messages initially.
If reuse becomes awkward, SGR0 can introduce:

```text
../mycel-api/api/proto/mycel/common/v1/semantic.proto
```

Recommended shared/display messages:

```proto
message SemanticGenerationRuleSummary {
  string semantic_rule_id = 1;
  string key = 2;
  string display_name = 3;
  string description = 4;
  string space_id = 5;
  string domain_id = 6;
  bool enabled = 7;
  SemanticRuleState state = 8;
  repeated SemanticEmbeddingBindingSummary bindings = 9;
  SemanticRuleStatus status = 10;
}

enum SemanticRuleState {
  SEMANTIC_RULE_STATE_UNSPECIFIED = 0;
  SEMANTIC_RULE_STATE_ACTIVE = 1;
  SEMANTIC_RULE_STATE_BUILDING = 2;
  SEMANTIC_RULE_STATE_STALE = 3;
  SEMANTIC_RULE_STATE_DISABLED = 4;
  SEMANTIC_RULE_STATE_ERROR = 5;
}

message SemanticEmbeddingBindingSummary {
  string key = 1;
  string purpose = 2;
  string intelligence_profile_id = 3;
  string intelligence_profile_key = 4;
  string vector_store_id = 5;
  string vector_store_key = 6;
  bool enabled = 7;
  SearchIndexStatus search_index = 8;
}

message SearchIndexStatus {
  SearchIndexState state = 1;
  int64 live_record_count = 2;
  string last_rebuild_at = 3;
  string last_error = 4;
}

enum SearchIndexState {
  SEARCH_INDEX_STATE_UNSPECIFIED = 0;
  SEARCH_INDEX_STATE_READY = 1;
  SEARCH_INDEX_STATE_BUILDING = 2;
  SEARCH_INDEX_STATE_DEGRADED = 3;
  SEARCH_INDEX_STATE_MISSING = 4;
  SEARCH_INDEX_STATE_ERROR = 5;
}

message SemanticRuleStatus {
  int32 queue_depth_pending = 1;
  int32 queue_depth_running = 2;
  int32 queue_depth_failed_retryable = 3;
  int32 queue_depth_failed_permanent = 4;
  string last_refresh_at = 5;
  string last_backfill_at = 6;
  string last_error = 7;
}
```

## Proposed admin semantic API

File:

```text
../mycel-api/api/proto/mycel/admin/v1/semantic.proto
```

Service:

```proto
service AdminSemanticService {
  rpc ListSemanticRules(ListSemanticRulesRequest) returns (ListSemanticRulesResponse);
  rpc GetSemanticRule(GetSemanticRuleRequest) returns (GetSemanticRuleResponse);
  rpc ValidateSemanticRule(ValidateSemanticRuleRequest) returns (ValidateSemanticRuleResponse);
  rpc CreateSemanticRule(CreateSemanticRuleRequest) returns (CreateSemanticRuleResponse);
  rpc UpdateSemanticRule(UpdateSemanticRuleRequest) returns (UpdateSemanticRuleResponse);
  rpc SetSemanticRuleEnabled(SetSemanticRuleEnabledRequest) returns (SetSemanticRuleEnabledResponse);
  rpc DeleteSemanticRule(DeleteSemanticRuleRequest) returns (DeleteSemanticRuleResponse);
}
```

### Rule definition messages

```proto
message SemanticGenerationRule {
  string semantic_rule_id = 1;
  string space_id = 2;
  string domain_id = 3;
  string key = 4;
  string display_name = 5;
  string description = 6;
  bool enabled = 7;
  SemanticTriggerPolicy trigger = 8;
  SemanticTargetSelector selector = 9;
  SemanticSourceAssemblyPolicy source = 10;
  repeated SemanticEmbeddingBinding embeddings = 11;
  SemanticMaintenancePolicy maintenance = 12;
  SemanticStoragePolicy storage = 13;
  string owner_principal_id = 14;
  string created_by_principal_id = 15;
  string created_at = 16;
  string updated_at = 17;
}

message SemanticTriggerPolicy {
  repeated string events = 1;
  repeated string labels = 2;
  string debounce = 3;
}

message SemanticTargetSelector {
  string mode = 1; // node_type | gql | explicit_nodes
  repeated string labels = 2;
  string gql = 3;
  string target_alias = 4;
  int32 max_results = 5;
  repeated string node_ids = 6;
}

message SemanticSourceAssemblyPolicy {
  string mode = 1; // self | subtree | context_query
  repeated string include_properties = 2;
  repeated string exclude_properties = 3;
  optional int32 max_depth = 4;
  int32 minimum_text_length = 5;
  string context_gql = 6;
}

message SemanticEmbeddingBinding {
  string key = 1;
  string purpose = 2;
  string intelligence_profile = 3;
  string intelligence_profile_id = 4;
  string vector_store = 5;
  string vector_store_id = 6;
  bool enabled = 7;
  map<string, string> metadata = 8;
}

message SemanticMaintenancePolicy {
  string dirty_cooldown = 1;
  int32 max_batch_size = 2;
  int32 worker_concurrency = 3;
}

message SemanticStoragePolicy {
  bool searchable = 1;
  string physical_index = 2; // exact | ann_future
}
```

### Admin request/response sketch

```proto
message ValidateSemanticRuleRequest {
  SemanticGenerationRule rule = 1;
}

message ValidateSemanticRuleResponse {
  bool valid = 1;
  repeated SemanticRuleValidationDiagnostic diagnostics = 2;
  SemanticGenerationRule normalized_rule = 3;
}

message SemanticRuleValidationDiagnostic {
  string severity = 1; // error | warning | info
  string path = 2;
  string message = 3;
}
```

Create/update should use structured rule messages rather than raw JSON as the
primary API. CLI/console may still accept JSON/YAML files and map them into the
structured proto.

Delete:

```proto
message DeleteSemanticRuleRequest {
  string space_id = 1;
  string semantic_rule_id = 2;
  bool purge_vectors = 3;
}

message DeleteSemanticRuleResponse {
  string semantic_rule_id = 1;
  bool vectors_purged = 2;
  int32 work_items_deleted = 3;
  int32 policy_decisions_deleted = 4;
}
```

## Proposed admin maintenance API changes

File:

```text
../mycel-api/api/proto/mycel/admin/v1/semantic_maintenance.proto
```

Rename index-oriented operations:

- `BackfillSemanticIndex` -> `BackfillSemanticRule`
- `semantic_index_id` -> `semantic_rule_id`
- add `embedding_binding_key` filters where useful

Key messages:

```proto
message SemanticMaintenanceWorkItem {
  string work_item_id = 1;
  string space_id = 2;
  string domain_id = 3;
  string semantic_rule_id = 4;
  string embedding_binding_key = 5;
  string target_node_id = 6;
  string action = 7;
  string status = 8;
  int32 attempt_count = 9;
  string not_before = 10;
  string claimed_until = 11;
  string last_error_category = 12;
  string last_error_message_sanitized = 13;
  string created_at = 14;
  string updated_at = 15;
}

message AnalyzeSemanticDirtyWorkRequest {
  string space_id = 1;
  string semantic_rule_id = 2;
  string embedding_binding_key = 3;
  int32 limit = 4;
}

message BackfillSemanticRuleRequest {
  string space_id = 1;
  string semantic_rule_id = 2;
  string embedding_binding_key = 3;
  repeated string node_ids = 4;
  bool force = 5;
  int32 limit = 6;
  bool continue_on_error = 7;
}
```

## Intelligence Access API surface

Files:

```text
../mycel-api/api/proto/mycel/common/v1/intelligence_access.proto
../mycel-api/api/proto/mycel/admin/v1/intelligence_access.proto
../mycel-api/api/proto/mycel/admin/v1/inference.proto
```

Intelligence Access is now a distinct API surface for shared access control,
profile resolution, credential grants, policies, policy decisions, and usage
telemetry across semantic generation rules, semantic search, graph automations,
and future intelligence workloads. The remaining admin inference catalog API owns
provider/model/vector-store catalog resources only.

The shared scope is:

```proto
message IntelligenceAccessScope {
  string space_id = 1;
  string domain_id = 2;
  string semantic_rule_id = 3;
  string node_id = 4;
  bool include_descendants = 5;
  string embedding_binding_key = 6;
}
```

The source API replaces admin `AdminInferenceProfileService`,
`AdminInferenceCredentialService`, `AdminInferenceGrantService`,
`AdminInferencePolicyService`, and `AdminInferenceUsageService` with
`AdminIntelligenceAccess*` service names. It also renames profile, policy,
decision, usage, grant state, and scope fields from inference-specific names to
Intelligence Access names, while retaining provider-neutral inference operation
and privacy/model parameter messages under `common/v1/inference.proto`.

## Query API implications

Current query API has semantic search expression references in:

```text
../mycel-api/api/proto/mycel/client/v1/query.proto
```

SGR0 should decide one of two paths:

1. Replace semantic index fields with semantic rule/binding fields now.
2. If query semantic expression is not wired to semantic rules yet, remove or
   mark it as future in the source proto for this tranche.

Recommendation: replace fields now so generated types do not preserve old
semantic-index terminology.

## Generated artifacts policy

After source proto edits:

- `mycel-api`: commit source protobuf changes.
- `mycel`: run `make generate-proto` and commit daemon-local `internal/gen/`
  changes only if SGR0 includes compile adaptation.
- `mycel-rust-sdk`: update only if explicitly included in the tranche after API
  source changes compile. Generated/public SDK/API code can be changed now per
  user approval, but keep it as a separate commit/tranche if it increases risk.
- `mycel-console`: update TypeScript API types later in SGR10 unless generated
  API changes require immediate compile fixes.

## SGR0 implementation steps

1. Edit `mycel-api` source protobufs with replacement semantic rule terminology.
2. Run `make`/`buf` validation in `mycel-api`.
3. Generate daemon protobuf stubs in `mycel`.
4. Compile `mycel` to discover breakage.
5. Record required follow-up compile fixes for SGR1+ if generation is committed
   before implementation.
6. Prefer making SGR0 a source-proto-only commit if generated daemon stubs cannot
   compile without SGR1 model work.

## Validation commands

In `mycel-api`:

```sh
cd ../mycel-api
make test
```

If daemon stubs are regenerated in `mycel`:

```sh
make generate-proto
go test ./internal/gen/... -count=1
```

Docs validation:

```sh
make docs-check
git diff --check
git -C ../mycel-api diff --check
```

## Acceptance criteria

SGR0 is complete when:

- protobuf source replacement surface is agreed and documented;
- old semantic-index public terminology is removed from the planned API surface;
- rule/binding/status/search-index messages are specified;
- Intelligence Access usage/scope fields are specified for rule and binding attribution;
- generated-artifact policy for `mycel`, `mycel-console`, and `mycel-rust-sdk` is
  explicitly recorded;
- no runtime implementation is attempted beyond what is necessary to keep the
  tranche functional.

## Risks and follow-ups

- Full generated-stub replacement may break many daemon files before SGR1-SGR8
  are implemented. If so, keep SGR0 as source API planning/proto-source only and
  regenerate in SGR1/SGR8.
- Console and Rust SDK changes should wait until the rule API is stable enough to
  avoid churn.
- Query semantic expression changes may cascade into Rust SDK query builders;
  track as SGR8/SDK follow-up if not handled immediately.
