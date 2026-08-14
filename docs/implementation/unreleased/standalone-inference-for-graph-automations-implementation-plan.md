# Standalone Inference for Graph Automations Implementation Plan

## Status

In progress. INF1 is implemented as a forward API-contract tranche: canonical
common inference enums/messages were added in `mycel-api`; Admin Inference was
split into themed catalog, profile, credential, grant, policy, and usage admin
services; those services include profile, credential rotation, policy decision,
and usage telemetry contracts; and automation APIs now expose
inference-reference metadata. INF2 is implemented as a skeleton-only tranche:
`internal/inference` now has canonical model structs, storage interfaces,
file-backed global/space/usage stores, a runtime module, daemon initialization,
and focused round-trip tests. INF3 is implemented as a management tranche:
admin catalog and credential writes now mirror into the standalone inference
stores, credential rotation preserves stable credential IDs, external secret refs
fail closed to supported `env://NAME` references, and space-scoped profile
CRUD is exposed through the themed admin API and CLI. INF4 is implemented as a
resolver tranche: the standalone inference subsystem can resolve profile,
capability, credential grant, credential/secret, and policy matches; denies
missing grants/policies fail closed; deny/no-inference policies win; policy
restrictions are enforced; policy decisions are persisted and readable through
the themed admin API/CLI. INF5 is implemented as a connector-runtime tranche:
standalone inference now has operation-generic fake and OpenAI-compatible
connectors for embeddings and chat-like generation, secret material resolution
after policy allow, connector invocation APIs, and neutral usage telemetry for
success, failure, denial, and cancellation. INF6 is implemented as a semantic
embedding conversion tranche: semantic backfill and semantic query embedding can
route through the standalone inference resolver/invocation/usage path when a
semantic index declares an inference profile, while legacy fallback keeps
existing semantic indexes functional until they are reprofiled. INF7 is
implemented as the graph automation conversion tranche: automation definitions
use inference profile refs, validation rejects embedded credentials/secrets/raw
endpoint URLs and legacy provider/model config, single-step LLM execution calls
the standalone inference resolver/invoker, run records keep neutral inference
provenance, and automation usage events include automation/run context. INF8 is
implemented as an authorization/on-behalf-of tranche: inference and automation
admin APIs enforce explicit scoped capabilities, the reserved non-login
automation service principal is bootstrapped with worker capabilities, runtime
inference validates actor/on-behalf principal activity, principal-owned
credential use on behalf of another principal requires an explicit grant, and
automation definitions/invocations/runs preserve actor, on-behalf-of, and owner
provenance. INF9 is implemented as a CLI and operations documentation tranche:
`mycel inference` now exposes systematic singular resource nouns with useful
plural/legacy aliases, top-level grant/decision/usage commands, credential
rotation and lifecycle verbs, on-behalf grant flags, and automation commands can
use `--space-id` plus domain key/ID refs while preserving domain UUID
compatibility.

No compatibility migration from the current semantic inference stores,
automation provider configuration, automation run records, CLI shapes, or
protobuf API surfaces is required.

Design reference:
[Standalone inference model for graph automations](../../design/semantic/inference-for-graph-automations.md).

## Problem Statement

mycel currently has two partially overlapping inference models:

1. The semantic/inference path, which models endpoints, models, capabilities,
   credentials, grants, inference policies, policy decisions, and usage events,
   but is mostly embedding-oriented and physically owned by the semantic
   subsystem.
2. The graph automation path, which uses a simpler provider/model configuration
   and daemon environment variables for LLM-backed generation.

This split creates duplicated runtime paths and inconsistent security semantics:

automations can call an LLM without using the same credential ownership, grant,
privacy policy, policy decision, secret redaction, and usage telemetry model that
semantic inference already started to define.

The target architecture is one standalone inference subsystem used by semantic
embeddings/search and graph automations. Automations reference stable
space-scoped inference profiles, not raw provider credentials or API keys.

## Goals

- Create a standalone inference subsystem that is not product- or
  application-specific.
- Keep business pricing, credits, subscriptions, margins, and billing out of
  mycel.
- Support system-level connector, endpoint, model, and capability catalog
  resources.
- Support space-level inference profiles, grants, policies, policy decisions,
  and usage telemetry.
- Support principal-owned, space-owned, and system-owned credentials.
- Support credential rotation without editing automations or semantic indexes.
- Route graph automation LLM work through the same inference resolver, policy
  engine, credential grant checks, secret resolution, connectors, and usage
  ledger used by semantic inference.
- Route semantic embedding generation through the same inference resolver where
  practical.
- Make model resolution respect space, domain, principal, usage mode, privacy,
  and policy context.
- Make graph automation definitions reference inference profiles/model refs and
  reject embedded secrets.
- Add systematic daemon APIs and CLI commands for endpoints, models,
  capabilities, profiles, credentials, grants, policies, decisions, usage, and
  automations.
- Keep each phase buildable and testable.

## Non-goals

- No backward compatibility for existing inference or automation data stores.
- No migration from old automation provider environment variables.
- No application billing, credits, customer accounts, product pricing, or cost
  policy in mycel.
- No Knot PKM-specific concepts.
- No generic end-user chat product surface in this tranche.
- No automatic repair of inconsistent inference catalog, credential, grant,
  policy, or automation state.
- No automatic restore/merge/rebalance/authoritative-node selection.
- No generated protobuf files hand-edited in mycel.

## Implementation Principles

- **Standalone inference subsystem.** Shared inference resources should live in a
  first-class inference subsystem rather than under semantic or automation.
- **Profiles over credentials.** Workloads reference stable profiles; profiles
  and grants resolve to capabilities and credentials at runtime.
- **Credentials are scoped and grant-gated.** Possessing a credential record is
  not enough to use it for a workload.
- **Fail closed.** Missing profiles, disabled resources, missing grants, missing
  policies, invalid secrets, and unsupported connectors deny execution.
- **Deny wins.** Explicit no-inference/deny policy overrides allow/restrict
  policy.
- **Neutral telemetry only.** Token counts, latency, request IDs, statuses, and
  resource refs are acceptable; pricing/credits are not.
- **Secret values never leak.** Secret material must not appear in API responses,
  CLI output, logs, automation definitions, run records, policy decisions, or
  usage events.
- **Principal-based authorization.** Management and execution use unified
  principals, roles, capabilities, scoped authorization, and on-behalf-of audit
  context.
- **Daemon-owned APIs.** CLI and applications talk to `myceld`; they do not open
  stores or bypass service managers.
- **Design with replacement in mind.** Remove inconsistent old structures instead
  of preserving them for compatibility.

## Target Resource Ownership

| Resource | Target owner |
|---|---|
| Connector adapter registrations | inference subsystem runtime / binary code |
| Endpoints, models, endpoint capabilities | inference global manager |
| Vector stores | inference global manager, consumed by semantic subsystem |
| Secrets and credentials | inference credential manager |
| Profiles | inference space manager |
| Credential grants | inference space manager |
| Inference policies | inference space manager |
| Policy decisions | inference space manager / audit store |
| Usage telemetry | inference accounting ledger |
| Semantic indexes and vector records | semantic subsystem |
| Automation definitions, invocations, runs, workflows | automation subsystem |

## Target Package Layout

A likely package layout inside `mycel`:

```text
internal/inference/model
internal/inference/service
internal/inference/storage
internal/inference/connectors
internal/inference/accounting
internal/inference/runtime
internal/daemon/api/admin/inference_service.go
internal/cli/cmd/inference.go
```

The semantic and automation subsystems become consumers:

```text
internal/semantic/service -> internal/inference/service runtime APIs
internal/automation/service -> internal/inference/service runtime APIs
```

## Phase INF1: API Contract and Naming Cleanup

### Objective

Define the future public API surface in `mycel-api` before reshaping internal
code. Because no compatibility is required, remove or replace inconsistent API
shapes rather than layering compatibility fields.

### Tasks

1. Update or replace Admin Inference protobufs in `mycel-api`:
   - endpoints;
   - models;
   - endpoint capabilities;
   - vector stores;
   - credentials;
   - credential rotation/status;
   - profiles;
   - grants;
   - policies;
   - policy decisions;
   - usage telemetry list/summary.
2. Add or update common enum/message shapes for:
   - `InferenceOperation`;
   - `UsageMode`;
   - `PrivacyClass`;
   - `NetworkClass`;
   - `CredentialOwnerType` with `principal`, `space`, and `system`;
   - `CredentialAuthType`;
   - `PolicyAction`;
   - `PolicyDecisionAction`;
   - `InferenceScope`.
3. Replace graph automation model/provider fields with inference refs:
   - top-level single-step `inference` ref;
   - workflow step `inference` ref;
   - parameters such as temperature, max output tokens, JSON/text response mode.
4. Remove public cost/price/credit fields from automation and inference API
   shapes.
5. Add neutral token and request telemetry fields where useful.
6. Add new capability enum values and canonical capability names for:
   - inference catalog read/manage;
   - credential read/manage;
   - profile read/manage;
   - grant manage;
   - policy manage;
   - usage/decision read.
7. Regenerate generated code in `mycel`, SDKs, and admin where needed.

### Validation

- `mycel-api` lint/test passes.
- Generated code is produced from protobuf definitions, not hand-edited.
- `mycel` compiles after minimal adapter stubs are added.
- No public API fields model product pricing or credits.

### Functional endpoint for the phase

The code may compile with unimplemented RPCs returning `Unimplemented`, but API
names and message shapes should reflect the target model.

## Phase INF2: Extract the Standalone Inference Subsystem Skeleton

### Objective

Create `internal/inference` as the shared subsystem boundary while preserving
current semantic and automation behavior temporarily behind adapters or stubs.

### Tasks

1. Create `internal/inference/model` with canonical domain structs for:
   - endpoint;
   - model;
   - endpoint capability;
   - vector store;
   - credential;
   - profile;
   - grant;
   - policy;
   - decision;
   - usage event.
2. Create `internal/inference/storage` interfaces:
   - global catalog manager;
   - credential manager;
   - space manager for profiles/grants/policies/decisions;
   - usage ledger interface.
3. Create file-backed store implementations using existing mycel file-store
   conventions.
4. Create `internal/inference/service.Module` implementing runtime init and
   service lookup patterns.
5. Register the inference subsystem in daemon app initialization.
6. Keep existing semantic storage working until migration phases replace it.
7. Add quiesce/local-write-gate hooks where mutations occur.

### Validation

- `go test ./internal/inference/...` passes.
- Full `make test` passes with old semantic/automation behavior still available
  or with temporary feature stubs.
- File store round-trip tests cover each new resource.
- Store output does not include secret material in non-secret records.

### Functional endpoint for the phase

The daemon starts with an initialized inference subsystem. Existing semantic and
automation paths remain buildable even if they do not yet use it.

## Phase INF3: Catalog, Credentials, Secrets, and Profiles

### Objective

Implement management of system-level catalog resources, credentials, secret refs,
and space-level profiles.

### Tasks

1. Implement endpoint/model/capability/vector-store CRUD and enable/disable
   lifecycle in the inference service.
2. Implement package application for neutral catalog resources.
3. Implement credentials with owner types:
   - `principal`;
   - `space`;
   - `system`.
4. Implement secret ref validation and redaction:
   - encrypted inline secret references;
   - `env://NAME` references;
   - explicit rejection of unsupported schemes.
5. Implement credential rotation:
   - update secret ref/version on stable credential ID;
   - optional grant retargeting to replacement credential.
6. Implement profile CRUD:
   - space-scoped key/ID lookup;
   - operation;
   - candidate endpoint/model/capability constraints;
   - privacy requirements;
   - default parameters;
   - enabled/disabled state.
7. Add admin API handlers and CLI commands for catalog, credentials, rotation,
   and profiles.
8. Add reference-safe delete checks for resources that can be referenced by
   profiles, grants, semantic indexes, or automation definitions.

### Validation

- Admin API tests for create/list/get/enable/disable/delete flows.
- CLI tests for command help and JSON output.
- Secret redaction tests for API, CLI, logs, and persisted public records.
- Credential rotation tests prove credential ID stability.
- Unsupported secret scheme tests fail closed.

### Implementation status

Implemented. The existing semantic-backed admin catalog and credential paths now
synchronize endpoints, models, capabilities, vector stores, secrets, credentials,
and packages into the standalone inference subsystem while current semantic
runtime paths remain unchanged. Profile CRUD is implemented directly against the
standalone inference space manager and exposed through `inference profile` CLI
commands. Credential rotation creates a new redacted secret record and updates
the stable credential ID. External secret references are restricted to
`env://NAME`; unsupported schemes are rejected.

### Functional endpoint for the phase

An administrator can install catalog resources, create/rotate credentials, and
create space-level profiles. No production inference execution is required yet.

## Phase INF4: Grants, Policies, Decisions, and Resolver

### Objective

Implement the central inference resolver and policy engine:

```text
profile -> capability -> credential grant -> credential/secret -> policy decision
```

### Tasks

1. Implement credential grant CRUD with matching dimensions:
   - space;
   - domain;
   - node/subtree;
   - operation;
   - profile;
   - endpoint/model/capability;
   - usage mode;
   - actor principal;
   - on-behalf-of principal.
2. Implement inference policy CRUD:
   - allow;
   - restrict;
   - deny;
   - no-inference;
   - allowed privacy classes;
   - require local endpoint;
   - disallow third-party;
   - request/token ceilings.
3. Implement scope specificity consistently:
   - node/subtree;
   - domain;
   - space.
4. Implement resolver candidate selection:
   - profile lookup;
   - enabled endpoint/model/capability checks;
   - feature checks such as JSON output or embeddings;
   - privacy/network constraints;
   - credential/grant matching;
   - policy evaluation.
5. Persist `PolicyDecision` for allowed and denied attempts.
6. Expose decision lookup and policy list commands.
7. Ensure deny/no-inference policies override allow/restrict policies.
8. Ensure missing policy/grant denies execution.

### Validation

- Resolver unit tests for all fail-closed cases.
- Policy unit tests for deny precedence and restrict narrowing.
- Grant matching tests for principal-owned, space-owned, and system-owned
  credentials.
- Node/subtree scope specificity tests.
- Decision persistence tests for allowed and denied attempts.
- Capability authorization tests for policy/grant management APIs.

### Implementation status

Implemented. `internal/inference/service` exposes `Resolve`, which evaluates a
hypothetical workload against enabled profiles, catalog capabilities, credential
grants, credential/secret state, owner constraints, usage mode, scope, privacy,
feature, token, and policy constraints. Missing profiles, capabilities, grants,
or matching policies deny and persist a denied policy decision. Matching allow or
restrict policies permit the request only after restrictions pass; deny and
no-inference policies take precedence. Admin grant/policy writes now synchronize
into the standalone inference space store, and `GetPolicyDecision` plus
`inference policy decision get` expose persisted decisions.

### Functional endpoint for the phase

The resolver can decide whether a hypothetical workload may invoke inference and
returns a resolved endpoint/model/capability/credential plus decision ID, but
connectors may still be limited to embeddings or fake calls.

## Phase INF5: Connector Runtime for Embeddings and Chat

### Objective

Implement operation-generic provider invocation through the inference subsystem.

### Tasks

1. Define connector interfaces for:
   - embeddings;
   - chat/text generation;
   - structured JSON generation where supported.
2. Port existing OpenAI-compatible embedding connector into the inference
   subsystem.
3. Add OpenAI-compatible chat connector support.
4. Add deterministic fake connector for tests.
5. Normalize connector request/response shapes:
   - provider request ID;
   - input/output/total token counts;
   - latency;
   - retryable/non-retryable error classification;
   - output text/JSON/embedding vector.
6. Resolve secret material only after policy allows a request.
7. Keep secret material in memory only for connector invocation.
8. Append `InferenceUsageEvent` for success, failure, denied, and canceled
   requests.
9. Ensure usage events have no price/cost/credit fields.

### Validation

- Fake connector unit and integration tests.
- OpenAI-compatible request-shape tests without real network calls.
- Denied request tests prove no connector call occurs.
- Usage event tests for success, failure, and denial.
- Secret material redaction tests.

### Implementation status

Implemented. `internal/inference/connectors` defines embedding and chat
connector interfaces, normalized request/response shapes, deterministic fake
connector behavior, and OpenAI-compatible embedding and chat-completions request
handling. `internal/inference/service.Invoke` resolves policy first, records
denied usage without resolving secrets or calling connectors, resolves secret
material only for allowed requests, invokes the registered connector, and appends
neutral usage events for success, failure, denial, and cancellation. The admin
usage service now lists and summarizes standalone usage ledger events. Tests
cover fake invocation, OpenAI-compatible request shapes, denied no-call behavior,
usage telemetry, connector failure classification, and secret redaction.

### Functional endpoint for the phase

A test workload can invoke fake chat and fake embeddings through the full
profile/grant/policy/secret/connector/usage path.

## Phase INF6: Convert Semantic Embeddings to the Inference Subsystem

### Objective

Make semantic indexing/search consume the standalone inference runtime for
embedding operations.

### Tasks

1. Update semantic index model to reference an embedding profile or capability
   instead of directly binding endpoint/model/capability IDs where possible.
2. Update semantic index creation API/CLI to accept `--profile` and optionally
   explicit lower-level refs for administrator-controlled cases.
3. Replace semantic-specific grant and policy evaluation with inference resolver
   calls.
4. Route semantic backfill embedding generation through the inference runtime.
5. Route semantic search query embedding through the inference runtime.
6. Record semantic usage events in the shared inference usage ledger.
7. Preserve semantic-specific ownership of:
   - semantic index definitions;
   - vector records;
   - vector stores where appropriate;
   - maintenance/backfill state.
8. Remove obsolete duplicate semantic connector/grant/policy code after the new
   path is complete.

### Validation

- Existing semantic service tests pass after updates.
- New semantic integration test creates profile/grant/policy, indexes content,
  and searches through shared inference usage.
- Policy denial prevents semantic embedding and records a decision/event.
- Disabled endpoint/model/capability/credential fails closed.
- `make test` passes.

### Implementation status

Implemented. Semantic embedding connector inputs now carry inference profile and
capability refs plus policy-decision provenance. `connectors.InferenceAdapter`
routes semantic embedding requests through `inference.Service.Invoke`, including
resolver, grant/policy decision, secret resolution, connector invocation, and
shared usage ledger recording; it falls back to the legacy semantic connector
only when a semantic index has no inference profile. Semantic backfill stores the
returned policy decision on vector records, and semantic search query embedding
can use the same standalone inference path. The semantic module discovers the
inference subsystem from runtime service lookup and wires the adapter into
backfill and search. Integration coverage creates a profile/grant/policy, indexes
content, searches through the shared inference path, and verifies usage events.

### Functional endpoint for the phase

Semantic embedding and search work through the standalone inference subsystem.

## Phase INF7: Convert Graph Automations to Inference Profiles

### Objective

Remove the automation-specific provider configuration path and route LLM-backed
automation steps through standalone inference.

### Tasks

1. Replace automation `Model{provider, model}` with `InferenceRef` in internal
   automation model types.
2. Update automation JSON validation:
   - require inference refs for LLM steps;
   - reject embedded API keys, bearer tokens, secret refs, and raw endpoint URLs;
   - validate operation/parameter shape;
   - defer availability/policy checks to runtime resolver.
3. Remove daemon automation provider environment variables:
   - `MYCELD_AUTOMATION_PROVIDER`;
   - provider base URL/API key configuration;
   - any provider-specific automation-only config.
4. Remove hard-coded automation cost estimation.
5. Replace `MaxCostPerRun` with neutral operational safety controls, such as:
   - max provider calls per run;
   - max input tokens per run;
   - max output tokens per run;
   - max elapsed time.
6. Update execution path to call inference runtime for LLM generation.
7. Record automation run references on usage events:
   - automation ID;
   - invocation ID;
   - run ID;
   - workflow instance/step run ID when present.
8. Store denormalized profile/model/token/provider-request summaries on run
   records only for debugging, not authoritative accounting.
9. Update examples to use inference profiles.

### Validation

- Automation validation rejects credentials/secrets in definitions.
- End-to-end fake-connector automation test:
   - create graph node;
   - trigger automation;
   - resolve profile/grant/policy;
   - generate text;
   - apply graph action;
   - record usage event.
- Policy denial test records decision and run failure without connector call.
- Credential rotation test proves automation definition does not change.
- Old automation provider environment variables are no longer needed by tests.

### Implementation status

Implemented. Internal automation definitions now use `inference` refs rather
than provider/model config, workflow LLM steps require their own inference refs,
and validation rejects legacy `model` config, embedded credentials, bearer
strings, secret refs, and raw endpoint URLs. The daemon no longer reads
`MYCELD_AUTOMATION_PROVIDER` or provider-specific automation environment
variables. Automation execution calls `inference.Service.Invoke` with usage mode
`automation`, profile/model/capability refs, automation ID, invocation ID, run
ID, and actor/on-behalf-of context. Run records denormalize inference profile,
model/capability/credential/grant, policy decision, provider request, and token
summaries without cost fields. Focused tests cover validation failures,
standalone fake connector execution, denial without connector calls, credential
rotation stability, token ceilings, and an end-to-end graph-change automation
that mutates a node and records an inference usage event.

### Functional endpoint for the phase

A graph automation can perform LLM-backed work through a profile in a clean,
standalone mycel deployment.

## Phase INF8: Authorization and On-behalf-of Execution Semantics

### Objective

Make management and runtime execution consistently principal-based and scoped.

### Tasks

1. Add or update built-in roles/capabilities for inference management and audit.
2. Update Admin Inference API adapters to use explicit authorization checks per
   method.
3. Update Admin Automation API adapters to use explicit authorization checks.
4. Define the automation worker actor principal:
   - reserved system/service principal;
   - non-login-enabled by default;
   - scoped capabilities required for execution.
5. Define on-behalf-of principal selection:
   - triggering graph write principal for graph-change automations when
     available;
   - automation owner or configured service principal for scheduled/scanned
     automations.
6. Ensure graph read/write checks remain separate from inference checks.
7. Record actor, on-behalf-of, and automation owner refs in invocations, runs,
   decisions, and usage events.
8. Ensure disabled principals and revoked grants fail closed.

### Validation

- System admin can manage catalog/system credentials.
- Space-scoped inference admin can manage profiles/policies/grants only in
  permitted spaces.
- Principal with graph write but no inference grants cannot cause inference.
- Principal-owned credential cannot be used on behalf of another principal unless
  explicitly granted.
- Disabled on-behalf-of principal behavior is defined and tested.

### Functional endpoint for the phase

Inference and automation execution have auditable principal context and enforce
scoped capabilities.

## Phase INF9: CLI and Operations Documentation

### Objective

Make the CLI systematic and discoverable across inference and automation
resources.

### Tasks

1. Rewrite `mycel inference` command tree around singular nouns:
   - `package`;
   - `endpoint`;
   - `model`;
   - `capability`;
   - `vector-store`;
   - `profile`;
   - `credential`;
   - `grant`;
   - `policy`;
   - `decision`;
   - `usage`.
2. Add plural aliases only where useful.
3. Standardize verbs:
   - `list`;
   - `get`;
   - `create`/`add`;
   - `enable`;
   - `disable`;
   - `rotate`;
   - `expire`;
   - `revoke`;
   - `delete`.
4. Standardize scope flags:
   - `--space-id`;
   - `--domain`;
   - `--node`;
   - `--include-descendants`.
5. Standardize output:
   - text/table output;
   - `--output json`;
   - pagination where applicable.
6. Rewrite automation CLI examples to use `--space-id` and domain key/ID refs.
7. Update docs:
   - `docs/operations/cli/inference.md`;
   - `docs/operations/cli/automation.md`;
   - semantic and automation design docs;
   - examples under `examples/inference` and `examples/automations`.

### Validation

- CLI tests for each noun/verb path.
- Help text tests for discoverability.
- Docs check passes.
- Examples validate against current automation schema.
- Secret material never appears in CLI output.

### Functional endpoint for the phase

Operators can provision inference resources, profiles, credentials, grants,
policies, and automations using consistent CLI commands.

## Phase INF10: Storage Authority, WAL/Raft, Reload, and Reference Safety

### Objective

Make the redesigned inference resources durable and consistent with mycel's
subsystem architecture.

### Tasks

1. Decide authoritative storage for each inference resource:
   - global catalog;
   - credentials/secrets;
   - space profiles;
   - grants;
   - policies;
   - decisions;
   - usage ledger.
2. Add WAL/raft records for authoritative resource mutations where required by
   current daemon architecture.
3. Add snapshot/reload support for inference subsystem state.
4. Register WAL appliers and quiesce gates.
5. Add reference-safe lifecycle checks:
   - endpoint referenced by capabilities/credentials/usage;
   - model referenced by capabilities/profiles/usage;
   - capability referenced by profiles/semantic indexes/automations/decisions;
   - credential referenced by grants/decisions/usage;
   - profile referenced by semantic indexes/automations/runs.
6. Define usage ledger retention/export strategy without billing semantics.
7. Ensure clustered deployment behavior is explicit and test-covered.

### Validation

- WAL apply tests.
- Snapshot/reload tests.
- Reference-safe delete tests.
- Quiesce behavior tests.
- Clustered route/local-write-gate tests where applicable.
- Full `make test` passes.

### Functional endpoint for the phase

Inference configuration is durable, reloadable, and consistent with mycel's
subsystem lifecycle and clustered-authority model.

## Phase INF11: Cleanup and Removal of Obsolete Paths

### Objective

Remove old duplicated or inconsistent structures after semantic and automation
paths use the standalone inference subsystem.

### Tasks

1. Delete automation-specific provider runtime and environment config.
2. Delete automation pricing/cost estimator code and cost policy fields.
3. Delete semantic-specific duplicate credential grant/policy resolver code.
4. Delete stale docs that describe application chat catalogs or provider API keys
   as automation configuration.
5. Delete obsolete API compatibility messages/commands introduced only for the
   old model.
6. Remove stale tests and replace them with profile/grant/policy tests.
7. Run public surface checks to ensure no generated/internal boundaries changed
   unexpectedly.

### Validation

- `make test` passes.
- `make build` passes.
- `scripts/check-public-surface.sh` passes.
- `make docs-check` passes.
- No references remain to removed automation provider config except historical
  implementation notes if intentionally preserved.

### Functional endpoint for the phase

The codebase has one inference path and no product-pricing or automation-specific
credential path.

## API and CLI Compatibility Policy

This plan assumes a clean breaking change:

- Existing automation definitions may be rejected until rewritten with inference
  profiles.
- Existing semantic indexes may need to be recreated with embedding profiles or
  revised capability refs.
- Existing CLI examples may be deleted or rewritten.
- Existing file stores may be replaced by new store layouts.
- Existing protobuf service/message names may be removed or replaced.

Do not add legacy compatibility unless a later product decision explicitly
changes this assumption.

## Cross-repository Work

### mycel-api

- Define canonical protobuf contracts.
- Remove old inconsistent automation provider/model API fields.
- Add inference profile/grant/policy/usage/decision surfaces.

### mycel

- Implement daemon services, storage, resolver, connectors, CLI, docs, and tests.

### mycel-go-sdk and mycel-rust-sdk

- Regenerate/update generated client bindings.
- Add minimal helpers for new inference and automation references where useful.
- Avoid introducing product billing abstractions.

### mycel-admin

- Later UI work should manage inference profiles, credentials, grants, policies,
  decisions, usage telemetry, and automation definitions without exposing secret
  values.
- UI work is not required to complete the daemon/core implementation, but API
  shapes should allow it.

## Testing Matrix

| Area | Required tests |
|---|---|
| API contracts | protobuf lint/generation; SDK compile checks |
| Storage | file round-trip; invalid records; redaction; reload |
| Resolver | profile/capability/grant/policy success and fail-closed cases |
| Credentials | principal/space/system ownership; rotation; revoked/disabled state |
| Secrets | env/encrypted refs; unsupported scheme denial; no output leakage |
| Policies | deny precedence; restrict narrowing; local-only; third-party denial |
| Usage telemetry | success/failure/denial/cancel events; no pricing fields |
| Semantic | embedding profile provisioning; backfill/search through inference runtime |
| Automation | profile-backed LLM run; denial; credential rotation; graph action output |
| Authorization | scoped capabilities for catalog/credential/profile/grant/policy/usage |
| CLI | help, JSON output, scope flags, secret redaction, examples |
| Durability | WAL/raft apply, snapshots, reference-safe deletes |

## Open Questions

1. Should inference profiles be strictly space-scoped, or should domain-scoped
   profiles be first-class resources?
2. Which external secret reference schemes should mycel support in the first
   implementation beyond encrypted-inline and `env://`?
3. How should principal-owned credential consent be exposed in CLI and admin UI?
4. Should the usage ledger be global with indexes, per-space, or both?
5. What retention/export controls are needed for usage telemetry?
6. Should semantic vector stores remain under inference catalog ownership or move
   more explicitly back under semantic ownership?
7. What exact graph authorization model should automation service principals use
   for scheduled/scanned automations?
8. Should workflow LLM/tool/proposal execution wait until single-step automation
   inference is complete?

## Suggested Implementation Order

Recommended near-term order:

1. INF1 API contracts.
2. INF2 subsystem skeleton.
3. INF3 catalog/credentials/profiles.
4. INF4 grants/policies/resolver.
5. INF5 fake connector + usage ledger.
6. INF7 automation conversion using fake connector first.
7. INF6 semantic conversion.
8. INF8 authorization hardening.
9. INF9 CLI/docs.
10. INF10 durability/raft hardening.
11. INF11 cleanup.

Automation can be converted before semantic once the resolver and fake/chat
connector exist. Semantic conversion can happen after or in parallel if the
embedding connector is ready. The important invariant is that no production LLM
workload should bypass profiles, grants, policies, secret resolution, and usage
telemetry once the standalone inference runtime exists.
