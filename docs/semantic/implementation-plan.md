# Semantic Implementation Plan

This plan turns the semantic design docs into incremental implementation phases across Mycel and Knot PKM.

Every phase must keep both systems working:

- Mycel must keep `go test ./...` and the CLI build working.
- Knot PKM server/importer/client must keep their current user-facing flows working.
- Existing MVP embedding/profile APIs remain compatible until the explicit cutover phase.
- Any design change discovered during implementation must update the relevant documentation in the same phase.
- Every phase ends with pushed commits:
  - Mycel to GitHub on `improved_embedding_support` until integration/release branches are chosen.
  - Knot PKM server/importer/client to GitLab `develop` or a feature branch merged into `develop`.

## Source References

Design docs:

- [concepts.md](concepts.md)
- [provisioning.md](provisioning.md)
- [credentials.md](credentials.md)
- [policies.md](policies.md)
- [embedding-generation.md](embedding-generation.md)
- [query-planning.md](query-planning.md)
- [accounting.md](accounting.md)
- [storage.md](storage.md)
- [../storage/semantic.md](../storage/semantic.md)
- [../storage/meta.md](../storage/meta.md)
- [../cli/README.md](../cli/README.md)

Recent Mycel GitHub change log entries that shaped this plan:

```text
7553bba docs: use kusag extension for inference usage ledger
d29033f docs: add inference accounting and split CLI commands
1b9f363 docs: resolve source policy change semantics
b9e6666 docs: resolve semantic deletion and revocation semantics
3045619 docs: resolve query embedding credential semantics
5340500 docs: resolve credential grant requirements
1ac8565 docs: resolve inference policy inheritance semantics
22b3fa8 docs: resolve inference policy default and restrict semantics
1daac2c docs: resolve semantic dirty event transaction model
3286f67 docs: resolve semantic source policy decisions
1beadc2 docs: resolve vector space model revision decisions
bbc0bb1 docs: resolve endpoint capability validation decisions
c131cd9 docs: rename runtimes to model endpoints
89433f6 docs: make semantic grants and policies space-owned
f8d296c docs: create semantic embedding documentation section
5315db1 Merge pull request #1 from MycelDB/support_domains
```

Recent Knot PKM GitLab change log entries to preserve:

```text
server: 1e8fe78 Merge branch 'support_domains' into 'develop'
server: 626864f feat: scope PKM sessions to graph domain
server: 946ad30 fix: refresh embeddings with default search profile
server: 0ce4244 feat: refresh embeddings for dirty PKM roots
importer: b6cdb9d Merge branch 'support_domains' into 'develop'
importer: 1a7ebf1 feat: import PKM content into graph domain
client: d2cc527 feat: add page picker for slash page command
client: a76cc13 fix: resolve fresh page links from local index
```

## Cross-Phase Rules

### Branch and dependency handling

- Mycel implementation work happens on `improved_embedding_support` until it is merged/released.
- Knot PKM work happens on GitLab `develop` or feature branches targeting `develop`; `main` remains production.
- Local Go `replace github.com/myceldb/mycel => .../myceldb/mycel` directives are allowed only for local development.
- Pushed Knot PKM commits must use a reproducible Mycel dependency: a tag, pseudo-version, or explicit branch commit that CI can fetch.
- If a phase needs Mycel and Knot PKM changes together, push Mycel first, then update Knot PKM dependency to that commit.

### Compatibility

- Do not remove the current `embeddings` CLI/API/profile behavior until the final cutover phase.
- New semantic APIs are additive at first.
- Knot PKM must be able to run with the current MVP embedding path until semantic search is explicitly enabled.
- If semantic provisioning is incomplete, Mycel must return clear warnings/errors rather than silently using defaults.
- No inference is allowed without an applicable inference policy and explicit credential grant.

### Test gates

Run the relevant gates before pushing a phase.

Mycel:

```sh
cd myceldb/mycel
make test
make build
```

Knot PKM server:

```sh
cd knot_pkm/knot_pkm_server
go test ./...
```

Knot PKM importer:

```sh
cd knot_pkm/knot_pkm_importer
make test
```

Knot PKM client:

```sh
cd knot_pkm/knot_pkm_client
npm test -- --runInBand
npm run build
npm run test:e2e
```

Playwright tests are required for any user-visible UI behavior change.

## Phase 0: Baseline, Feature Flags, and CI Safety

Goal: create a safe implementation lane without changing runtime behavior.

### Mycel deliverables

- Confirm current tests/build pass on `improved_embedding_support`.
- Add a small semantic implementation status section to docs if needed.
- Add feature flag/config naming for advanced semantic support where code paths need gating.
- Ensure docs clearly mark current MVP commands vs target semantic commands.

### Knot PKM deliverables

- Confirm server, importer, and client tests still pass on `develop`.
- Add server configuration flags for future semantic mode if needed, defaulting to existing MVP behavior.
- Keep current automatic debounced embedding refresh active.

### Tests

- Mycel: `make test`, `make build`.
- PKM server/importer/client: existing unit/build/e2e gates.

### Push gate

- Push Mycel docs/config-only commit to GitHub if changed.
- Push Knot PKM config-only commit to GitLab if changed.

## Phase 1: Additive Semantic Resource Model and Stores

Goal: introduce the new resource model without changing embedding behavior.

### Mycel deliverables

Add domain types and validation for:

- `ConnectorType`
- `ModelEndpoint`
- `InferenceModel`
- `ModelEndpointCapability`
- `VectorStoreType`
- `VectorStoreBackend` metadata record
- `InferenceCredential`
- `CredentialGrant`
- `InferencePolicy`
- `SemanticIndex`
- `EmbeddingRecord` provenance extensions
- `InferenceUsageEvent`

Add JSON stores for the documented paths:

```text
meta/inference/packages.json
meta/inference/model_endpoints.json
meta/inference/models.json
meta/inference/model_endpoint_capabilities.json
meta/inference/vector_stores.json
meta/secrets/secrets.json
meta/credentials/credentials.json
graphs/<space_id>/semantic/indexes.json
graphs/<space_id>/semantic/credential_grants.json
graphs/<space_id>/semantic/inference_policies.json
```

`mycel init` should create the built-in `mycel-file` vector store definition if absent.

### Knot PKM deliverables

- Update Mycel dependency after Mycel push.
- No runtime behavior change.
- Confirm existing domain-scoped sessions and Logseq import still work.

### Tests

Mycel unit tests:

- type validation
- JSON store create/read/update/delete
- idempotent `mycel-file` creation during init
- backwards compatibility with existing `meta/embedding/embeddings.json`

Knot PKM tests:

- server/importer existing Go tests
- no required Playwright unless UI changes occur

### Push gate

- Push Mycel phase commit to GitHub.
- Push Knot PKM dependency bump to GitLab if needed.

## Phase 2: Provisioning CLI, Packages, Secrets, Credentials, Grants, and Policies

Goal: make semantic resources provisionable, still without relying on them for existing search.

### Mycel deliverables

Implement target CLI commands documented under [`../cli/commands/`](../cli/commands/):

- `mycel inference package apply`
- `mycel inference capability add`
- `mycel inference credential add`
- `mycel inference credential grant`
- `mycel inference policy allow`
- `mycel inference policy deny`
- `mycel inference policy restrict`
- `mycel semantic index add`

Implement:

- package parsing/upsert semantics
- encrypted secret storage or secret references
- credential CRUD and revocation state
- grant resolution data model, but not yet required by legacy profile search
- policy persistence and validation
- semantic config event append for changed semantic resources:

```text
meta/semantic_events/semantic-config-000001.ksem
```

### Knot PKM deliverables

- Extend local/dev provisioning scripts to be able to provision the new semantic resources behind a feature flag.
- Keep existing AI key/profile setup working.
- Do not switch production search/chat behavior yet.

### Tests

Mycel unit/CLI tests:

- package apply idempotency
- capability required and trusted as provisioned
- credentials do not imply grants
- grants are space-owned and scoped
- policy default deny and validation
- semantic config event append on changes

Knot PKM tests:

- server/importer current tests
- utility script smoke tests where practical

### Push gate

- Push Mycel first.
- Push Knot PKM provisioning-script/config updates second.

## Phase 3: Accounting Ledger Foundation

Goal: record all new semantic model endpoint calls in an append-only ledger before those calls become common.

### Mycel deliverables

Implement:

```text
meta/accounting/manifest.json
meta/accounting/inference-usage-000001.kusag
meta/accounting/indexes/
meta/accounting/rollups/
```

Add APIs and stores for appending `InferenceUsageEvent` records.

Implement CLI commands:

- `mycel accounting usage summarize`
- `mycel accounting usage events`
- `mycel accounting usage export`
- `mycel accounting usage rebuild-indexes`
- `mycel accounting usage rebuild-rollups`

Rules:

- append events for success, failed, and cancelled calls
- record `token_count_source` as `provider_reported`, `estimated`, or `unavailable`
- do not store raw prompts or graph content by default
- indexes and rollups are derived/rebuildable

### Knot PKM deliverables

- No UI requirement yet.
- Server may expose internal diagnostics only if needed.
- Existing chat/search paths continue working; if they call model endpoints through Mycel adapters later, they must be accounted then.

### Tests

Mycel unit/CLI tests:

- append-only ledger segment writing/recovery
- summaries by period/user/space/domain/node/model/endpoint/grant/status
- rebuild indexes from ledger
- rebuild rollups from ledger
- failed call accounting

Knot PKM tests:

- current server/importer/client gates if dependency updated

### Push gate

- Push Mycel accounting commit to GitHub.
- Push Knot PKM dependency bump only if required.

## Phase 4: Connector Abstraction and `mycel-file` Vector Backend

Goal: create the execution/storage interfaces needed by semantic indexes.

### Mycel deliverables

Implement:

- connector interface for model endpoint calls
- initial `openai-compatible` embedding connector
- model endpoint capability validation before execution
- credential loading/decryption for connector calls
- token usage capture into accounting events
- `VectorStoreBackend` interface
- built-in `mycel-file` vector store backend
- advanced `.kvec` records with semantic index/model endpoint/model/grant/policy provenance
- local tombstone/delete records that make deleted embeddings non-searchable immediately

Current MVP profile search must keep working. Reuse current vector code where possible, but keep the new backend interface independent.

### Knot PKM deliverables

- Update dependency when stable.
- Keep existing embedding refresh/search behavior.
- Optional: add server-only configuration for semantic index feature flag, default off.

### Tests

Mycel unit tests:

- connector request construction and response parsing with mocked HTTP
- capability missing/disabled errors
- credential absent/revoked/disabled errors
- accounting event emitted for success and failure
- vector upsert/search/delete/latest-record semantics
- tombstoned records are not searchable

Knot PKM tests:

- existing server/importer tests after dependency bump

### Push gate

- Push Mycel connector/vector-backend commit to GitHub.
- Push Knot PKM dependency/config commit if needed.

## Phase 5: Semantic Index Backfill and Current-MVP Bridge

Goal: make semantic indexes usable while preserving existing embedding profiles.

### Mycel deliverables

Implement:

- semantic index backfill command/API
- source policy root selection
- non-nesting effective source roots
- `self` and `subtree` extraction
- source hashing
- skip when current source hash exists
- semantic index record write/read/search through `mycel-file`
- compatibility bridge from MVP embedding profiles to semantic indexes or side-by-side execution

Backfill must evaluate:

- semantic index endpoint/model/vector store binding
- enabled endpoint/model capability
- inference policy
- explicit credential grant with background use when running offline

Every endpoint call must append an accounting event.

### Knot PKM deliverables

- Extend PKM initialization/import flow to provision a default semantic index for the personal PKM domain behind a feature flag.
- Keep current MVP embedding profile generation as the default path until semantic search is enabled.
- Add server tests for provisioning default semantic index definitions without breaking import.

### Tests

Mycel unit/CLI tests:

- backfill enqueues/generates expected records
- `self`/`subtree` extraction parity with MVP source text
- effective roots do not nest
- stale record/latest record behavior
- missing policy means no inference
- missing grant means no endpoint call
- accounting emitted per endpoint call

Knot PKM tests:

- importer tests for domain-aware template/content import
- server tests for default semantic provisioning
- no Playwright unless visible UI changes are made

### Push gate

- Push Mycel first.
- Push Knot PKM default semantic provisioning second.

## Phase 6: Policy and Grant Enforcement, Query Planning, and Semantic Search

Goal: replace raw profile thinking with semantic-index query planning.

### Mycel deliverables

Implement:

- policy resolution with default deny
- deny-wins semantics, including inherited deny over more-specific allow
- restrict intersection semantics
- containment-only policy inheritance
- traversal stops at restricted/private subtrees
- grant resolution specificity:
  1. node/subtree
  2. semantic index
  3. domain
  4. space
- semantic search planner:
  - resolve requested scope
  - find applicable semantic indexes
  - filter by policy
  - group by `vector_space_key`
  - resolve query embedding grants
  - generate query embeddings
  - search vector stores
  - return provenance and warnings

Query embeddings may use any compatible explicit grant; they do not need the same grant that generated index content.

### Knot PKM deliverables

- Add a server-side semantic search path using Mycel semantic search.
- Keep fallback to current MVP profile search when semantic mode is disabled or provisioning is incomplete.
- Update search/chat tests to cover semantic mode warnings and fallback behavior.

### Tests

Mycel unit tests:

- all policy resolution rules
- containment moves do not imply synchronous endpoint calls
- grant specificity and conflict errors
- multi-index/multi-vector-space planning
- missing query credential skips index/group with warning
- query accounting events

Knot PKM tests:

- server API tests for semantic search success/fallback/warnings
- client unit tests only if response shape/UI changes
- Playwright if visible search UI behavior changes

### Push gate

- Push Mycel query planner commit to GitHub.
- Push Knot PKM semantic search integration to GitLab.

## Phase 7: Dirty Events, Analyzer, Workers, Cleanup, and Revocation

Goal: move refresh work out of PKM server debounce logic and into Mycel semantic maintenance.

### Mycel deliverables

Implement graph dirty event append transactionally with graph writes:

```text
graphs/<space_id>/semantic/events/graph-dirty-000001.ksem
```

Implement:

- one raw graph dirty event per graph transaction
- raw event idempotency by `txn_id`
- semantic config event consumption
- per-index analyzer checkpoints
- dirty queue coalescing by `semantic_index_id + target_node_id`
- semantic dirty work statuses and retry metadata
- background worker/maintainer APIs
- cleanup/delete work for:
  - `no_inference`
  - policy changes
  - credential revocation
  - grant revocation
  - source-policy changes
- external vector deletion interface hooks, even if only `mycel-file` is implemented now

Graph writes must not call model endpoints synchronously.

### Knot PKM deliverables

- Replace or disable PKM server-owned debounced embedding refresh when semantic mode is enabled.
- Add server controls/observability for semantic worker status if needed.
- Keep MVP refresh path for non-semantic mode until final cutover.

### Tests

Mycel unit/integration tests:

- graph write appends one raw dirty event
- analyzer replay is idempotent
- coalescing prevents duplicate dirty work
- moves dirty old and new effective roots
- deletes produce cleanup/tombstones
- policy changes and credential revocations enqueue cleanup
- background grant must have `allow_background_use = true`
- no live user session required for background work

Knot PKM tests:

- server tests for dirty refresh behavior under semantic mode
- importer tests confirm import does not synchronously call model endpoints
- Playwright only if UI status/controls are added

### Push gate

- Push Mycel worker/dirty-pipeline commit to GitHub.
- Push Knot PKM semantic worker integration to GitLab.

## Phase 8: Knot PKM UX, Settings, and Accounting Visibility

Goal: expose semantic provisioning, search/chat behavior, and accounting in the application where useful.

### Mycel deliverables

- Stabilize APIs needed by Knot PKM settings/search/chat/accounting UI.
- Update docs for any API/behavior changes discovered during integration.

### Knot PKM deliverables

Likely UI/server work:

- settings UI for model endpoints/credentials/grants/policies at the PKM level, or a simplified personal-PKM setup flow
- semantic index provisioning status
- search/chat warnings for skipped indexes/content due to policy or missing grants
- optional usage/accounting views or admin endpoints for token summaries
- local utility updates:
  - fake PKM recreation provisions semantic resources
  - OpenAI key registration creates credential + grant + policy + index

All UI changes require unit tests and Playwright tests.

### Tests

Knot PKM client:

- Jest tests for settings/search/chat components changed in this phase
- Playwright tests for:
  - adding/updating an AI credential if UI exists
  - enabling semantic search/indexing if UI exists
  - search result warnings/fallback if visible
  - accounting/usage view if visible

Knot PKM server/importer:

- server tests for semantic settings/accounting endpoints
- importer tests for semantic provisioning during initialization/import if applicable

Mycel:

- regression tests for APIs consumed by PKM

### Push gate

- Push Mycel API/doc adjustments first if needed.
- Push Knot PKM server/importer/client UI commits to GitLab.

## Phase 9: Migration, Deprecation, and Final Cutover

Goal: complete the refactor, remove legacy assumptions, and leave both systems on semantic indexes.

### Mycel deliverables

- Provide migration from MVP embedding profiles to semantic resources where possible:
  - provider key -> `InferenceCredential`
  - profile provider/model -> `ModelEndpoint` + `InferenceModel` + `ModelEndpointCapability`
  - profile source mode/template filters -> `SemanticIndex` source policy
  - profile ownership/defaults -> credential grants and app defaults
- Keep old CLI commands as deprecated wrappers for at least one release if practical.
- Remove or isolate profile-centric implementation internals after compatibility window.
- Finalize docs:
  - concepts
  - provisioning
  - credentials
  - policies
  - embedding generation
  - query planning
  - accounting
  - storage
  - CLI command docs

### Knot PKM deliverables

- Remove server assumptions that search targets a single embedding profile.
- Remove PKM-owned embedding refresh logic when semantic worker is mandatory.
- Ensure all production/dev provisioning uses semantic resources.
- Remove local `replace` directives before release/CI unless intentionally pinned to a local dependency.
- Update runbooks/utilities.

### Tests

Full regression gates:

Mycel:

```sh
make test
make build
```

Knot PKM:

```sh
cd knot_pkm/knot_pkm_server && go test ./...
cd knot_pkm/knot_pkm_importer && make test
cd knot_pkm/knot_pkm_client && npm test -- --runInBand && npm run build && npm run test:e2e
```

Manual/local smoke:

```sh
cd /Users/martinbeauvais/Projects/knotbase/Knotbase
LOGSEQ_SOURCE_DIR="$HOME/PKM/FakeLogseq" utilities/recreate_dev_knotpkm.sh --yes
OPENAI_API_KEY=sk-... utilities/add_fake_user_openai_key.sh
```

Then verify:

- import succeeds into the personal PKM domain
- semantic index provisioning exists
- background semantic maintenance runs
- semantic search returns results
- private/restricted subtrees are skipped
- token accounting summarizes usage by user, space, domain, and node

### Push gate

- Push final Mycel semantic implementation to GitHub.
- Push final Knot PKM cutover to GitLab.
- Tag/release only after CI passes in both systems.

## Final Acceptance Criteria

The development effort is complete when:

- Mycel exposes semantic indexes as the primary semantic search abstraction.
- Direct embedding-profile thinking is deprecated or wrapped for compatibility.
- Model endpoints, models, capabilities, vector stores, credentials, grants, policies, semantic indexes, embedding records, and accounting records exist as documented.
- `mycel-file` is the built-in/default vector store.
- No model endpoint call occurs without explicit policy allow/restrict and an explicit credential grant.
- Graph writes append dirty events but do not synchronously refresh embeddings.
- Background semantic maintenance can run without live user sessions using grants with `allow_background_use = true`.
- Disallowed/revoked embeddings are tombstoned/deleted and not searchable.
- Query planning targets semantic indexes, supports multiple vector spaces, and reports warnings for skipped indexes/content.
- Every model endpoint call appends an accounting record under `meta/accounting/inference-usage-*.kusag`.
- CLI usage reports work by period, principal/user, space, domain, node, operation, endpoint/model, grant, and status.
- Knot PKM provisions and uses semantic indexes for PKM search/chat/import flows.
- Unit tests cover core semantic behavior in Mycel and Knot PKM.
- Playwright tests cover user-visible UI changes.
- Documentation matches implemented behavior.
