# Intelligence Access Raft authority implementation plan

## Status

Implemented. This plan implements the design in
[Design / Admin / Intelligence Access Raft authority](../../design/admin/intelligence-access-raft-authority.md).

The production bug was that `CreateIntelligenceProfile` still wrote to
the standalone inference subsystem. In clustered/Raft mode that path correctly
failed closed because the inference Raft executor is not wired:

```text
clustered local write rejected: raft executor is not configured for this subsystem
```

The fix makes Intelligence profiles semantic-authoritative, raft-owned
space configuration, matching credential grants and access policies.

## Goals

- Make `AdminIntelligenceAccessProfileService` create/update/delete durable
  profiles through `semantic.space.mutation.v1`.
- Keep direct standalone inference writes fail-closed in clustered/Raft mode.
- Keep the standalone inference profile store as a derived runtime projection.
- Ensure semantic workers and graph automations can resolve profiles, grants,
  policies, credentials, endpoints, models, and capabilities after Raft apply,
  snapshot reload, restart, and request retries.
- Preserve secret-safety: no plaintext credentials, tokens, private keys, raw
  provider requests/responses, raw semantic source, or raw vectors in metadata,
  logs, events, or tests.

## Non-goals

- Do not implement a full inference Raft executor in this tranche.
- Do not make Activity Events authoritative.
- Do not introduce automatic cross-node repair or divergent PVC reconciliation.
- Do not change public protobuf API names unless implementation proves it is
  unavoidable.
- Do not preserve compatibility with unreleased transitional file shapes beyond
  simple local developer migration if needed.

## Current-state summary

Already semantic/Raft-owned or mostly aligned:

- inference package/catalog application;
- model endpoints;
- models;
- endpoint capabilities;
- vector-store backend configuration;
- encrypted secret metadata;
- credential metadata;
- credential grants;
- access policies;
- semantic generation rules.

Current outlier:

- Intelligence profiles are created, listed, updated, and deleted from the
  standalone inference space store in `internal/daemon/api/admin/inference_profiles.go`.

Relevant existing files:

- `internal/daemon/api/admin/inference_profiles.go`
- `internal/daemon/api/admin/inference_grants.go`
- `internal/daemon/api/admin/inference_policies.go`
- `internal/daemon/api/admin/inference_sync.go`
- `internal/daemon/api/admin/inference_mapping.go`
- `internal/inference/service/module.go`
- `internal/semantic/model/semantic.go`
- `internal/semantic/storage/interface.go`
- `internal/semantic/storage/file_store.go`
- `internal/semantic/service/wal_space_methods.go`
- `internal/semantic/service/wal_wrappers.go`
- `internal/semantic/service/raft_snapshot.go`
- `internal/daemon/app/raft_experimental_test.go`
- `internal/clustering/consensus/raft_record_coverage_test.go`

## Tranche IA-RAFT-1 — Semantic profile domain/storage

Add semantic-authoritative storage for Intelligence profiles.

Tasks:

1. Add a semantic-domain profile type, for example
   `domainsemantic.IntelligenceProfile`, with fields matching the public
   `admin.v1.IntelligenceProfile` shape and the inference resolver's profile
   needs:
   - ID;
   - space ID;
   - key;
   - display name;
   - description;
   - operation;
   - purpose;
   - domain IDs;
   - capability refs;
   - endpoint refs;
   - model refs;
   - required features;
   - privacy requirement;
   - default parameters;
   - enabled;
   - created by;
   - created/updated timestamps;
   - metadata.
2. Add normalization and validation helpers:
   - require non-empty space ID, key, and operation;
   - trim strings and canonicalize key casing consistently with current
     inference profile behavior;
   - reject unsafe metadata only if an existing metadata sanitizer pattern is
     available; otherwise keep metadata pass-through but do not add secrets.
3. Extend semantic storage interfaces:
   - `UpsertIntelligenceProfile(ctx, profile)`;
   - `ListIntelligenceProfiles(ctx)`;
   - `DeleteIntelligenceProfile(ctx, id)`.
4. Implement file-store persistence under each space semantic directory.
5. Add storage tests for:
   - create assigns ID/timestamps;
   - upsert by ID;
   - idempotent retry by stable key if supported by the storage resolver;
   - list returns persisted profiles after reload;
   - delete removes profile;
   - invalid profile validation.

Acceptance criteria:

- Semantic storage can persist and reload Intelligence profiles without using
  the standalone inference store.
- Existing semantic grant/policy/rule tests still pass.

Suggested validation:

```sh
go test ./internal/semantic/model ./internal/semantic/storage
```

## Tranche IA-RAFT-2 — Semantic WAL/Raft mutations

Make profile storage available through the existing semantic space mutation
record.

Tasks:

1. Extend semantic WAL canonicalization/resolution with profile upsert/delete:
   - `intelligence_profile.upsert`;
   - `intelligence_profile.delete`.
2. Extend `applySemanticSpaceMutation` to apply profile mutations.
3. Ensure `walSpaceManager` profile upserts assign stable IDs/timestamps before
   append/propose, so the API can return the committed identity.
4. Verify profile mutations use `semantic.space.mutation.v1`, which is already
   owned by partition Raft in clustered mode.
5. Add tests covering:
   - WAL-backed profile upsert/list/delete;
   - Raft-backed profile upsert/list/delete if an existing semantic raft test
     harness can be reused;
   - unsupported record ownership remains unique.

Acceptance criteria:

- Profile create/update/delete can be proposed through semantic partition Raft.
- No new `inference.space.mutation.v1` ownership is introduced.

Suggested validation:

```sh
go test ./internal/semantic/service ./internal/daemon/app ./internal/clustering/consensus
```

## Tranche IA-RAFT-3 — Mapping and derived inference projection

Add conversion and projection for profiles from semantic authority to the
standalone inference runtime store.

Tasks:

1. Add mapping helpers:
   - public profile request/response <-> semantic profile;
   - semantic profile -> inference profile;
   - inference profile -> public response only where still needed for reference
     compatibility.
2. Extend `derivedInferenceSpaceSync` in
   `internal/daemon/api/admin/inference_sync.go` with:
   - `UpsertDerivedProfile(context.Context, string, domaininference.Profile)`;
   - optionally `DeleteDerivedProfile(context.Context, string, profileID)` if
     delete sync is needed by API paths.
3. Implement derived profile upsert/delete on `internal/inference/service.Module`
   using the base space manager, bypassing only the derived projection write gate.
4. Keep normal `inference.SpaceManager(...).UpsertProfile(...)` gated in
   clustered mode.
5. Add tests modeled after the existing package sync test:
   - direct normal inference profile write fails with a rejecting local-write
     gate;
   - admin/semantic profile path syncs a derived inference profile despite that
     rejecting gate.

Acceptance criteria:

- Runtime projection has the profile immediately after successful admin create.
- Projection sync does not create an independent durable authority.

Suggested validation:

```sh
go test ./internal/daemon/api/admin ./internal/inference/service
```

## Tranche IA-RAFT-4 — Admin profile service migration

Switch `AdminIntelligenceAccessProfileService` to the semantic-authoritative
path.

Tasks:

1. Change `CreateIntelligenceProfile` to:
   - authorize as today;
   - parse/build a semantic profile from the request;
   - `beginSemanticMutation`;
   - open semantic space manager;
   - upsert profile through semantic space manager;
   - sync derived inference profile;
   - return mapped public response.
2. Change `ListIntelligenceProfiles` to read semantic profiles from the
   semantic space manager and keep existing filters:
   - domain;
   - operation;
   - purpose;
   - include disabled;
   - pagination.
3. Change `GetIntelligenceProfile` and profile resolution helpers to use
   semantic profile authority.
4. Change `SetIntelligenceProfileEnabled` to upsert through semantic mutation
   and sync projection.
5. Change `DeleteIntelligenceProfile` to:
   - resolve semantic profile;
   - preserve reference-safety checks against semantic rules and standalone
     usage/decision references;
   - delete through semantic mutation;
   - delete or tombstone the derived inference projection;
   - return the deleted ID.
6. Keep error messages actionable and preserve gRPC status classifications.

Acceptance criteria:

- The API surface behaves the same from clients' perspective.
- In clustered/Raft mode, profile writes no longer hit the standalone inference
  local-write gate.
- Reads return the authoritative semantic profile state.

Suggested validation:

```sh
go test ./internal/daemon/api/admin
```

## Tranche IA-RAFT-5 — Projection rebuild on apply/reload/snapshot

Ensure every node can resolve profiles after applying committed semantic state.

Tasks:

1. Add profile projection hooks after semantic space apply, or provide a
   rebuild-from-semantic method invoked after applies/reloads.
2. Extend snapshot export/import to include semantic-authoritative profiles.
3. Ensure `ReloadAfterSnapshot` or equivalent semantic/inference reload paths
   rebuild derived inference profiles, grants, policies, catalog resources, and
   credentials as needed.
4. Consider a bounded startup reconciliation that rebuilds inference projection
   from semantic authoritative stores. It must be local projection rebuild only,
   not cross-node repair.
5. Add tests for:
   - profile survives semantic snapshot/reload;
   - projection is recreated after inference space cache reset;
   - runtime resolver can find a profile created before restart/reload.

Acceptance criteria:

- A node that did not handle the original admin RPC can resolve the profile after
  applying Raft state.
- Restart/snapshot reload does not lose the profile projection.

Suggested validation:

```sh
go test ./internal/semantic/service ./internal/inference/service ./internal/daemon/api/admin
```

## Tranche IA-RAFT-6 — End-to-end clustered provisioning validation

Prove the original Knot PKM failure is fixed without weakening fail-closed
behavior.

Tasks:

1. Add or update a Mycel clustered integration test that creates:
   - endpoint/model/capability/vector-store package;
   - encrypted credential;
   - embedding and generation profiles;
   - credential grants;
   - access policies;
   - semantic rule and/or automation binding referencing profiles.
2. If practical, add a Knot PKM integration or managed-fixture test that retries
   `mycel_intelligence_access_provisioning` after a daemon fix and reaches
   `provisioned`.
3. Verify a profile created through one cluster entrypoint can be resolved by
   semantic maintenance or automation execution on another node.
4. Verify direct standalone inference writes still fail closed in clustered mode.
5. Record validation commands/results in the implementation PR or a follow-up
   validation note.

Acceptance criteria:

- The observed onboarding error no longer occurs:

  ```text
  create embedding intelligence profile: rpc error: code = Unavailable desc = clustered local write rejected: raft executor is not configured for this subsystem
  ```

- Provisioning can be retried and converges to usable Intelligence Access
  resources.
- Semantic embedding/search and graph automation/page-summary paths can resolve
  the provisioned resources.

Suggested validation:

```sh
make test
make docs-check
git diff --check
# For raft-sensitive validation, as time permits:
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
```

Destructive Compose/K3s validation must only be run when explicitly requested:

```sh
make test-compose-cluster
make test-k3s-cluster
```

## Rollout and recovery notes

- Existing failed Knot PKM provisioning records should be explicitly retried by
  the application/operator after deploying the fixed daemon.
- Mycel should not automatically repair application-side provisioning state.
- If partial profile/grant/policy resources were created by a previous attempt,
  retry behavior should converge by stable keys/scopes rather than creating
  duplicate active resources.
- Operators should verify resources through Admin Intelligence Access list APIs
  and application provisioning status.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Profile duplicated on onboarding retry | Canonicalize keys and resolve existing profile by stable key before creating a new ID. |
| Projection present only on the RPC node | Rebuild projection during semantic Raft apply/reload, not only in Admin API handlers. |
| Runtime resolver reads stale projection | Fail closed with actionable errors; add reload/rebuild tests. |
| Secret leakage in profile metadata/events/tests | Do not add secret fields; audit new metadata and logs. |
| Split authority between semantic and inference stores | Reads/writes for durable config use semantic; inference writes are derived only. |
| Snapshot misses profile state | Include profiles in semantic snapshot export/import tests. |

## Definition of done

- Design doc and implementation plan are linked from docs indexes.
- `CreateIntelligenceProfile`, list/get/update/delete, grants, policies,
  credentials, catalog resources, semantic rules, and automation consumers follow
  the documented authority model.
- Clustered/Raft profile provisioning succeeds while direct standalone inference
  durable writes remain fail-closed.
- Tests cover rejecting local-write gate, semantic authority, projection sync,
  snapshot/reload, and at least one clustered provisioning path.
- Documentation checks and normal code validation pass.
