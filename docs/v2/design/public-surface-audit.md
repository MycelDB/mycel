# Mycel Public Go Surface Audit

## Context

Mycel is moving to a daemon-only architecture. The stable public contracts should be:

- `mycel-api` protobuf definitions and generated packages
- `mycel-go-sdk` daemon client helpers
- binaries under `cmd/`

Packages in `mycel` outside `internal/` were mostly leftovers from the embedded-library era. This audit tracks their internalization and verifies the remaining application-facing contracts live in `mycel-api` and `mycel-go-sdk`.

## Current external Go packages in `mycel`

```text
github.com/myceldb/mycel
```

`domain/*`, `query`, and `store/*` have been internalized under `internal/domain/*`, `internal/graph/query`, and `internal/store/*`.

## Internal usage and replaceability

| Package | Direct mycel importers | External consumers found | Replaceability / recommendation |
|---|---:|---:|---|
| `github.com/myceldb/mycel` | 0 | 0 | Doc-only root package. Harmless, but not a meaningful public API. |
| `internal/space/access` | 3 | 0 | Internal ACL model. Internalized in Phase 3. |
| `internal/identity/auth` | 4 | 0 | Internal refresh-session/audit/token model. Internalized in Phase 3. |
| `internal/graph/model` | 29 | former importer only | Core in-process graph model. External use replaced with `mycel-api` graph/template proto messages and SDK helpers; internalized in Phase 3. |
| `internal/identity/model` | 21 | former importer only | Core ID/user model. External use replaced with proto string IDs and SDK user/admin helpers; internalized in Phase 3. |
| `internal/semantic/model` | 14 | 0 | Internal semantic/inference/vector/maintenance records. Internalized in Phase 3. |
| `internal/space/model` | 24 | former importer only | Core space model. External use replaced with SDK `SpaceInfo` / proto IDs; internalized in Phase 3. |
| `internal/graph/query` | 2 | 0 | In-memory session query builder, used only by internal session implementation. Internalized in Phase 2. |
| `internal/semantic/accounting` | 3 | 0 | File-backed implementation. Internalized in Phase 2. |
| `internal/space/storage/acl` | 1 | 0 | File-backed implementation. Internalized in Phase 2. |
| `internal/space/storage/domains` | 1 | 0 | File-backed implementation. Internalized in Phase 2. |
| `internal/semantic/storage` | 7 | 0 | File-backed semantic resources and maintenance queues. Internalized in Phase 2. |
| `internal/identity/storage/session` | 3 | 0 | File-backed refresh-session store. Internalized in Phase 2. |
| `internal/space/storage/spaces` | 1 | 0 | File-backed space store. Internalized in Phase 2. |
| `internal/graph/template/storage` | 5 | 0 | File-backed template store plus import DTOs. Internalized in Phase 2; API boundary uses proto import/export messages. |
| `internal/identity/storage/user` | 0 | 0 | Internalized in Phase 2 to preserve tests/old store behavior while removing public access. |

## Workspace external consumers

A workspace search found no live Go consumers of non-API Mycel packages outside the `mycel` repo after the `knot_pkm_importer` daemon migration.

Checked workspace components do not import `github.com/myceldb/mycel/...` directly:

- `knot_pkm/knot_pkm_server` uses `mycel-go-sdk` and `mycel-api`
- `myceldb/mycel-go-sdk` uses `mycel-api`
- `myceldb/mycelbench` uses `mycel-go-sdk` and `mycel-api`

## Replacement path for `knot_pkm_importer`

| Current importer dependency | Replacement |
|---|---|
| `domain/identity` | Plain username/password config through `mycel-go-sdk`; proto/admin user IDs as strings. |
| `domain/space` | SDK `SpaceInfo` / proto `Space` string IDs. |
| `domain/graph` | `mycel-api` client graph/template messages (`clientv1.Node`, `Edge`, `Template`, `NodeCreate`, `EdgeCreate`, `GraphOperation`) plus SDK graph/session helpers. |
| `engine` | `mycel-go-sdk` connection/auth/admin/session clients against a running `myceld`. |
| `session` | SDK session/transaction/graph helpers; add SDK helpers where current raw proto clients are too verbose. |

## Recommended migration order

1. **Migrate `knot_pkm_importer` to daemon SDK/API.** Done; this removed the only known workspace production consumer of old Mycel Go implementation packages.
2. **Move low-risk packages first.** Done:
   - `query` -> `internal/graph/query`
   - `store/*` -> `internal/store/*`
3. **Move domain packages as a coordinated mechanical refactor.** Done:
   - `domain/*` -> `internal/domain/*`
4. **Add guardrails.** Done:
   - CI/script check that sibling production repos do not import `github.com/myceldb/mycel/(domain|store|query|engine|session)`
   - documentation that `mycel` is daemon-only and not a public library API

## Risks

- Moving `internal/graph/model` and `internal/semantic/model` remains high-churn for future refactors because they are imported by many internal packages and API adapters.
- `internal/graph/template/storage` includes import document structs used by session/template APIs; ensure proto/client import-export messages remain the public boundary.
- Old on-disk JSON is decoded into these domain structs. Moving packages does not change JSON, but any type/field renames should be avoided in the same phase.
- Reintroducing public implementation packages would undermine the daemon-only boundary; keep guardrails strict.

## Audit commands

```sh
# Public packages in mycel
go list ./... | rg '^github.com/myceldb/mycel($|/(domain|store|query|engine|session)(/|$))'

# Workspace external consumers
cd /Users/martinbeauvais/Projects/knotbase/Knotbase
rg 'github\.com/myceldb/mycel(/(domain|store|query|engine|session|$)|")' \
  --glob '!*myceldb/mycel/**' \
  --glob '!**/go.sum' \
  --glob '!**/vendor/**'

# Verify implementation packages remain internalized
go list ./... | rg '^github.com/myceldb/mycel/(domain|store|query|engine|session)(/|$)' && exit 1 || true
```
