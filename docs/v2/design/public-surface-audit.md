# Mycel Public Go Surface Audit

## Context

Mycel is moving to a daemon-only architecture. The stable public contracts should be:

- `mycel-api` protobuf definitions and generated packages
- `mycel-go-sdk` daemon client helpers
- binaries under `cmd/`

Packages in `mycel` outside `internal/` are mostly leftovers from the embedded-library era. This audit classifies whether those packages are still used, whether they are externally consumed, and whether they are replaceable.

## Current external Go packages in `mycel`

```text
github.com/myceldb/mycel
github.com/myceldb/mycel/domain/access
github.com/myceldb/mycel/domain/auth
github.com/myceldb/mycel/domain/graph
github.com/myceldb/mycel/domain/identity
github.com/myceldb/mycel/domain/semantic
github.com/myceldb/mycel/domain/space
github.com/myceldb/mycel/query
github.com/myceldb/mycel/store/accounting
github.com/myceldb/mycel/store/acl
github.com/myceldb/mycel/store/domains
github.com/myceldb/mycel/store/semantic
github.com/myceldb/mycel/store/session
github.com/myceldb/mycel/store/spaces
github.com/myceldb/mycel/store/template
github.com/myceldb/mycel/store/user
```

## Internal usage and replaceability

| Package | Direct mycel importers | External consumers found | Replaceability / recommendation |
|---|---:|---:|---|
| `github.com/myceldb/mycel` | 0 | 0 | Doc-only root package. Harmless, but not a meaningful public API. |
| `domain/access` | 3 | 0 | Internal ACL model. Replaceable by proto/admin API at boundaries. Move with `store/acl` and space module internals. |
| `domain/auth` | 4 | 0 | Internal refresh-session/audit/token model. Move with `store/session` and user/auth daemon modules. |
| `domain/graph` | 29 | yes, importer only | Core in-process graph model. Replace external use with `mycel-api` graph/template proto messages and SDK helpers. High-churn internal move. |
| `domain/identity` | 21 | yes, importer only | Core ID/user model. Replace external use with proto string IDs and SDK user/admin helpers. Move early if moving all domain packages. |
| `domain/semantic` | 14 | 0 | Internal semantic/inference/vector/maintenance records. Replace external use with Admin/Client semantic and inference proto messages. Move with semantic stores/workers. |
| `domain/space` | 24 | yes, importer only | Core space model. Replace external use with SDK `SpaceInfo` / proto IDs. Move with identity/graph. |
| `query` | 2 | 0 | In-memory session query builder, used only by internal session implementation. Good early candidate for `internal/query` or `internal/session/query`. |
| `store/accounting` | 3 | 0 | File-backed implementation. Imports `internal/filestore`; not a clean public API. Move to `internal/store/accounting`. |
| `store/acl` | 1 | 0 | File-backed implementation. Move to `internal/store/acl`. |
| `store/domains` | 1 | 0 | File-backed implementation. Move to `internal/store/domains`. |
| `store/semantic` | 7 | 0 | File-backed semantic resources and maintenance queues. Move to `internal/store/semantic` after/with semantic domain path updates. |
| `store/session` | 3 | 0 | File-backed refresh-session store. Move to `internal/store/session`. |
| `store/spaces` | 1 | 0 | File-backed space store. Move to `internal/store/spaces`. |
| `store/template` | 5 | 0 | File-backed template store plus import DTOs. Move to `internal/store/template`; API boundary should use proto import/export messages. |
| `store/user` | 0 | 0 | Appears unused by production code. Candidate for deletion after confirming tests/old migration paths do not need it. |

## Workspace external consumers

A workspace search found one live Go consumer of non-API Mycel packages outside the `mycel` repo:

```text
knot_pkm/knot_pkm_importer
```

Imports:

```text
github.com/myceldb/mycel/domain/graph
github.com/myceldb/mycel/domain/identity
github.com/myceldb/mycel/domain/space
github.com/myceldb/mycel/engine
github.com/myceldb/mycel/session
```

`knot_pkm_importer/go.mod` currently requires:

```text
github.com/myceldb/mycel v0.1.0-alpha.6
```

The importer is therefore still an embedded-era consumer. It should be migrated before making a release that hides or removes these packages.

Other checked workspace components do not import `github.com/myceldb/mycel/...` directly:

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

1. **Migrate `knot_pkm_importer` to daemon SDK/API.** This removes the only known workspace production consumer of old Mycel Go implementation packages.
2. **Move low-risk packages first:**
   - `query` -> `internal/query` or `internal/session/query`
   - `store/*` -> `internal/store/*`
   - delete or internalize `store/user` if still unused
3. **Move domain packages as a coordinated mechanical refactor:**
   - `domain/identity`
   - `domain/space`
   - `domain/graph`
   - `domain/access`
   - `domain/auth`
   - `domain/semantic`
4. **Add guardrails:**
   - CI/script check that sibling production repos do not import `github.com/myceldb/mycel/(domain|store|query|engine|session)`
   - documentation that `mycel` is daemon-only and not a public library API

## Risks

- Moving `domain/graph` and `domain/semantic` is high-churn because they are imported by many internal packages and API adapters.
- `store/template` includes import document structs used by session/template APIs; ensure proto/client import-export messages remain the public boundary.
- Old on-disk JSON is decoded into these domain structs. Moving packages does not change JSON, but any type/field renames should be avoided in the same phase.
- Hiding packages before migrating `knot_pkm_importer` will break that module unless it remains pinned to an older `mycel` release.

## Audit commands

```sh
# Public packages in mycel
go list ./... | rg '^github.com/myceldb/mycel/(domain|store|query)(/|$)|^github.com/myceldb/mycel$'

# Workspace external consumers
cd /Users/martinbeauvais/Projects/knotbase/Knotbase
rg 'github\.com/myceldb/mycel(/(domain|store|query|engine|session|$)|")' \
  --glob '!*myceldb/mycel/**' \
  --glob '!**/go.sum' \
  --glob '!**/vendor/**'

# Reverse importers inside mycel
go list -deps ./... | rg '^github.com/myceldb/mycel/(domain|store|query)(/|$)' | sort -u
```
