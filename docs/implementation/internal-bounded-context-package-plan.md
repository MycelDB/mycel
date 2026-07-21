# Internal Bounded-Context Package Plan

## Goal

Continue the daemon-only package cleanup by organizing `internal/` around important Mycel themes/bounded contexts instead of horizontal implementation layers.

The target style is the new graph layout:

```text
internal/graph/
  model/      in-process graph records and template policy types
  storage/    graph segment/file persistence
  change/     neutral graph commit events/sinks
```

This plan proposes similar consolidation for semantic, space/access/template, identity/auth, blob, and graph runtime/query packages.

## Constraints

- Do not change public application contracts. Applications use `mycel-api` and `mycel-go-sdk`.
- Do not change on-disk JSON/storage schemas during package moves.
- Prefer mechanical `git mv` + import-path updates.
- Keep package names stable initially where it reduces churn, even when the import path changes.
- Avoid import cycles by preserving existing dependency direction.
- Keep daemon API/module wiring distinct from core bounded-context logic unless a later phase intentionally refactors daemon adapters.
- Run focused tests after each phase and `go test ./...` before moving to the next phase.

## Naming conventions

Use directories for bounded contexts and subdirectories for roles:

```text
internal/<context>/model
internal/<context>/storage
internal/<context>/change
internal/<context>/query
internal/<context>/accounting
```

Package names may remain domain-oriented for readability at call sites. For example:

```go
graph "github.com/myceldb/mycel/internal/graph/model"
semantic "github.com/myceldb/mycel/internal/semantic/model"
```

Avoid large single packages such as `internal/semantic` containing all semantic code. Keep clear subpackages.

## Current target map

### Already implemented

```text
internal/domain/graph       -> internal/graph/model
internal/graphstorage       -> internal/graph/storage
internal/graphchange        -> internal/graph/change
internal/domain/semantic    -> internal/semantic/model
internal/store/semantic     -> internal/semantic/storage
internal/store/accounting   -> internal/semantic/accounting
internal/domain/space       -> internal/space/model
internal/domain/access      -> internal/space/access
internal/store/spaces       -> internal/space/storage/spaces
internal/store/domains      -> internal/space/storage/domains
internal/store/acl          -> internal/space/storage/acl
internal/store/template     -> internal/graph/template/storage
internal/domain/identity    -> internal/identity/model
internal/domain/auth        -> internal/identity/auth
internal/store/user         -> internal/identity/storage/user
internal/store/session      -> internal/identity/storage/session
internal/blobstorage        -> internal/blob/storage
internal/query              -> internal/graph/query
internal/session/filesession -> internal/graph/filesession
internal/session/metadataindex -> internal/graph/metadataindex
```

### Proposed moves

```text
```

## Phase A: Semantic bounded context

Status: implemented. Semantic model, semantic persistence, and inference usage accounting now live under `internal/semantic/`.

### Goal

Put semantic model, semantic persistence, and inference usage accounting under the semantic context.

### Moves

```text
internal/domain/semantic  -> internal/semantic/model
internal/store/semantic   -> internal/semantic/storage
internal/store/accounting -> internal/semantic/accounting
```

Keep package names initially:

```text
package semantic     # in internal/semantic/model
package semantic     # or storesemantic? decide during move; stable package name can remain semantic
package accounting   # in internal/semantic/accounting
```

Recommended import aliases after move:

```go
semanticmodel "github.com/myceldb/mycel/internal/semantic/model"
semanticstorage "github.com/myceldb/mycel/internal/semantic/storage"
semanticaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
```

### Candidate command sketch

```sh
git mv internal/domain/semantic internal/semantic/model
git mv internal/store/semantic internal/semantic/storage
git mv internal/store/accounting internal/semantic/accounting
rg 'github.com/myceldb/mycel/internal/domain/semantic' -l --glob '*.go' \
  | xargs perl -pi -e 's#github.com/myceldb/mycel/internal/domain/semantic#github.com/myceldb/mycel/internal/semantic/model#g'
rg 'github.com/myceldb/mycel/internal/store/semantic' -l --glob '*.go' \
  | xargs perl -pi -e 's#github.com/myceldb/mycel/internal/store/semantic#github.com/myceldb/mycel/internal/semantic/storage#g'
rg 'github.com/myceldb/mycel/internal/store/accounting' -l --glob '*.go' \
  | xargs perl -pi -e 's#github.com/myceldb/mycel/internal/store/accounting#github.com/myceldb/mycel/internal/semantic/accounting#g'
gofmt -w internal/semantic
```

### Focused validation

```sh
go test ./internal/semantic/... ./internal/daemon/modules/semantic ./internal/daemon/api/admin ./internal/daemon/api/client ./internal/graph/filesession ./internal/cli/cmd
```

### Risks

- Semantic touches many packages: connectors, search, maintenance, backfill, vectorstore, daemon API adapters, CLI tests, and file sessions.
- Avoid creating dependencies from semantic storage back into semantic worker/search packages.
- `internal/embedding` remains separate for now to avoid mixing legacy migration/profile utilities with active semantic maintenance.

### Acceptance

```sh
rg 'internal/domain/semantic|internal/store/semantic|internal/store/accounting' --glob '*.go' && exit 1 || true
go test ./...
```

## Phase B: Space/access/domain/template bounded context

Status: implemented. Space/access models and space/domain/ACL storage now live under `internal/space/`. Template storage moved under `internal/graph/template/storage` because templates are graph schema/policy records.

### Goal

Group space ownership, graph domains, ACL/access, and possibly templates around the space/graph-schema context.

### Moves

```text
internal/domain/space   -> internal/space/model
internal/domain/access  -> internal/space/access
internal/store/spaces   -> internal/space/storage/spaces
internal/store/domains  -> internal/space/storage/domains
internal/store/acl      -> internal/space/storage/acl
internal/store/template -> internal/graph/template/storage
```

### Template decision

Decision: template storage lives at `internal/graph/template/storage`. Templates are graph schema/model policy records even though the space module currently wires the catalog manager.

### Focused validation

```sh
go test ./internal/space/... ./internal/graph/... ./internal/daemon/modules/space ./internal/daemon/api/admin ./internal/daemon/api/client ./internal/graph/filesession
```

### Risks

- `internal/graph/model` currently depends on space IDs/types. Moving `space/model` preserves that dependency but changes import paths broadly.
- ACL/access is used by the space module and CLI formatting.
- Template placement can cause churn in graph/session/template API adapters.

### Acceptance

```sh
rg 'internal/domain/(space|access)|internal/store/(spaces|domains|acl|template)' --glob '*.go' && exit 1 || true
go test ./...
```

## Phase C: Identity/auth bounded context

Status: implemented. Identity and auth models plus user/session persistence now live under `internal/identity/`.

### Goal

Group user identity, auth session records, user persistence, and refresh-session persistence.

### Moves

```text
internal/domain/identity -> internal/identity/model
internal/domain/auth     -> internal/identity/auth
internal/store/user      -> internal/identity/storage/user
internal/store/session   -> internal/identity/storage/session
```

Potential later move, only if it remains clean:

```text
internal/daemon/auth -> internal/identity/token OR keep as daemon auth middleware
```

Recommendation: leave `internal/daemon/auth` in the daemon adapter layer unless it becomes purely reusable token logic.

### Focused validation

```sh
go test ./internal/identity/... ./internal/daemon/modules/user ./internal/daemon/api/admin ./internal/daemon/api/client ./internal/daemon/auth ./internal/space/... ./internal/semantic/...
```

### Risks

- `identity.UserID` and related types are widely imported by space, semantic, accounting, embedding, and daemon APIs.
- Keep identity/model minimal and acyclic.
- Do not mix browser/application auth concerns into Mycel identity internals.

### Acceptance

```sh
rg 'internal/domain/(identity|auth)|internal/store/(user|session)' --glob '*.go' && exit 1 || true
go test ./...
```

## Phase D: Blob bounded context

Status: implemented. Blob persistence now lives under `internal/blob/storage`.

### Goal

Move blob persistence into a blob context, mirroring graph storage.

### Move

```text
internal/blobstorage -> internal/blob/storage
```

Keep daemon module code in:

```text
internal/daemon/modules/blob
```

Do not move daemon module code unless there is a broader daemon module refactor.

### Focused validation

```sh
go test ./internal/blob/... ./internal/daemon/modules/blob ./internal/daemon/api/client ./internal/graph/filesession
```

### Risks

- Low. This is mostly mechanical.
- Blob storage imports graph BlobID; keep that as model-level dependency only.

### Acceptance

```sh
rg 'internal/blobstorage' --glob '*.go' && exit 1 || true
go test ./...
```

## Phase E: Graph query/session runtime cleanup

Status: implemented. Graph query, file-backed graph sessions, and graph metadata indexing now live under `internal/graph/`. `internal/session/api` remains as the internal daemon/session contract boundary.

### Goal

Move graph-specific query/session runtime packages under the graph context if it still improves clarity after earlier phases.

### Moves

```text
internal/query                 -> internal/graph/query
internal/session/filesession   -> internal/graph/filesession
internal/session/metadataindex -> internal/graph/metadataindex
```

Decision: use `internal/graph/filesession` instead of `internal/graph/session` so the import path continues to reflect the package's file-backed implementation and avoids confusion with daemon client session lifecycle code.

Keep `internal/session/api` until its API is narrowed or a better long-term home is chosen. It is used by daemon modules, semantic backfill, CLI adapters, and file sessions.

### Focused validation

```sh
go test ./internal/graph/... ./internal/session/... ./internal/daemon/modules/graph ./internal/daemon/api/client ./internal/semantic/backfill
```

### Risks

- `session` is overloaded: daemon client session lifecycle vs file graph sessions.
- `internal/session/api` exposes graph operations, template import aliases, and semantic search types. Moving it too early may produce confusing dependencies.
- Consider this phase optional/deferred.

### Acceptance

```sh
rg 'internal/query|internal/session/(filesession|metadataindex)' --glob '*.go' && exit 1 || true
go test ./...
```

## Phase F: Daemon module relocation — defer by default

### Goal

Only if desired later, move daemon module implementations into their bounded contexts.

Potential examples:

```text
internal/daemon/modules/graph    -> internal/graph/module
internal/daemon/modules/semantic -> internal/semantic/module
internal/daemon/modules/space    -> internal/space/module
```

Recommendation: defer. The current `internal/daemon` layer is a useful adapter/composition boundary, and moving modules can blur core domain logic with daemon runtime wiring.

## Dependency rules

Preserve these directional rules:

```text
model packages        -> tiny ID/model dependencies only
storage packages      -> model packages, internal/filestore when needed
change/event packages -> model packages only; no semantic maintenance imports
daemon modules        -> bounded-context packages + daemon runtime concerns
daemon API adapters   -> daemon modules + proto conversion
```

Specific constraints:

- `internal/graph/change` must stay semantic-neutral.
- `internal/semantic/maintenance` may depend on graph change events, not vice versa.
- `internal/graph/model` should not depend on graph storage/change/query/session packages.
- `internal/semantic/storage` should not depend on semantic workers/search/connectors.
- `internal/identity/model` should not depend on space/semantic/auth storage.

## Final acceptance for the full cleanup

```sh
cd myceldb/mycel
rg 'internal/domain|internal/store|internal/blobstorage|internal/graphstorage|internal/graphchange|internal/query|internal/session/(filesession|metadataindex)' --glob '*.go'
go list ./... | rg '^github.com/myceldb/mycel/(domain|store|query|engine|session)(/|$)' && exit 1 || true
scripts/check-public-surface.sh --workspace /Users/martinbeauvais/Projects/knotbase/Knotbase --strict
go test ./...
make build
git diff --check
```

## Recommended order

1. Phase A: Semantic model/storage/accounting.
2. Phase B: Space/access/domain storage; decide template home explicitly.
3. Phase C: Identity/auth/user/session storage.
4. Phase D: Blob storage.
5. Phase E: Graph query/session runtime, optional.
6. Phase F: Daemon module relocation, deferred unless clearly beneficial.
