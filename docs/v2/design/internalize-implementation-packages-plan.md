# Internalize Mycel Implementation Packages Plan

## Goal

Make `mycel-api` protobuf definitions and `mycel-go-sdk` the supported public application contracts, and move Mycel daemon implementation types behind Go `internal/` package boundaries.

In daemon-only Mycel, external applications should not import:

```text
github.com/myceldb/mycel/domain/...
github.com/myceldb/mycel/store/...
github.com/myceldb/mycel/query
github.com/myceldb/mycel/engine
github.com/myceldb/mycel/session
```

Instead, applications should use:

```text
github.com/myceldb/mycel-api/gen/go/...
github.com/myceldb/mycel-go-sdk
```

This plan builds on [Mycel Public Go Surface Audit](public-surface-audit.md).

## Non-goals

- Do not change on-disk storage JSON schemas while moving packages.
- Do not rename domain structs or fields in the same phase as moving packages.
- Do not remove daemon Admin/Client API methods as part of this work.
- Do not remove the migration-only legacy embedding profile/key reader until the legacy migration window is explicitly closed.
- Do not force consumers to use raw proto clients when a small SDK helper can preserve ergonomics.

## Current blockers

One known workspace production consumer still imports embedded-era Mycel packages:

```text
knot_pkm/knot_pkm_importer
```

Current imports include:

```text
github.com/myceldb/mycel/domain/graph
github.com/myceldb/mycel/domain/identity
github.com/myceldb/mycel/domain/space
github.com/myceldb/mycel/engine
github.com/myceldb/mycel/session
```

This consumer must be migrated before public implementation packages can be removed or made inaccessible in a released Mycel version.

## Phase 0: Freeze the intended boundary

### Tasks

- Document that `mycel` is daemon-only and not an application library API.
- Add a checked script that fails on external consumer imports of old packages.
- Run the script initially in report-only mode if `knot_pkm_importer` has not yet migrated.
- Add a Mycel-local check that no new packages are added outside approved public locations.

Approved public locations:

```text
cmd/                 binaries only
internal/            daemon implementation
README/docs          documentation
doc.go              optional root module documentation
```

The actual stable public API lives in sibling repos:

```text
mycel-api
mycel-go-sdk
```

### Candidate script

```sh
#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-/Users/martinbeauvais/Projects/knotbase/Knotbase}"
rg 'github\.com/myceldb/mycel(/(domain|store|query|engine|session|$)|")' \
  "$ROOT" \
  --glob '!*myceldb/mycel/**' \
  --glob '!**/go.sum' \
  --glob '!**/vendor/**'
```

### Acceptance

```sh
cd myceldb/mycel
git diff --check
go test ./...
```

If importer is not yet migrated, the external-import script may be allowed to report known hits only. After Phase 1, it must be zero-hit.

## Phase 1: Migrate `knot_pkm_importer` to daemon SDK/API

### Goal

Remove the only known workspace production dependency on Mycel embedded implementation packages.

### Replacement mapping

| Current importer dependency | Replacement |
|---|---|
| `domain/identity` | Plain username/password config and proto string IDs. |
| `domain/space` | SDK/proto space IDs as strings. |
| `domain/graph` | `mycel-api` client graph/template messages plus SDK helpers. |
| `engine` | `mycel-go-sdk` daemon connection/admin/session clients. |
| `session` | SDK session/transaction/graph helpers; add SDK helpers for gaps. |

### Tasks

1. Inspect importer flows:
   - init/provisioning
   - template provisioning
   - Logseq graph import
   - delete/replace existing imported journal/page nodes
   - tests and fixtures
2. Add missing SDK helpers if raw proto usage would make importer code brittle. Likely helper areas:
   - ensure/list/import templates
   - open session / transaction wrapper
   - apply graph operations
   - list nodes/edges/templates
   - resolve default domain
3. Update importer config:
   - remove `DataDir`/embedded engine settings as runtime requirements
   - add daemon address/TLS/auth config
   - document that `myceld` must be running
4. Replace embedded engine calls with SDK/Admin/Client API calls.
5. Replace `domain/graph` structs with proto request/response types or local importer DTOs converted to proto at the boundary.
6. Update importer tests:
   - unit-test pure Logseq/template conversion without Mycel runtime
   - integration-test daemon import with a test `myceld` fixture or skip unless daemon fixture is available
7. Remove `github.com/myceldb/mycel` from `knot_pkm_importer/go.mod`.

### Acceptance

```sh
cd knot_pkm/knot_pkm_importer
go test ./...
rg 'github\.com/myceldb/mycel' --glob '*.go' --glob '!**/go.sum'

cd /Users/martinbeauvais/Projects/knotbase/Knotbase
rg 'github\.com/myceldb/mycel(/(domain|store|query|engine|session|$)|")' \
  --glob '!*myceldb/mycel/**' \
  --glob '!**/go.sum' \
  --glob '!**/vendor/**'
```

Expected result: no production Go imports of `github.com/myceldb/mycel/...` outside the Mycel repo.

### Risks

- Importer may currently rely on embedded read-your-writes/session behavior not mirrored by existing SDK helpers.
- Large imports may need daemon transaction batching and request-size awareness.
- Test fixtures may require a daemon fixture similar to PKM server daemon tests.

## Phase 2: Move low-risk Mycel packages under `internal/`

### Goal

Internalize packages with no external consumers and minimal dependency fanout.

### Packages

```text
query                -> internal/query or internal/session/query
store/accounting     -> internal/store/accounting
store/acl            -> internal/store/acl
store/domains        -> internal/store/domains
store/semantic       -> internal/store/semantic
store/session        -> internal/store/session
store/spaces         -> internal/store/spaces
store/template       -> internal/store/template
store/user           -> delete if still unused, otherwise internal/store/user
```

### Recommended order

1. `query`
   - direct importers: `internal/session/api`, `internal/session/filesession`
   - low risk; validates the mechanical move pattern
2. Store packages with one direct importer:
   - `store/acl`
   - `store/domains`
   - `store/spaces`
3. Store packages used by auth/session/template paths:
   - `store/session`
   - `store/template`
4. Semantic/accounting stores:
   - `store/accounting`
   - `store/semantic`
5. `store/user`
   - delete if no non-test production usage remains

### Tasks per package

For each package:

1. Move directory to `internal/...`.
2. Update imports mechanically.
3. Keep package names stable where practical to reduce churn.
4. Run focused tests for direct importers.
5. Run `go test ./...` before moving to the next package group.

Example:

```sh
git mv query internal/query
rg 'github.com/myceldb/mycel/query' -l | xargs perl -pi -e 's#github.com/myceldb/mycel/query#github.com/myceldb/mycel/internal/query#g'
go test ./internal/session/api ./internal/session/filesession ./internal/query
```

### Acceptance

```sh
cd myceldb/mycel
go list ./... | rg '^github.com/myceldb/mycel/(store|query)(/|$)' && exit 1 || true
go test ./...
git diff --check
```

### Risks

- `store/template` exports import document DTOs currently used by session APIs. This is OK internally, but verify that external import/export API uses proto messages.
- `store/semantic` is large and central to maintenance; isolate this move in its own commit.
- Package alias names such as `storesemantic` should be updated consistently to avoid confusing imports.

## Phase 3: Move domain packages under `internal/domain/`

### Goal

Make in-process domain structs implementation-only. Proto/API messages remain the public DTOs.

### Packages

```text
domain/identity   -> internal/domain/identity
domain/space      -> internal/domain/space
domain/graph      -> internal/domain/graph
domain/access     -> internal/domain/access
domain/auth       -> internal/domain/auth
domain/semantic   -> internal/domain/semantic
```

### Dependency order

The domain dependency graph is:

```text
identity
  -> space
    -> graph
      -> semantic
access -> identity + space
auth   -> identity
query  -> graph
store/* -> domain/*
internal/* -> domain/*
```

Recommended move sequence:

1. `identity`
2. `space`
3. `graph`
4. `access`
5. `auth`
6. `semantic`

However, because many packages import multiple domain packages, it may be safer to move all `domain/*` directories in one mechanical commit:

```sh
mkdir -p internal/domain
git mv domain/identity internal/domain/identity
git mv domain/space internal/domain/space
git mv domain/graph internal/domain/graph
git mv domain/access internal/domain/access
git mv domain/auth internal/domain/auth
git mv domain/semantic internal/domain/semantic
rg 'github.com/myceldb/mycel/domain/' -l --glob '*.go' \
  | xargs perl -pi -e 's#github.com/myceldb/mycel/domain/#github.com/myceldb/mycel/internal/domain/#g'
```

### Tasks

- Update all internal imports.
- Keep package names (`graph`, `semantic`, etc.) unchanged.
- Do not change JSON tags or field names.
- Re-run API adapter tests carefully; these adapters define the proto/internal translation boundary.
- Update docs referencing old implementation import paths.

### Acceptance

```sh
cd myceldb/mycel
go list ./... | rg '^github.com/myceldb/mycel/domain(/|$)' && exit 1 || true
rg 'github\.com/myceldb/mycel/domain/' --glob '*.go' && exit 1 || true
go test ./...
git diff --check
```

### Risks

- High churn across daemon API adapters, semantic maintenance, graph/session, and stores.
- Any sibling module still importing `domain/*` will fail immediately if it updates to this Mycel version.
- Docs/examples may need updates even when code compiles.

## Phase 4: Remove or neutralize obsolete top-level packages

### Goal

Ensure `mycel` does not present itself as an embeddable library.

### Tasks

- Decide whether root `doc.go` stays as daemon-only documentation or is removed.
- Confirm no `engine` or `session` public package remains in current branch. If present in a future branch, move/delete them.
- Confirm no public constructors expose daemon stores/sessions.
- Update README with explicit public-contract statement:

```text
Applications should use mycel-go-sdk and mycel-api. The mycel module contains daemon binaries and internal implementation packages only.
```

### Acceptance

```sh
go list ./... | rg '^github.com/myceldb/mycel/(domain|store|query|engine|session)(/|$)' && exit 1 || true
go test ./...
```

## Phase 5: Enforce guardrails in CI/scripts

### Goal

Prevent regression to public implementation imports.

### Tasks

- Add a Mycel script, for example:

```text
scripts/check-public-surface.sh
```

Checks:

1. No `domain`, `store`, `query`, `engine`, or `session` package exists outside `internal/`.
2. No sibling production repo imports `github.com/myceldb/mycel/(domain|store|query|engine|session)`.
3. Optional: root module only exposes approved packages.

- Wire script into Makefile/CI.
- Add documentation in README and `docs/v2/design/daemon-only-boundary.md`.

### Acceptance

```sh
./scripts/check-public-surface.sh /Users/martinbeauvais/Projects/knotbase/Knotbase
go test ./...
```

## Suggested commit breakdown

1. `Migrate PKM importer to Mycel daemon SDK`
2. `Add public surface guardrail script`
3. `Move query package under internal`
4. `Move file store packages under internal`
5. `Internalize domain packages`
6. `Document daemon-only Go package boundary`

Keep Phase 3 as a dedicated commit because it will be mostly mechanical and high-churn.

## Final done criteria

- Workspace search has no external imports of Mycel implementation packages:

```sh
cd /Users/martinbeauvais/Projects/knotbase/Knotbase
rg 'github\.com/myceldb/mycel(/(domain|store|query|engine|session|$)|")' \
  --glob '!*myceldb/mycel/**' \
  --glob '!**/go.sum' \
  --glob '!**/vendor/**'
```

- Mycel package list exposes no implementation packages:

```sh
cd myceldb/mycel
go list ./... | rg '^github.com/myceldb/mycel/(domain|store|query|engine|session)(/|$)'
```

- All tests pass:

```sh
cd myceldb/mycel
go test ./...

cd ../mycel-go-sdk
go test ./...

cd ../../knot_pkm/knot_pkm_importer
go test ./...
```

- Public docs state that `mycel-api` + `mycel-go-sdk` are the supported application-facing contracts.
