# Documentation Information Architecture Reorganization Plan

## Goal

Reorganize `docs/` so mycel documentation is easier to navigate by audience and intent:

- `docs/README.md` becomes a concise table of contents and feature map.
- `docs/design/` explains the current system design from the top down.
- `docs/operations/` explains how to run, operate, validate, and recover mycel.
- `docs/implementation/` becomes an archival, release-grouped record of implementation plans.

The reorganization should improve discoverability without losing historical implementation context or breaking common links unnecessarily.

## Non-goals

- Do not rewrite every design document in one pass.
- Do not delete historical implementation plans.
- Do not move or rename source code packages.
- Do not change product behavior, CLI behavior, or release artifacts.
- Do not treat implementation plans as current operator guidance; operator-facing instructions belong under `docs/operations/`.

## Proposed target tree

```text
docs/
  README.md
  design/
    README.md
    system-overview.md
    api/
      README.md
      *.md
    admin-api/
      README.md
      *.md
    identity/
      README.md
      *.md
    spaces-domains/
      README.md
      *.md
    graph/
      README.md
      *.md
    blobs/
      README.md
      *.md
    schema/
      README.md
      *.md
    semantic/
      README.md
      *.md
    automation/
      README.md
      *.md
    clustering/
      README.md
      *.md
    backup-restore/
      README.md
      *.md
    runtime/
      README.md
      *.md
  operations/
    README.md
    cli/
      README.md
      admin.md
      auth.md
      automation.md
      blob.md
      change-stream.md
      cluster.md
      domain.md
      export.md
      graph.md
      import.md
      inference.md
      metadata.md
      query.md
      schema.md
      semantic.md
      session.md
      space.md
      transaction.md
      user.md
    procedures/
      README.md
      backup-restore.md
      split-brain-recovery.md
      raft-cluster-operations.md
      raft-cluster-manual-repair-workflows.md
      raft-cluster-test-matrix.md
      compose-cluster-validation.md
      k3s-cluster-validation.md
  implementation/
    README.md
    unreleased/
    v0.1/
    v0.2/
    v0.3/
    v0.4/
    v0.5/
    v0.6/
```

Notes:

- Use `operations`, not `operation`, because the repo already uses `docs/operations/` and it is the common term for runbooks.
- `design/system-overview.md` should be the canonical high-level system document.
- `implementation/README.md` should explicitly say that implementation plans are historical or planning artifacts, not operator instructions.
- Product-facing roadmap/status pages should live in the public manual Roadmap appendix; repository `docs/roadmap/` files should be removed or kept only as compatibility pointers.

## Current docs inventory snapshot

Current root-level docs include:

- `docs/README.md`
- `docs/gql-schema-behavior.md`
- `docs/graph-automations.md`
- `docs/makefile_commands.md`
- `docs/schema-subsystem.md`
- `docs/design/**`
- `docs/operations/**`
- `docs/implementation/**`
- `docs/roadmap/**` compatibility pointers, if any

Important existing groupings:

- `docs/design/api/` already contains Client API documents.
- `docs/design/admin/` already contains Admin API documents.
- `docs/operations/` already contains Raft cluster operation/runbook material.
- `docs/implementation/` currently contains all implementation plans in one flat directory.

## Proposed component mapping

| Component area | Target design location | Existing docs to consider |
| --- | --- | --- |
| API contracts | `docs/design/api/` | `docs/design/api/*.md` |
| Admin API | `docs/design/admin-api/` or keep `docs/design/admin/` | `docs/design/admin/*.md`, `grpc-admin-auth.md`, `grpc-admin-list.md` |
| Identity/access | `docs/design/identity/` | `access-control.md`, `grpc-client-auth.md`, `grpc-admin-auth.md`, `design/admin/user.md`, `design/admin/operator.md` |
| Spaces/domains | `docs/design/spaces-domains/` | `design/api/space.md`, `design/api/domain.md`, `design/admin/domain.md` |
| Graph/query/GQL | `docs/design/graph/` | `design/api/graph.md`, `design/api/query.md`, `gql-schema-behavior.md`, GQL plans |
| Blob subsystem | `docs/design/blobs/` | `design/api/blob.md`, blob-related raft/snapshot implementation notes |
| Schema subsystem | `docs/design/schema/` | `schema-subsystem.md`, `design/schema-subsystem.md`, `schema-management.md` |
| Semantic/inference | `docs/design/semantic/` | `design/api/semantic.md`, `design/admin/semantic*.md`, `embedding-package.md` |
| Automation | `docs/design/automation/` | `graph-automations.md`, `design/graph-automations.md` |
| Clustering/raft | `docs/design/clustering/` | `clustering-replication-reliability.md`, `space-partitioned-raft-clustering.md`, authoritative raft metadata docs |
| Backup/restore | `docs/design/backup-restore/` | `quiesce-and-backup.md`, user-scoped backup/restore plan, backup admin design |
| Runtime/subsystems | `docs/design/runtime/` | `subsystem-runtime-architecture.md`, `subsystem-runtime-package-map.md`, daemon service interface docs |

## Phased implementation

### Phase 0 — Inventory and link audit

Status: complete. See `docs/implementation/docs-reorganization-inventory.md` for the file inventory summary, broken-link audit, duplicate/overlap notes, and migration map.

1. Generate a complete file list:
   ```sh
   find docs -type f | sort
   ```
2. Search for internal docs links:
   ```sh
   rg -n "\]\([^)]*\.md" docs
   ```
3. Identify duplicate/current-vs-historical docs, especially:
   - schema docs
   - graph automation docs
   - raft clustering docs
   - backup/restore docs
4. Produce a temporary migration map listing:
   - source path
   - target path
   - current/historical status
   - whether the file should be moved, copied, rewritten, or replaced by a redirect stub

Deliverable:

- `docs/implementation/docs-reorganization-inventory.md` or a checked-off appendix in this plan.

### Phase 1 — Add navigation scaffolding without moving files

Status: complete.

Create or rewrite the master index documents first:

1. `docs/README.md`
   - brief mycel feature overview
   - audience guide
   - links to `design/`, `operations/`, `implementation/`, `roadmap/`
   - quick links for common tasks
2. `docs/design/README.md`
   - link to `system-overview.md`
   - list major components and component subdirectories
   - explain current-state vs historical docs
3. `docs/design/system-overview.md`
   - high-level architecture
   - major subsystems/components
   - relationships among daemon, APIs, identity, spaces/domains, graph/blob/schema/semantic, clustering, backup/restore
4. `docs/operations/README.md`
   - explain operational docs scope
   - link to CLI and procedures docs
5. `docs/operations/cli/README.md`
   - list top-level CLI commands
   - describe doc generation/update expectations
6. `docs/operations/procedures/README.md`
   - list operator procedures and runbooks
7. `docs/implementation/README.md`
   - explain release-grouped archive
   - warn that implementation plans are not current operator runbooks

Validation:

```sh
find docs -type f | sort >/tmp/mycel-docs-files.txt
rg -n "TODO|FIXME|BROKEN-LINK" docs/README.md docs/design/README.md docs/operations/README.md docs/implementation/README.md
```

### Phase 2 — Create component and operations subdirectories

Status: complete.

Create the target subdirectories with `README.md` files, but do not move most content yet.

Design directories:

```text
docs/design/api/
docs/design/admin-api/       # or keep existing docs/design/admin/ and document that choice
docs/design/identity/
docs/design/spaces-domains/
docs/design/graph/
docs/design/blobs/
docs/design/schema/
docs/design/semantic/
docs/design/automation/
docs/design/clustering/
docs/design/backup-restore/
docs/design/runtime/
```

Operations directories:

```text
docs/operations/cli/
docs/operations/procedures/
```

Each directory README should include:

- scope
- current docs linked from old locations
- planned migration notes

Validation:

```sh
find docs/design docs/operations -maxdepth 2 -name README.md | sort
```

### Phase 3 — Move operator-facing docs first

Status: complete.

Move or copy operational material into `docs/operations/procedures/`:

| Current path | Target path |
| --- | --- |
| `docs/operations/raft-cluster-operations.md` | `docs/operations/procedures/raft-cluster-operations.md` |
| `docs/operations/raft-cluster-manual-repair-workflows.md` | `docs/operations/procedures/raft-cluster-manual-repair-workflows.md` |
| `docs/operations/raft-cluster-test-matrix.md` | `docs/operations/procedures/raft-cluster-test-matrix.md` |

Add new procedure docs as needed:

- `docs/operations/procedures/backup-restore.md`
- `docs/operations/procedures/split-brain-recovery.md`
- `docs/operations/procedures/compose-cluster-validation.md`
- `docs/operations/procedures/k3s-cluster-validation.md`

For moved files, either:

- update all links immediately, or
- leave short compatibility stubs at old paths that point to the new locations.

Prefer stubs for heavily referenced docs during the first reorganization PR.

Validation:

```sh
rg -n "operations/raft-cluster" docs README.md || true
```

### Phase 4 — Add CLI command documentation

Status: complete for current top-level commands.

Create one doc per top-level CLI command under `docs/operations/cli/`.

Initial command list from `NewRootCommand`:

- `admin`
- `auth`
- `automation`
- `blob`
- `change-stream`
- `cluster`
- `domain`
- `export`
- `graph`
- `import`
- `inference`
- `metadata`
- `node`
- `query`
- `schema`
- `semantic`
- `session`
- `space`
- `transaction`
- `user`

Each command file should contain:

- purpose
- common examples
- authentication mode: user vs operator
- important flags
- related procedures/design docs

For `admin`, include subsections for:

- operators
- users
- spaces/domains
- backups
- `admin user-backup` export/validate/import

Add a future follow-up item to generate CLI docs from Cobra help to avoid drift.

Validation:

```sh
for cmd in admin auth automation blob change-stream cluster domain export graph import inference metadata node query schema semantic session space transaction user; do
  test -f "docs/operations/cli/${cmd}.md" || echo "missing ${cmd}.md"
done
```

### Phase 5 — Move current design docs by component

Status: complete for the current move map.

Move current-state design docs into component directories. Keep compatibility stubs for old paths with high likelihood of external references.

Example target moves:

| Current path | Target path |
| --- | --- |
| `docs/design/clustering-replication-reliability.md` | `docs/design/clustering/clustering-replication-reliability.md` |
| `docs/design/authoritative-system-raft-cluster-metadata.md` | `docs/design/clustering/authoritative-system-raft-cluster-metadata.md` |
| `docs/design/space-partitioned-raft-clustering.md` | `docs/design/clustering/space-partitioned-raft-clustering.md` |
| `docs/design/subsystem-runtime-architecture.md` | `docs/design/runtime/subsystem-runtime-architecture.md` |
| `docs/design/subsystem-runtime-package-map.md` | `docs/design/runtime/subsystem-runtime-package-map.md` |
| `docs/design/schema-subsystem.md` | `docs/design/schema/schema-subsystem.md` |
| `docs/schema-subsystem.md` | `docs/design/schema/schema-subsystem-overview.md` or operations-oriented schema guide |
| `docs/gql-schema-behavior.md` | `docs/design/schema/gql-schema-behavior.md` or `docs/design/graph/gql-schema-behavior.md` |
| `docs/design/graph-automations.md` | `docs/design/automation/graph-automations.md` |
| `docs/graph-automations.md` | compatibility stub or operations-oriented automation guide |
| `docs/design/quiesce-and-backup.md` | `docs/design/backup-restore/quiesce-and-backup.md` |

Decision point:

- Existing `docs/design/api/` can stay as the API component directory.
- Existing `docs/design/admin/` can either stay as-is or be renamed to `admin-api/`. Keeping it avoids churn; if renamed, use compatibility stubs.

Validation:

```sh
rg -n "docs/design/(clustering-replication|authoritative-system|space-partitioned|schema-subsystem|graph-automations|quiesce-and-backup)" . || true
```

### Phase 6 — Group implementation plans by release

Status: complete for v0.5, v0.6, and unreleased buckets.

Create release directories and move plans based on when the work landed or the release they supported.

Suggested initial classification:

#### `docs/implementation/v0.5/`

- raft reliability phases A-G and inventories
- subsystem snapshot recovery plans/inventory
- raft graph write routing related plans if present
- clustering test matrix planning if implementation-focused

#### `docs/implementation/v0.6/`

- `user-scoped-backup-restore-implementation-plan.md`
- `docs-information-architecture-reorganization-plan.md` once this work lands

#### `docs/implementation/unreleased/`

- plans not clearly shipped yet
- future cleanup plans
- partially completed migration plans

Keep `docs/implementation/README.md` as the release index.

Use `git mv` for moves to preserve history.

Validation:

```sh
find docs/implementation -maxdepth 2 -type f | sort
rg -n "implementation/[^/]+\.md" docs README.md || true
```

### Phase 7 — Link repair and compatibility cleanup

Status: complete for simple relative Markdown links.

1. Update relative links in moved files.
2. Add stubs only for old paths that are likely externally referenced.
3. For internal-only implementation plans, prefer direct moves and link updates.
4. Run a lightweight Markdown link check if available. If not, use grep-based checks:

```sh
python3 - <<'PY'
from pathlib import Path
import re
root = Path('docs')
missing = []
for path in root.rglob('*.md'):
    text = path.read_text()
    for match in re.finditer(r'\]\(([^)#][^)]*\.md)(?:#[^)]*)?\)', text):
        target = match.group(1)
        if '://' in target:
            continue
        resolved = (path.parent / target).resolve()
        if not resolved.exists():
            missing.append((str(path), target))
if missing:
    for src, target in missing:
        print(f'{src}: missing {target}')
    raise SystemExit(1)
PY
```

### Phase 8 — CI/doc hygiene follow-ups

Status: partially complete: `make docs-check` and `scripts/checkDocs.py` were added; generated CLI Markdown and CI wiring remain follow-ups.

Optional follow-ups after the first reorganization lands:

- Add a `make docs-check` target.
- Add CLI doc presence check for top-level Cobra commands.
- Generate CLI Markdown from Cobra help output.
- Add Markdown link check in CI.
- Add frontmatter or status banners:
  - `current`
  - `historical`
  - `draft`
  - `operator-runbook`
  - `implementation-plan`

## Acceptance criteria

- `docs/README.md` is a clear entrypoint and table of contents.
- `docs/design/README.md` links to a high-level system overview and component docs.
- `docs/design/system-overview.md` explains major mycel components and relationships.
- `docs/operations/README.md` explains operations scope and links to CLI/procedure indexes.
- `docs/operations/cli/README.md` lists top-level CLI commands.
- `docs/operations/procedures/README.md` lists operator procedures.
- `docs/implementation/README.md` indexes release-grouped implementation plans.
- Implementation plans are grouped under release subdirectories or `unreleased/`.
- Existing important operator docs remain findable through either updated links or compatibility stubs.
- No source code behavior changes are required.

## Validation checklist

Before committing:

```sh
# Basic file inventory
find docs -type f | sort

# No broken simple relative Markdown links
python3 - <<'PY'
from pathlib import Path
import re
root = Path('docs')
missing = []
for path in root.rglob('*.md'):
    text = path.read_text()
    for match in re.finditer(r'\]\(([^)#][^)]*\.md)(?:#[^)]*)?\)', text):
        target = match.group(1)
        if '://' in target:
            continue
        resolved = (path.parent / target).resolve()
        if not resolved.exists():
            missing.append((str(path), target))
if missing:
    for src, target in missing:
        print(f'{src}: missing {target}')
    raise SystemExit(1)
PY

# Optional if docs-check target is added
make docs-check
```

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Broken internal links | Run link check before commit; keep stubs for high-value old paths. |
| Losing historical context | Use `git mv`; preserve all implementation plans. |
| Confusing current design with old plans | Add status language in `implementation/README.md` and component indexes. |
| CLI docs drift | Add generated docs or a doc presence check in a follow-up. |
| Huge noisy PR | Stage as scaffolding first, then targeted moves by area/release. |

## Recommended first PR scope

Keep the first docs cleanup PR small enough to review:

1. Add/update index docs:
   - `docs/README.md`
   - `docs/design/README.md`
   - `docs/design/system-overview.md`
   - `docs/operations/README.md`
   - `docs/operations/cli/README.md`
   - `docs/operations/procedures/README.md`
   - `docs/implementation/README.md`
2. Add component directory README files.
3. Move only the three existing operations raft docs into `operations/procedures/` with compatibility stubs.
4. Leave implementation plan release grouping for the second PR unless the team wants a single larger docs-only reorg.
