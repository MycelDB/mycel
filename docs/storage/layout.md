# Storage Layout

KnotDB stores data under a single data root. The root is split into metadata and graph data so the same logical layout can be used by local filesystem storage or object storage backends such as SeaweedFS.

```text
<data-root>/
  meta/
    users.json
    spaces.json
    access.json
    system.json
    templates/
      <space_id>.json

  graphs/
    <space_id>/
      nodes.json
      edges.json
```

## Top-level directories

### `meta/`

`meta/` contains system metadata required to authenticate users, define spaces, enforce access, and validate graph data.

Metadata should remain logically separate from graph payload data even when both are stored in the same physical backend.

### `graphs/`

`graphs/` contains the actual graph payload for each space.

Each space gets its own graph directory keyed by `SpaceID`.

## Metadata files

### `meta/users.json`

Authoritative user identity and credential store.

Contains:

- internal user ID
- external user ref
- optional email/username
- user status
- credential material or credential references

### `meta/spaces.json`

Authoritative space definition store.

Contains:

- space ID
- owner user ID
- space name
- space status
- space settings

### `meta/access.json`

Access-control metadata.

Reserved for roles, grants, memberships, and policy records.

Access control should not be embedded only in users or spaces. Keeping it separate allows future support for shared spaces, read-only grants, groups, and admin roles.

### `meta/templates/<space_id>.json`

Per-space node template definitions.

Contains immutable template versions for a space, including:

- template ID
- key
- semver version
- property policy
- direct child-node policy

### `meta/system.json`

System/storage metadata.

Reserved for:

- storage schema version
- created timestamp
- engine version
- migration state
- backend metadata

## Graph files

### `graphs/<space_id>/nodes.json`

Persisted nodes for a space.

### `graphs/<space_id>/edges.json`

Persisted edges for a space.

## ID format

IDs are UUID strings unless otherwise noted.

Current key IDs:

- `UserID`
- `SpaceID`
- `TemplateID`
- `NodeID`
- `EdgeID`

## Backend mapping

For local filesystem storage, these paths are directories and files under the data root.

For object storage, the same paths should be treated as object keys under a configured prefix, for example:

```text
knotdb/meta/users.json
knotdb/graphs/<space_id>/nodes.json
```

## Current implementation note

The current code may still use transitional root-level files while storage components are being refactored. New storage work should target the layout described in this document.
