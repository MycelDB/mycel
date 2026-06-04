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

For an initialized store, `meta/users.json`, `meta/spaces.json`, and `meta/access.json` are required. If any of these files is missing while another exists, startup fails instead of recreating empty metadata and risking silent data loss.

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

Contains system-role and per-space allow/grant rules for users.

Access control should not be embedded only in users or spaces. Keeping it separate allows support for system superusers, user admins, operators, shared spaces, read-only grants, and space admin rights. The system must retain at least one superuser, and every space must retain at least one admin rule.

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

## Hard-delete behavior

Delete operations physically remove persisted records and associated files:

- deleting a user removes the user record, access rules for that user, and all spaces owned by the user
- deleting a space removes its metadata, access rules, templates, and graph directory
- deleting a node removes the node and incident edges; descendant nodes require recursive deletion

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
<data-root>/meta/users.json
<data-root>/graphs/<space_id>/nodes.json
```

