# Client Template API

## Status

Implemented daemon-oriented Client Template API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/template.proto
```

This document depends on:

```text
docs/v2/design/access-control.md
docs/v2/design/api/space.md
docs/v2/design/api/graph.md
```

## Purpose

`TemplateService` is the client-facing API for managing graph templates scoped to a space.

Templates define reusable, versioned node shapes. Nodes reference templates by `template_id`. Templates provide schema-like contracts for node props and direct child relationships.

## Scope

`TemplateService` includes:

- list templates
- get template by id
- find template by key/version
- create template
- update template display metadata
- archive template
- delete template subject to space template usage policy and reference rules
- import multiple templates

`TemplateService` does not include:

- graph node mutation
- graph query
- blob upload/download
- semantic index configuration
- space creation

Template references on nodes are mutated through `GraphService`.

## Space scope

Templates are space-scoped, not domain-scoped.

A node in any domain of a space may reference a template from that space.

Every request includes:

```text
space_id
```

## Template usage policy

Template usage is a space-level policy selected when the space is created.

Space creation is an Admin API operation, but Client `SpaceService` exposes the selected policy as space metadata.

Supported policies:

```text
optional
mandatory
```

### Optional template usage

Nodes may omit `template_id`.

When deleting a template, the caller may explicitly request detach behavior:

```text
TEMPLATE_DELETE_MODE_DETACH_REFERENCES
```

This clears `template_id` from active nodes that reference the deleted template.

Detach behavior must be explicit. Template deletion should not silently clear references.

### Mandatory template usage

Nodes must have `template_id`.

Template deletion is blocked while active nodes reference the template. Referencing objects must first be migrated to another template or deleted/archived.

If archived nodes reference a template, the template must be archived rather than hard-deleted so archived data remains readable/interpretable.

## Service definition

```protobuf
service TemplateService {
  rpc ListTemplates(ListTemplatesRequest) returns (ListTemplatesResponse);
  rpc GetTemplate(GetTemplateRequest) returns (GetTemplateResponse);
  rpc FindTemplate(FindTemplateRequest) returns (FindTemplateResponse);
  rpc CreateTemplate(CreateTemplateRequest) returns (CreateTemplateResponse);
  rpc UpdateTemplate(UpdateTemplateRequest) returns (UpdateTemplateResponse);
  rpc ArchiveTemplate(ArchiveTemplateRequest) returns (ArchiveTemplateResponse);
  rpc DeleteTemplate(DeleteTemplateRequest) returns (DeleteTemplateResponse);
  rpc ImportTemplates(ImportTemplatesRequest) returns (ImportTemplatesResponse);
}
```

## Template identity

Templates have:

- stable `template_id`
- `space_id`
- `key`
- `version`

The pair `key + version` must be unique within a space.

`FindTemplate` exists because applications often refer to templates by key/version during setup/import flows.

## Template model

The client template resource mirrors the current Mycel domain model:

```text
template_id
space_id
key
version
display_name
description
system
state
property policy
child policy
```

### Property policy

A property policy defines:

- whether extra properties are allowed
- allowed properties
- forbidden property names

Property types:

- string
- number
- bool
- object
- array
- date

### Child policy

A child policy defines:

- whether direct child nodes are allowed
- allowed child template refs
- optional child order policy

Child ordering can be represented on `contains` edge props, consistent with existing Mycel behavior.

## Mutability and versioning

Template key, version, and policies should be treated as immutable after creation.

Policy changes should create a new template version.

`UpdateTemplate` is limited to display metadata such as:

- display name
- description

This avoids silently changing validation semantics for existing nodes.

## ArchiveTemplate

Archiving marks a template as archived while preserving it for existing references.

Archive is appropriate when:

- archived nodes still reference the template
- the template should no longer be used for new nodes
- hard deletion would break historical interpretation

Archived templates should be hidden from normal list results unless `include_archived` is true.

## DeleteTemplate

Deleting a template is destructive and subject to reference rules.

Supported delete modes:

### REQUIRE_UNUSED

Delete only if no active or archived nodes reference the template.

### DETACH_REFERENCES

Allowed only when the space template usage policy is optional.

Effects:

- clear `template_id` from active nodes that reference the template
- delete the template
- return detached node ids
- audit the operation

Hard delete is still rejected if archived nodes reference the template. In that case the template should be archived instead.

## ImportTemplates

Imports multiple template definitions into a space.

This supports product setup and import flows such as PKM/Logseq initialization.

`ImportTemplates` requires `template.manage`.

## Authorization

Suggested capability mapping:

| Operation | Required capability |
| --- | --- |
| List/Get/Find templates | `template.read` |
| Create/Update/Archive/Delete/Import templates | `template.manage` |

System templates require admin/system visibility for management.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| missing capability | `PERMISSION_DENIED` |
| malformed request | `INVALID_ARGUMENT` |
| duplicate key/version | `ALREADY_EXISTS` |
| template not found | `NOT_FOUND` |
| policy update attempted | `FAILED_PRECONDITION` |
| delete blocked by mandatory usage | `FAILED_PRECONDITION` |
| delete blocked by archived node references | `FAILED_PRECONDITION` |
| detach requested in mandatory space | `FAILED_PRECONDITION` |
| service unavailable | `UNAVAILABLE` |

## Mesh implications

Templates are durable space metadata and must replicate with the space across the mesh.

Template archive/delete operations must replicate with enough ordering to preserve node/template interpretation on all replicas.

## CLI

The CLI now uses daemon gRPC and standard-user credentials for template commands:

```sh
./bin/mycel -u alice -p '<password>' template list --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template import --file templates.json --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template create note --version 1.0.0 --display-name Note --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template find note --version 1.0.0 --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template get '<template-id>' --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template update '<template-id>' --display-name 'Note v1' --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template archive '<template-id>' --space-id '<space-id>'
./bin/mycel -u alice -p '<password>' template delete '<template-id>' --space-id '<space-id>'
```

## Current implementation notes

- `TemplateService` is registered on the daemon Client API and uses user bearer tokens from Client `AuthService`.
- Templates are stored under `<MYCELD_DATA_DIR>/templates/<space-id>.json`.
- List/get/find require readable space access; create/update/archive/delete/import require effective space admin access.
- Archive marks template state as archived.
- Delete currently deletes template metadata. Reference-aware delete/detach will be hardened when daemon graph/session services are migrated.

## Open questions

- Should `ArchiveTemplate` support an optional replacement template id for migration guidance?
- Should `DeleteTemplate` expose a dry-run/preview mode to count references before deleting?
- Should template import be atomic across all templates in a request?
- Should archived templates be allowed in new node creation if explicitly requested by advanced clients?
