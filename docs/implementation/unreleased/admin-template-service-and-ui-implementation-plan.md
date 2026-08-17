# AdminTemplateService and Mycel Console template UI implementation plan

## Goal

Add an operator/admin-facing template API and wire it into `mycel-console` so operators can view templates for a given space without using user-scoped client sessions.

Initial scope is read-only:

- list templates for a space
- view template details
- populate the `SpaceDetailPage` Templates tab

Management operations can be added later once the read model is stable.

## Principles

- Use a dedicated `AdminTemplateService` in `mycel.admin.v1`.
- Keep the API space-scoped.
- Do not use user/client sessions in `mycel-console` for operator workflows.
- Do not introduce template mutation APIs in the first phase.
- Preserve room for future WAL-first admin template mutations.
- In clustered mode, read-only template inspection may be allowed on primary or follower as long as local replica state is consistent.

## Phase 1: API design in `mycel-api`

### Files

- `mycel-api/api/proto/mycel/admin/v1/template.proto`
- update any admin package/proto build config if needed

### Proposed proto

```proto
syntax = "proto3";

package mycel.admin.v1;

service AdminTemplateService {
  rpc ListTemplates(ListTemplatesRequest) returns (ListTemplatesResponse);
  rpc GetTemplate(GetTemplateRequest) returns (GetTemplateResponse);
}

message ListTemplatesRequest {
  string space_id = 1;
  int32 page_size = 2;
  string page_token = 3;
  bool include_archived = 4;
}

message ListTemplatesResponse {
  repeated Template templates = 1;
  string next_page_token = 2;
}

message GetTemplateRequest {
  string space_id = 1;
  string template_id = 2;
}

message GetTemplateResponse {
  Template template = 1;
}

message Template {
  string template_id = 1;
  string space_id = 2;
  string name = 3;
  string key = 4;
  string description = 5;
  TemplateState state = 6;
  repeated TemplateField fields = 7;
  string create_time = 8;
  string update_time = 9;
}

enum TemplateState {
  TEMPLATE_STATE_UNSPECIFIED = 0;
  TEMPLATE_STATE_ACTIVE = 1;
  TEMPLATE_STATE_ARCHIVED = 2;
}

message TemplateField {
  string field_id = 1;
  string key = 2;
  string name = 3;
  string description = 4;
  string value_type = 5;
  bool required = 6;
  bool repeated = 7;
}
```

### Compatibility note

Before finalizing `Template` / `TemplateField`, compare with the existing client template proto/domain model. Prefer reusing equivalent names and fields where possible, but avoid exposing unstable internal-only fields.

If the existing domain template model is more flexible than typed fields, use one of these alternatives:

- `string schema_json`
- `bytes schema_json`
- `repeated TemplateProperty properties`

The chosen API should be stable enough for `mycel-console` tables/detail views.

### Acceptance

- `buf lint` passes in `mycel-api`.
- Generated Go and Rust protobuf bindings include `AdminTemplateService`.

## Phase 2: Generate protobuf bindings

### Tasks

1. Regenerate daemon Go stubs in `mycel`.
2. Regenerate Go SDK stubs in `mycel-go-sdk`.
3. Ensure Rust proto crate rebuilds against the new proto.
4. Confirm `AdminTemplateService` appears in generated code.

### Validation

```bash
cd mycel-api && go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
cd ../mycel && ./scripts/generate-proto.sh
cd ../mycel-go-sdk && ./scripts/generate-proto.sh && go test ./...
cd ../mycel-rust-sdk && cargo check -p mycel-proto && cargo check -p mycel-sdk
```

## Phase 3: Daemon admin template service

### Files

- new: `mycel/internal/daemon/api/admin/template_service.go`
- tests: `mycel/internal/daemon/api/admin/template_service_test.go`
- server registration: `mycel/internal/daemon/server/server.go`
- runtime/app wiring if needed: `mycel/internal/daemon/app/app.go`

### Service shape

Implement generated `AdminTemplateServiceServer` directly in daemon, consistent with other admin services.

Dependencies likely needed:

- template storage/manager from space module
- operator auth/capability check helpers
- maybe space read/admin capability checks

### Authorization

Read-only endpoints should require an authenticated operator session and an admin capability. Suggested initial policy:

- `ListTemplates`: system admin or operator with space read/admin capability
- `GetTemplate`: same as list

If capability model does not yet have a template-specific admin capability, reuse the same admin-space-read capability used by existing AdminSpaceService list/get operations.

### Behavior

`ListTemplates`:

- validate `space_id`
- normalize `page_size`
- pass `include_archived`
- return sorted/paginated templates
- include `next_page_token` if applicable

`GetTemplate`:

- validate `space_id` and `template_id`
- return `NotFound` if missing or archived and not allowed by underlying behavior
- return admin template model

### Mapping

Add conversion from internal/client template domain model to `mycel.admin.v1.Template`:

- ID
- space ID
- name/key/description
- active/archived state
- fields/properties/schema
- create/update timestamps

### Acceptance

- admin template service compiles
- protected by existing auth interceptor
- unit tests cover:
  - unauthenticated call rejected at server/interceptor level if existing pattern supports it
  - list empty
  - list active templates
  - include archived behavior
  - get existing
  - get missing
  - invalid arguments

## Phase 4: CLI optional smoke command

This is optional but useful for validation. Either add CLI commands or skip if `mycel-console` is the only consumer.

Possible commands:

```bash
mycel admin template list --space-id SPACE_ID
mycel admin template get --space-id SPACE_ID TEMPLATE_ID
```

or under existing resource hierarchy:

```bash
mycel template list --space-id SPACE_ID
mycel template get --space-id SPACE_ID TEMPLATE_ID
```

If implemented, commands should use operator login and AdminTemplateService, not user client TemplateService.

## Phase 5: mycel-console Tauri command layer

### Files

- `mycel-console/src-tauri/src/commands/templates.rs` or add to an existing command module
- `mycel-console/src-tauri/src/commands/mod.rs`
- `mycel-console/src-tauri/src/lib.rs`

### Commands

```rust
admin_list_templates(input: ListTemplatesInput) -> ListTemplatesResponse
admin_get_template(input: GetTemplateInput) -> TemplateInfo
```

### Rust DTOs

Mirror frontend camelCase shape:

```rust
struct ListTemplatesInput {
  space_id: String,
  page_size: Option<i32>,
  page_token: Option<String>,
  include_archived: Option<bool>,
}

struct TemplateInfo {
  template_id: String,
  space_id: String,
  name: String,
  key: Option<String>,
  description: Option<String>,
  state: String,
  fields: Vec<TemplateFieldInfo>,
  create_time: Option<String>,
  update_time: Option<String>,
}
```

### Acceptance

- Tauri command compiles.
- Invalid input produces friendly error.
- Generated client call uses authenticated admin session.

## Phase 6: Frontend service/types

### Files

- new: `mycel-console/src/types/templates.ts`
- update: `mycel-console/src/services/adminService.ts`

### Types

```ts
export type TemplateInfo = {
  templateId: string;
  spaceId: string;
  name: string;
  key?: string;
  description?: string;
  state: string;
  fields: TemplateFieldInfo[];
  createTime?: string;
  updateTime?: string;
};

export type TemplateFieldInfo = {
  fieldId?: string;
  key: string;
  name?: string;
  description?: string;
  valueType?: string;
  required?: boolean;
  repeated?: boolean;
};

export type ListTemplatesInput = {
  spaceId: string;
  pageSize?: number;
  pageToken?: string;
  includeArchived?: boolean;
};

export type ListTemplatesResponse = {
  templates: TemplateInfo[];
  nextPageToken?: string;
};
```

### Service functions

```ts
export async function listTemplates(input: ListTemplatesInput): Promise<ListTemplatesResponse>;
export async function getTemplate(input: GetTemplateInput): Promise<TemplateInfo>;
```

### Tests

Update `src/services/adminService.test.ts` for invoke names and payload shape.

## Phase 7: Space detail Templates tab UI

### Files

- `mycel-console/src/features/spaces/pages/SpaceDetailPage.tsx`
- optional new component:
  - `mycel-console/src/features/spaces/components/TemplateTable.tsx`
  - `mycel-console/src/features/spaces/components/TemplateDetailDrawer.tsx`

### Behavior

Replace current placeholder Templates tab with:

- loading state
- error state
- empty state
- include archived checkbox
- table:
  - state
  - name
  - key
  - template ID
  - field count
  - updated time
  - action: view details
- pagination/load more if `next_page_token` is present

Template detail view:

- template identity
- state
- description
- timestamps
- fields/properties table

### Data loading

For first implementation, eager loading is acceptable because `SpaceDetailPage` already eagerly loads domains and semantic data. Better follow-up:

- lazy-load templates only when Templates tab is selected
- refresh templates when include-archived changes

Recommended implementation:

- add `activeTab`-aware lazy loading for templates only, to avoid unnecessary calls if operators never open the tab.

### Acceptance

- Templates tab lists templates for current space.
- Include archived toggle refetches with `includeArchived=true`.
- Empty state replaces old placeholder.
- Detail action displays fields/properties.
- Tests cover loading, empty, populated, archived toggle, detail view.

## Phase 8: Validation

Run:

```bash
cd mycel-api && go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint
cd ../mycel && ./scripts/generate-proto.sh && go test ./internal/...
cd ../mycel-go-sdk && ./scripts/generate-proto.sh && go test ./...
cd ../mycel-rust-sdk && cargo check -p mycel-proto && cargo check -p mycel-sdk
cd ../mycel-console/src-tauri && cargo check
cd .. && npm test -- --runInBand
cd .. && npm run build
```

## Future phase: template management

Once read-only UI is stable, add mutation APIs:

```proto
rpc CreateTemplate(CreateTemplateRequest) returns (Template);
rpc UpdateTemplate(UpdateTemplateRequest) returns (Template);
rpc ArchiveTemplate(ArchiveTemplateRequest) returns (Template);
rpc DeleteTemplate(DeleteTemplateRequest) returns (Template);
```

Requirements for future mutations:

- WAL-first template mutation path
- primary-only write guardrails in clustered mode
- structured not-primary hints
- optimistic concurrency/update masks if needed
- audit/history if admin mutations become sensitive
