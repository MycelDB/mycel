# Principal Access Checkbox and Bulk Grants Implementation Plan

## Status

Implemented in this working tree for phases 1–14. Public proto enum/RPC changes were made in `mycel-api`, downstream Go protobuf outputs were regenerated, daemon/API bulk set handlers and tests were added, SDK/CLI/Console wiring was added, and Principal detail now includes an exact-scope checkbox editor while preserving direct grant record tables.

## Goal

Replace one-at-a-time role/capability grant workflows in mycel Console with a scoped checkbox editor on the Principal detail page.

Target admin workflow:

```text
Principals → Alice → Roles & capabilities
  choose scope
  check roles
  check additional direct capabilities
  Save changes
```

Effective capabilities remain additive:

```text
effective capabilities = capabilities from roles + direct capability grants
```

Capabilities inherited from roles should be visible as satisfied/inherited, but not removed by unchecking direct capability grants unless the role is also removed.

## Key Decisions

- Start public API changes in `mycel-api/api/proto/...`.
- Do not hand-edit generated protobuf files.
- Add missing automation/inference/admin capability enum values to the public `Capability` enum.
- Keep roles as convenience bundles; daemon/API authorization remains capability-authoritative.
- Add bulk/set-style APIs for exact-scope direct grants.
- Checkbox edits operate on **direct grants at the selected exact scope**, not all effective inherited access.
- Existing single grant/revoke APIs can remain for compatibility and simple tools.
- Console should show inherited capabilities as checked/locked with source labels.
- Use mycel/subsystem terminology.

## Desired Console UX

### Principal detail: Roles & capabilities tab

```text
Alice
alice · prn_abc123 · active

[ Overview ] [ Roles & capabilities ] [ Sessions ]

Roles & capabilities
Daemon authorization is authoritative. Roles expand to capabilities; direct
capability grants are additive.

Scope to edit
( ) System-wide
(•) Space    [ Research space       v ]
( ) Domain   [ Research space       v ] [ Notes domain v ]

┌───────────────────────────────────────────────────────────────────────────┐
│ Roles for this scope                                            [Reset]   │
├───────────────────────────────────────────────────────────────────────────┤
│ ☑ space.owner                                                            │
│ ☐ space.admin                                                            │
│ ☑ automation.admin                                                       │
│ ☐ inference.admin                                                        │
│ ☐ semantic.admin                                                         │
└───────────────────────────────────────────────────────────────────────────┘

Capabilities
Inherited capabilities are shown checked and locked. Direct capability grants
are editable and additive.

┌───────────────────────────────────────────────────────────────────────────┐
│ Automation                                                                │
│ ☑ automation.read                 inherited from automation.admin 🔒       │
│ ☑ automation.manage               inherited from automation.admin 🔒       │
│ ☑ automation.run                  inherited from automation.admin 🔒       │
│                                                                           │
│ Inference                                                                 │
│ ☑ inference.profile.read          inherited from automation.admin 🔒       │
│ ☐ inference.audit.read            direct grant                             │
│                                                                           │
│ Graph                                                                     │
│ ☑ graph.read                     direct grant                              │
│ ☑ graph.write                    direct grant                              │
│ ☐ graph.delete                   direct grant                              │
└───────────────────────────────────────────────────────────────────────────┘

Reason
[ Allow Alice to manage graph automations in Research ]

[ Cancel ] [ Save changes ]
```

### Unsaved changes summary

```text
Pending changes
Roles to add: automation.admin
Roles to remove: space.admin
Capabilities to add: graph.write
Capabilities to remove: blob.delete

[Cancel] [Save changes]
```

### Important behavior

If `automation.admin` is checked, its inherited capabilities appear checked/locked:

```text
☑ automation.manage    inherited from automation.admin 🔒
```

If the admin wants to remove `automation.manage`, they must uncheck `automation.admin`; they cannot uncheck only the inherited capability.

If a capability is both inherited and directly granted:

```text
☑ automation.manage    inherited + direct
```

Unchecking the direct grant should leave it checked/locked if still inherited.

## Public API Changes

### Phase 1 — Extend `Capability` enum

File:

```text
mycel-api/api/proto/mycel/common/v1/access.proto
```

Add enum values for canonical daemon capabilities that currently exist as internal strings but are not exposed publicly.

Suggested additions:

```proto
// Semantic management.
CAPABILITY_SEMANTIC_MANAGE = 122;

// Inference capabilities.
CAPABILITY_INFERENCE_CATALOG_READ = 180;
CAPABILITY_INFERENCE_CATALOG_MANAGE = 181;
CAPABILITY_INFERENCE_PROFILE_READ = 182;
CAPABILITY_INFERENCE_PROFILE_MANAGE = 183;
CAPABILITY_INFERENCE_CREDENTIAL_READ = 184;
CAPABILITY_INFERENCE_CREDENTIAL_MANAGE = 185;
CAPABILITY_INFERENCE_GRANT_MANAGE = 186;
CAPABILITY_INFERENCE_POLICY_MANAGE = 187;
CAPABILITY_INFERENCE_AUDIT_READ = 188;
CAPABILITY_INFERENCE_INVOKE = 189;

// Automation capabilities.
CAPABILITY_AUTOMATION_READ = 200;
CAPABILITY_AUTOMATION_MANAGE = 201;
CAPABILITY_AUTOMATION_RUN = 202;
CAPABILITY_AUTOMATION_WORKER = 203;

// Cluster read, because Console already treats cluster.read separately from manage.
CAPABILITY_CLUSTER_READ = 220;
```

Keep existing numeric values stable. Do not reuse reserved numbers.

### Phase 2 — Add bulk/set APIs to AdminPrincipalService

File:

```text
mycel-api/api/proto/mycel/admin/v1/principal.proto
```

Add RPCs:

```proto
rpc SetPrincipalRolesForScope(SetPrincipalRolesForScopeRequest) returns (SetPrincipalRolesForScopeResponse);
rpc SetPrincipalCapabilitiesForScope(SetPrincipalCapabilitiesForScopeRequest) returns (SetPrincipalCapabilitiesForScopeResponse);
```

Add messages:

```proto
message SetPrincipalRolesForScopeRequest {
  string principal_id = 1;
  mycel.common.v1.AccessScope scope = 2;
  repeated string roles = 3;
  string reason = 4;
}

message SetPrincipalRolesForScopeResponse {
  repeated PrincipalRoleGrant grants = 1;
  repeated string effective_roles = 2;
  repeated mycel.common.v1.Capability effective_capabilities = 3;
}

message SetPrincipalCapabilitiesForScopeRequest {
  string principal_id = 1;
  mycel.common.v1.AccessScope scope = 2;
  repeated mycel.common.v1.Capability capabilities = 3;
  string reason = 4;
}

message SetPrincipalCapabilitiesForScopeResponse {
  repeated PrincipalCapabilityGrant grants = 1;
  repeated mycel.common.v1.Capability effective_capabilities = 2;
}
```

Semantics:

- For the target principal and exact scope, direct grants should equal the requested set after the operation.
- Grants at other scopes are unchanged.
- Effective access is returned after mutation.
- Empty `roles` or `capabilities` means remove all direct grants for that principal at the exact scope.
- Caller must have `identity.grant.manage`.

## Daemon Implementation

### Phase 3 — Regenerate protobuf outputs

After editing `mycel-api`, regenerate downstream code using the existing repo commands/scripts. Do not hand-edit generated files.

Expected downstream repos:

```text
mycel
mycel-go-sdk
mycel-rust-sdk
mycel-console/src-tauri via mycel-rust-sdk proto generation
```

### Phase 4 — Update capability mapping

Update daemon/admin mapping so new enum values map to canonical internal strings:

```text
CAPABILITY_AUTOMATION_MANAGE      ↔ automation.manage
CAPABILITY_INFERENCE_PROFILE_READ ↔ inference.profile.read
...
```

Files likely involved:

```text
mycel/internal/daemon/api/admin/helpers.go
mycel/internal/identity/service/principal/policy.go
mycel/internal/daemon/api/client/auth_service.go
```

Ensure:

- role expansion still returns canonical/internal capabilities;
- Admin API responses can map those to public enum values;
- Console self-access still canonicalizes them for UX gates.

### Phase 5 — Implement exact-scope set operations

Add principal service methods to set direct grants for a principal/scope.

Preferred behavior:

1. Load direct role/capability grants for principal.
2. Filter to exact matching scope.
3. Compute:
   - grants to add
   - grants to revoke
   - grants to keep
4. Apply mutations with one logical operation if the service supports it; otherwise apply in a deterministic sequence and return aggregate errors.
5. Return current direct grants and effective access.

Exact scope matching means:

```text
system == system
space(sp1) == space(sp1)
domain(sp1, dom1) == domain(sp1, dom1)
```

Space-scoped grants may apply to domain requests for authorization, but the **editor** should only mutate grants with the exact selected scope.

### Phase 6 — Tests for daemon/API behavior

Add tests for:

- New enum values round-trip through admin API.
- `SetPrincipalRolesForScope` adds missing roles.
- `SetPrincipalRolesForScope` revokes roles omitted from request for the same exact scope.
- Grants at other scopes are unchanged.
- Empty set clears exact-scope direct role grants.
- Same cases for capabilities.
- Capability grants are additive to role-derived capabilities.
- Inherited capabilities remain effective until the role is removed.
- Caller without `identity.grant.manage` receives PermissionDenied.

## SDK/CLI Updates

### Phase 7 — SDK support

Add helpers for the new AdminPrincipal APIs where SDKs expose admin functionality.

Potential helpers:

```go
SetPrincipalRolesForScope(ctx, principalID, scope, roles, reason)
SetPrincipalCapabilitiesForScope(ctx, principalID, scope, capabilities, reason)
```

Rust equivalent if applicable.

### Phase 8 — CLI support

Optionally add CLI commands for parity:

```sh
mycel principal role set --principal-id prn_alice --scope space --space-id sp1 --role automation.admin --role space.editor --reason 'update access'

mycel principal capability set --principal-id prn_alice --scope space --space-id sp1 --capability graph.read --capability graph.write --reason 'update access'
```

This can be follow-up if Console is the immediate goal.

## Console Implementation

### Phase 9 — Tauri/service wiring

Add Console service methods:

```ts
setPrincipalRolesForScope(input)
setPrincipalCapabilitiesForScope(input)
```

Add Tauri commands calling the new gRPC methods.

Keep existing single grant/revoke methods for now.

### Phase 10 — Scope editor

On Principal detail → Roles & capabilities:

- Add scope selector at top of tab.
- System/Space/Domain radio buttons.
- Space dropdown.
- Domain dropdown filtered by space.

When scope changes:

- compute direct roles at exact scope;
- compute direct capabilities at exact scope;
- compute inherited capabilities from effective roles at that scope where possible.

### Phase 11 — Role checkbox editor

Replace **Grant role** modal-first workflow with inline checkboxes:

```text
Roles for selected scope
☐ space.viewer
☐ space.editor
☑ automation.admin
☐ inference.admin
```

Role changes update local draft state until Save.

### Phase 12 — Capability checkbox editor

Show capabilities grouped by subsystem.

Each capability row should show:

- checked state
- direct vs inherited source
- disabled/locked if only inherited

Examples:

```text
☑ automation.manage    inherited from automation.admin 🔒
☑ graph.write          direct
☐ blob.delete          direct
```

Rules:

- Direct capabilities are editable.
- Inherited-only capabilities are checked and disabled.
- Direct + inherited capabilities can have the direct grant toggled off, but remain checked/disabled if inherited.

### Phase 13 — Save behavior

On Save:

1. Call `SetPrincipalRolesForScope` with selected role checkboxes.
2. Call `SetPrincipalCapabilitiesForScope` with selected direct capability checkboxes.
3. Reload role/capability access.
4. Show success message.

If atomicity across roles+capabilities is required, add a combined API later. First cut can save roles then capabilities and report partial failure clearly.

### Phase 14 — Preserve revoke/advanced detail affordances

The checkbox editor handles most updates. Keep detail tables or collapsible sections for audit/debug:

```text
Direct grant records
[ Role grants ] [ Capability grants ]
```

These can still expose revoke actions for individual grant cleanup if useful, but they should no longer be the primary editing workflow.

## Tests

### API/daemon tests

- New capability enum mapping.
- Bulk set role/capability add/remove/noop behavior.
- Exact-scope preservation.
- Effective capability additive behavior.
- PermissionDenied when caller lacks grant management.

### Console tests

- Access Management is not in sidebar.
- Principal detail has Roles & capabilities tab.
- Scope selector uses shared select component.
- Role checkboxes render selected direct grants for scope.
- Capability checkboxes render direct and inherited states.
- Inherited-only capability checkbox is checked/disabled.
- Saving calls bulk role/capability set services with selected exact scope.
- Cancel/reset discards draft changes.
- Graph automation author preset selects expected role/capabilities.

## Validation Commands

```sh
cd mycel-api && make test
cd mycel && go test ./...
cd mycel-go-sdk && go test ./...
cd mycel-rust-sdk && MYCEL_API_ROOT=../mycel-api cargo test
cd mycel-console && npx tsc --noEmit
cd mycel-console && npm test -- --runInBand
cd mycel-console && npm run build
cd mycel-console/src-tauri && MYCEL_API_ROOT=../../mycel-api cargo check
git diff --check
```

## Acceptance Criteria

- Public `Capability` enum includes automation and inference capabilities needed by Console.
- Admin APIs can set exact-scope direct role grants in bulk.
- Admin APIs can set exact-scope direct capability grants in bulk.
- Console Principal detail uses checkboxes for role/capability assignment.
- Capabilities inherited from roles are visible, checked, and locked unless the role is removed.
- Direct capabilities remain additive to role-derived capabilities.
- Access Management remains removed from primary navigation.
- Daemon/API authorization remains authoritative.
- Tests/build pass across affected repos.

## Follow-ups

- Add a combined `SetPrincipalAccessForScope` API if we need roles+capabilities saved atomically together.
- Add access audit history once daemon audit APIs expose grant mutation events.
- Consider richer capability metadata service so Console can discover capability labels/groups from daemon instead of hard-coding them.
