# mycel-console friendly resource labels implementation plan

## Status

Planned.

## Problem

Several Console screens expose daemon/API wire values directly in user-facing
text: UUID-like resource IDs, protobuf enum names, raw operation keys, and raw
scope identifiers. This makes related objects harder to recognize and creates
inconsistent presentation across screens. For example, a space can appear as a
friendly named link in one feature, as a raw `spaceId` in another, and as a raw
protobuf enum state where a shared badge already exists.

## Goals

- Prefer product names, display names, keys, and shared badges over raw IDs in
  ordinary user-facing table/detail text.
- Preserve raw IDs where they are operationally useful by making them secondary
  metadata, copyable detail, hover/title text, or explicit diagnostic fields.
- Reuse existing shared components first, such as `SpaceStateBadge`,
  `UserStateBadge`, `TextLink`, and typography primitives.
- Add small shared label components/formatters only when a pattern has at least
  two callers.
- Keep changes reviewable by landing feature-by-feature tranches.

## Non-goals

- Do not change daemon APIs or protobufs without a separate design decision.
- Do not remove IDs from operator/forensic cluster diagnostics where exact IDs
  are the primary value.
- Do not perform unbounded background lookups just to replace IDs with names.
- Do not hide unknown enum values; unknown values should remain visible as raw
  fallback text for debugging.
- Do not redesign page layouts as part of this cleanup.

## Design principles

1. **Friendly primary, exact secondary**: where a name/key/display name exists,
   show it first and keep the ID available as secondary muted text, tooltip, or
   copy action.
2. **No fake resolution**: if the current response does not include enough data
   to resolve an ID, either leave the ID visible or add an explicitly bounded
   lookup in a separate tranche.
3. **Shared mapping for shared enums**: status/enum formatting should be a
   component or helper, not duplicated string maps at call sites.
4. **Diagnostic exception**: Cluster/Raft, checksums, addresses, and forensic
   exports may keep raw identifiers prominent because exact wire values are the
   point of the screen.
5. **Accessibility**: badges/chips must expose readable text, not color-only
   state.

## Proposed building blocks

### Label components

- `PrincipalLabel` / `PrincipalLink`
  - Inputs: principal ID, username/display name when available, optional route.
  - Primary: username/display name.
  - Secondary: principal ID, truncated/copyable where useful.
- `SpaceLabel` / `SpaceLink`
  - Inputs: space ID, space name when available, optional route.
  - Primary: space name.
  - Secondary: space ID.
- `DomainLabel`
  - Inputs: domain ID, name/key when available, optional containing space label.
  - Primary: domain name/key.
  - Secondary: domain ID.
- `ResourceIdText`
  - Standard truncation/copy styling for IDs that remain intentionally visible.

### Formatting helpers/badges

- `formatEnumLabel(value: string)` for last-resort enum/key labelization.
- Shared badges/formatters for:
  - user state and session state;
  - space state, reusing `SpaceStateBadge`;
  - semantic rule/index/work-item state;
  - automation/procedure/binding/invocation status;
  - inference operation/model kind/privacy class/vector store type;
  - activity category/event type.

## Tranches

### FL1 — inventory and guardrails

- Convert the current audit into a tracked checklist with owners and acceptance
  criteria.
- Add lightweight test helpers for asserting that known protobuf enum prefixes
  are not rendered in specific product tables, except in explicit raw JSON or
  diagnostic views.
- Decide allowed raw-value zones:
  - raw JSON drawers;
  - Cluster/Raft forensic panels;
  - copyable secondary IDs.

Validation:

- Console tests continue to pass.
- Checklist distinguishes normal UX surfaces from diagnostic surfaces.

### FL2 — shared label primitives

- Add `ResourceIdText` for consistent truncation/copy/title behavior.
- Add `SpaceLabel`/`SpaceLink` and migrate existing ad hoc `space.name ||
  space.spaceId` call sites.
- Add `PrincipalLabel`/`PrincipalLink` using already loaded principal fields.
- Add `DomainLabel` for existing domain name/key + ID pairs.

Candidate files:

- `src/features/spaces/components/SpaceTable.tsx`
- `src/features/spaces/pages/SpaceDetailPage.tsx`
- `src/features/users/components/UserTable.tsx`
- `src/features/users/pages/UserDetailPage.tsx`
- `src/features/access/AccessPage.tsx`
- `src/features/maintenance/pages/MaintenancePage.tsx`
- `src/features/intelligence/semantic/pages/SemanticPage.tsx`
- `src/features/intelligence/automations/pages/AutomationsPage.tsx`

Validation:

- Existing route targets remain unchanged.
- IDs remain available in secondary/copyable form where previously visible.
- Light and dark mode snapshots/manual checks for table rows.

### FL3 — identity and access surfaces

- Users list: make username/display name the primary principal value and demote
  `Principal ID` to secondary metadata or a details affordance.
- User detail: show principal ID as copyable metadata, not the primary identity
  label.
- Account page: show current username/session principal in friendly form.
- Access page: reuse `PrincipalLink` and replace raw scope labels with resolved
  `SpaceLabel`/`DomainLabel` where data is already loaded.
- Delete/disable/password dialogs: keep exact principal ID available but make
  username/display name the primary confirmation target.

Candidate files:

- `src/features/users/components/UserTable.tsx`
- `src/features/users/components/DeleteUserDialog.tsx`
- `src/features/users/components/DisableUserDialog.tsx`
- `src/features/users/components/SetUserPasswordDialog.tsx`
- `src/features/users/pages/UserDetailPage.tsx`
- `src/features/access/AccessPage.tsx`
- `src/features/account/pages/AccountPage.tsx`

Validation:

- Tests assert username/display name primary labels.
- Principal IDs still appear in explicit metadata/copy locations.

### FL4 — spaces, domains, and maintenance surfaces

- Reuse `SpaceStateBadge` everywhere a space state appears.
- Replace owned-space `Space ID` table columns with `SpaceLabel` where possible.
- Replace domain ID columns with domain name/key primary and ID secondary.
- Keep route parameters as IDs internally.
- Improve Maintenance owner display with `PrincipalLabel` if owner fields are
  available.

Candidate files:

- `src/features/spaces/components/SpaceTable.tsx`
- `src/features/spaces/pages/SpaceDetailPage.tsx`
- `src/features/maintenance/pages/MaintenancePage.tsx`
- `src/features/users/pages/UserDetailPage.tsx`

Validation:

- No user-facing `SPACE_STATE_` outside raw/debug surfaces.
- Space and domain links still navigate to the same routes.

### FL5 — semantic and automation surfaces

- Add shared semantic status/automation status badges that label raw status keys
  consistently.
- Replace semantic rule/domain ID rows with `DomainLabel`, rule display name/key
  primary, rule ID secondary.
- Replace binding/profile/vector store ID fallbacks with keys/names where the
  API response already includes them.
- In automation lists, show procedure/binding/automation names first and put IDs
  in secondary copyable metadata.
- For invocations/runs/events, keep IDs accessible but add friendlier labels such
  as event type, target node/title, created/updated time, or automation name
  where already available.

Candidate files:

- `src/features/intelligence/semantic/pages/SemanticPage.tsx`
- `src/features/intelligence/automations/pages/AutomationsPage.tsx`
- `src/features/spaces/pages/SpaceDetailPage.tsx`

Validation:

- Raw JSON drawers still expose full IDs for debugging.
- Tables no longer use IDs as the only recognizable labels when a name/key is
  present.

### FL6 — inference catalog surfaces

- Add formatters for inference operations, model kind, connector type, privacy
  class, and vector store type.
- Prefer endpoint/model/vector store keys over IDs in visible group labels.
- Keep endpoint/model/capability IDs in tooltips/details, not primary cells.
- Consider resolving `installedBy` to a principal label only if the response or a
  bounded lookup can provide the principal without unbounded fanout.

Candidate files:

- `src/features/inference/components/InferenceModelTable.tsx`
- `src/features/inference/components/ModelEndpointTable.tsx`
- `src/features/inference/components/ModelEndpointCapabilityTable.tsx`
- `src/features/inference/components/InferencePackageTable.tsx`
- `src/features/inference/components/VectorStoreTable.tsx`
- `src/features/inference/pages/InferencePage.tsx`

Validation:

- Operation/model/privacy labels are human-readable.
- Unknown values still render visibly.

### FL7 — activity and dashboard event labels

- Add activity event type/category label helpers.
- Keep raw event type/source/resource in an expandable details affordance or
  tooltip.
- Ensure dashboard cards use friendly category/type labels while preserving the
  raw event in detail pages.

Candidate files:

- `src/features/activity/ActivityPage.tsx`
- `src/features/dashboard/components/LatestActivityCard.tsx`
- `src/features/cluster/components/ClusterEventLog.tsx`

Validation:

- Activity tables remain searchable/readable.
- Raw values are still available for support/debug workflows.

### FL8 — cluster diagnostics polish, not replacement

- Keep cluster IDs, raft group IDs, node IDs, checksums, and backend addresses
  visible because they are operational diagnostics.
- Add consistent truncation/copy/title treatment via `ResourceIdText`.
- Prefer node names next to node IDs wherever available.

Candidate files:

- `src/features/cluster/pages/ClusterPage.tsx`
- `src/features/cluster/pages/NodeDetailPage.tsx`
- `src/features/cluster/components/MembershipTable.tsx`
- `src/features/cluster/components/SpaceDistributionCard.tsx`

Validation:

- No loss of exact diagnostic values.
- Operator workflows can still copy IDs/checksums/addresses.

## Testing strategy

For each tranche:

```sh
cd ../mycel-console
npm test -- --runInBand
npm run build
git diff --check
```

Add focused tests for each converted surface:

- friendly label appears when name/key/display name exists;
- raw ID remains available where intentionally preserved;
- missing name falls back safely;
- unknown enum/status values render visibly rather than blanking;
- raw protobuf enum prefixes are absent from normal product surfaces.

Manual visual checks before final acceptance:

- light and dark mode;
- table density and truncation;
- copy/tooltip interactions;
- raw JSON/detail drawers still useful for debugging.

## Open questions

- Should principal/user name resolution be added to service responses for
  package `installedBy`, activity actors/resources, and access scope labels, or
  should Console perform bounded follow-up lookups?
- Which ID classes should always remain visible for operators even outside the
  Cluster page?
- Should `SpaceStateBadge` and `UserStateBadge` move into a shared resource
  label package once additional features import them?
- Should route breadcrumbs carry resolved names so detail pages reached by raw ID
  URLs can still show friendly titles before data loads?
