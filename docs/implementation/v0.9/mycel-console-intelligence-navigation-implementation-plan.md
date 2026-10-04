# mycel-console Intelligence Navigation Implementation Plan

## Status

Implemented in `mycel-console` with the initial global Intelligence navigation, Access, Automations, and Semantic surfaces. The Semantic page uses existing semantic index and maintenance APIs; a dedicated semantic rule creation editor remains a backend/API follow-up if desired.

## Goal

Reorganize mycel-console's left navigation so AI/semantic/automation workflows
are presented as one operator-facing **Intelligence** area instead of a narrow
**Inference** area.

Target navigation shape:

```text
INTELLIGENCE
  Access
  Automations
  Semantic
```

Where:

- **Access** contains the existing inference setup surfaces: endpoints, models,
  credentials, grants, policies, profiles, plus related catalog/import/usage
  views as appropriate.
- **Automations** provides a holistic graph automation management page: list,
  status, enable/disable, create/edit, run history, diagnostics, and token usage.
- **Semantic** provides a holistic semantic generation/indexing page: generation
  rules/indexes, maintenance/backlog status, backfill/process actions, failures,
  and token usage.

Space detail pages may still offer contextual creation shortcuts for automations
and semantic rules, but the primary management and observability surfaces should
live under **Intelligence**.

## Design rationale

The current **Inference** nav label is implementation-oriented. Operators and
administrators are more likely to think in terms of intelligent system behavior:

- What model access is configured?
- What automations are running?
- What semantic generation rules/indexes exist?
- Which spaces/domains are consuming tokens?
- What failed, what is queued, and what is costing money?

The **Intelligence** grouping makes these relationships visible without forcing
operators to jump between spaces, inference catalog internals, and maintenance
pages.

## Current console context

Relevant current files in `mycel-console`:

```text
src/features/console/featureRegistry.ts
src/features/console/navigation.ts
src/components/layout/Sidebar.tsx
src/components/layout/AppShell.tsx
src/features/inference/pages/InferencePage.tsx
src/services/adminService.ts
src/types/inference.ts
src/types/automations.ts
src/types/semantic.ts
src/types/semanticMaintenance.ts
src-tauri/src/commands/inference.rs
src-tauri/src/commands/automations.rs
src-tauri/src/commands/semantic.rs
src-tauri/src/commands/semantic_maintenance.rs
```

Current observations:

- Navigation groups are defined in `featureRegistry.ts` / `navigation.ts`.
- The existing nav group is `inference` with label `Inference`.
- `InferencePage` is already a large tabbed page with tabs for endpoints,
  models, vector stores, profiles, credentials, grants, policies, usage, and
  import history.
- `Semantic` is currently registered under the `data` group as a placeholder.
- Automation APIs and types exist, and space/domain-level automation management
  exists in backend command surfaces, but there is not yet a dedicated global
  Intelligence/Automations page.
- Semantic and semantic maintenance commands/types exist, but there is not yet a
  dedicated global Intelligence/Semantic management page.

## Non-goals

- Do not remove space-local automation or semantic creation shortcuts.
- Do not change daemon APIs unless an explicit data gap is found.
- Do not expose credential secret values.
- Do not conflate inference access setup with automation/semantic operations;
  keep Access, Automations, and Semantic as separate pages.
- Do not make destructive semantic rebuild actions easy to trigger accidentally;
  keep rebuild/backfill actions explicit.

## Proposed information architecture

### Intelligence / Access

Route:

```text
/intelligence/access
```

Initial implementation can reuse the existing `InferencePage` with revised title
and copy. It should include existing setup tabs:

- Endpoints
- Models
- Credentials
- Grants
- Policies
- Profiles

Recommended secondary tabs or sections:

- Vector stores
- Usage
- Import history / packages

Naming decision:

- Use **Access** in the left nav because the page includes credentials, grants,
  policies, and profiles, not only catalog objects.
- Use "Catalog" only as an internal section heading if endpoints/models/import
  history need a label.

### Intelligence / Automations

Route:

```text
/intelligence/automations
```

Main page responsibilities:

- list graph automations across accessible spaces/domains;
- filter by space, domain, enabled state, trigger, status, model/profile;
- show latest run status and last error;
- show usage summary/token totals per automation;
- create/edit/validate/delete automations;
- enable/disable automations;
- inspect invocation/run history;
- deep-link to space/domain context;
- provide safe contextual shortcuts from space pages.

### Intelligence / Semantic

Route:

```text
/intelligence/semantic
```

Main page responsibilities:

- list semantic generation/index rules across accessible spaces/domains;
- filter by space/domain/status/profile/vector store;
- show maintenance status, dirty/backlog counts, checkpoints, and failures;
- show usage summary/token totals per rule/index/scope where available;
- manage semantic generation rules/indexes;
- run explicit maintenance actions such as analyze/process/backfill with clear
  confirmation and status feedback;
- deep-link to space/domain context;
- provide safe contextual shortcuts from space pages.

## Capability model

Suggested initial navigation requirements:

| Page | Read capability | Manage capabilities |
| --- | --- | --- |
| Intelligence / Access | `inference.catalog.read` | existing inference manage/admin capabilities per tab/action |
| Intelligence / Automations | automation read/list capability if available; otherwise existing automation command authorization | automation manage capability if available |
| Intelligence / Semantic | `semantic.search` or semantic read/list capability if available | `semantic.manage` for maintenance/backfill/manage actions |

Implementation should preserve capability-based hiding/disabled behavior. If the
backend does not expose a fine-grained automation read capability, use the same
capability assumptions already used by existing automation surfaces and document
any gap.

## Usage and token accounting

Both Automations and Semantic should surface token usage. The first version can
use existing inference usage APIs if they contain enough metadata to group by:

- purpose (`automation`, `semantic`, etc.);
- profile/model/endpoint;
- space/domain;
- automation ID or semantic index/rule ID when available.

If usage events do not currently carry enough correlation data, implement the UI
with available totals and create a follow-up daemon/API task to add stable
correlation fields.

Minimum viable usage display:

- total input tokens;
- total output tokens;
- total requests;
- failed requests if available;
- grouped by space/domain and profile/model;
- time window filter.

Preferred future display:

- automation-level totals;
- semantic rule/index-level totals;
- trend over time;
- latest expensive runs;
- denied/no-capability inference attempts.

## Implementation phases

### IN0 — Confirm route names and IA copy

Decisions to confirm before code:

- left nav label: `INTELLIGENCE`;
- Access route: `/intelligence/access`;
- Automations route: `/intelligence/automations`;
- Semantic route: `/intelligence/semantic`;
- whether `/inference` redirects to `/intelligence/access` for compatibility;
- whether `/semantic` redirects to `/intelligence/semantic`.

Acceptance:

- route and label decisions are captured in this plan or follow-up notes.

### IN1 — Navigation registry restructure

Update console feature registration:

- replace nav group `inference` with `intelligence`;
- update `navGroupOrder` and `navGroupLabels`;
- rename existing `Inference` feature to `Access`;
- change route from `/inference` to `/intelligence/access`;
- move `Semantic` out of `Data` into `Intelligence`;
- add `Automations` feature under `Intelligence`.

Compatibility routes:

- `/inference` -> `/intelligence/access`;
- `/semantic` -> `/intelligence/semantic`.

Acceptance:

- sidebar shows `INTELLIGENCE` with `Access`, `Automations`, and `Semantic`.
- old routes redirect rather than dead-end.
- capability gating still hides/disables features as before.

Tests:

```sh
npm test -- navigation
npm test -- Sidebar
```

### IN2 — Access page refactor

Refactor `InferencePage` into an Intelligence Access page without changing its
core behavior.

Possible file layout:

```text
src/features/intelligence/access/pages/AccessPage.tsx
src/features/intelligence/access/index.ts
```

or, for a smaller first step, keep implementation in `features/inference` and
export it as `AccessPage` until a later cleanup.

Required page changes:

- title/copy should say `Intelligence Access`, not just `Inference`;
- primary tabs should prioritize Endpoints, Models, Credentials, Grants,
  Policies, Profiles;
- keep vector stores, usage, and import history accessible but secondary;
- preserve existing create/edit/import flows and tests.

Acceptance:

- Access page has no behavior regressions from existing Inference page.
- Existing inference tests are updated for new naming/routes.

Tests:

```sh
npm test -- InferencePage
npm test -- AccessPage
```

### IN3 — Automations overview page

Create a new global Automations page under Intelligence.

Suggested files:

```text
src/features/intelligence/automations/pages/AutomationsPage.tsx
src/features/intelligence/automations/components/AutomationTable.tsx
src/features/intelligence/automations/components/AutomationUsageSummary.tsx
src/features/intelligence/automations/index.ts
```

Initial data strategy:

- load accessible spaces;
- load domains per selected/all spaces as needed;
- call existing automation list APIs per scope;
- flatten results into one table;
- fetch recent invocations/runs for selected automation or latest summary;
- fetch usage summaries filtered to automation purpose/profile/scope where
  supported.

UI should include:

- space/domain filters;
- enabled/disabled/status filters;
- automation list table;
- latest invocation/run status;
- usage summary card;
- actions: create, edit, validate, enable, disable, delete, view runs.

Acceptance:

- operator can see graph automations across accessible scopes from one page;
- existing space-local automation workflows still work;
- create/edit can either be fully implemented here or deep-link/open the same
  editor used from space detail.

Tests:

```sh
npm test -- AutomationsPage
```

### IN4 — Semantic overview page

Create a new global Semantic page under Intelligence.

Suggested files:

```text
src/features/intelligence/semantic/pages/SemanticPage.tsx
src/features/intelligence/semantic/components/SemanticRuleTable.tsx
src/features/intelligence/semantic/components/SemanticUsageSummary.tsx
src/features/intelligence/semantic/components/SemanticMaintenancePanel.tsx
src/features/intelligence/semantic/index.ts
```

Initial data strategy:

- load accessible spaces/domains;
- list semantic indexes/rules using existing semantic APIs;
- load semantic maintenance status/work items;
- fetch usage summaries filtered to semantic purpose/profile/scope where
  supported.

UI should include:

- space/domain filters;
- semantic rule/index table;
- status/backlog/dirty-work summary;
- explicit maintenance actions: analyze, process, backfill;
- usage summary card;
- detail drawer for raw diagnostic payloads.

Acceptance:

- operator can see semantic generation rules/indexes across accessible scopes;
- operator can understand backlog/failures without going space-by-space;
- destructive or expensive actions are explicit and confirmed.

Tests:

```sh
npm test -- SemanticPage
```

### IN5 — Space page contextual shortcuts

Keep space-local creation and management shortcuts but make them contextual.

On space/domain detail pages:

- show `Create automation for this domain` shortcut;
- show `Create semantic rule/index for this domain` shortcut;
- link to global Intelligence pages with preselected `spaceId`/`domainId` query
  params;
- avoid duplicating full global observability tables inside space detail.

Acceptance:

- users can still start from a space when thinking locally;
- global pages remain the canonical management/observability surfaces.

Tests:

```sh
npm test -- SpaceDetailPage
```

### IN6 — Usage correlation and fallback handling

Wire usage cards into Access, Automations, and Semantic.

Implementation rules:

- use existing `listInferenceUsageEvents` / `summarizeInferenceUsage` first;
- if backend usage events lack automation/rule IDs, show available scope/profile
  totals and display a note: `Detailed per-automation usage requires correlated
  usage events`;
- do not block the page if usage APIs fail; show a scoped warning.

Potential backend follow-up if needed:

- add usage event correlation fields:
  - `purpose`;
  - `automation_id`;
  - `automation_invocation_id`;
  - `semantic_index_id`;
  - `semantic_work_item_id`;
  - `space_id` and `domain_id` when known.

Acceptance:

- each Intelligence page can answer, at least at a high level, how much inference
  is being consumed and by which subsystem/scope.

### IN7 — Tests, migration, and cleanup

Update tests and docs:

- navigation tests for new group/order/routes;
- sidebar tests for `INTELLIGENCE` label;
- route tests for redirects;
- Access page regression tests;
- Automations and Semantic page smoke/render tests;
- docs or README notes if console docs exist for navigation.

Validation:

```sh
cd ../mycel-console
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm test
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run build
```

If Tauri build is required:

```sh
MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" npm run tauri dev
```

## Open questions

1. Should the left nav item be **Access** or **Catalog**?
   - Recommendation: **Access**.
2. Should vector stores live in Access or Semantic?
   - Recommendation: keep them in Access initially because they are provider
     infrastructure, but link them from Semantic when a semantic rule uses one.
3. Should `Maintenance` remain under Operations or move semantic-specific
   maintenance into Intelligence/Semantic?
   - Recommendation: move semantic maintenance views/actions into
     Intelligence/Semantic and keep Operations/Maintenance for broader daemon
     maintenance if needed.
4. Do usage events already include enough correlation fields for per-automation
   and per-semantic-rule token usage?
   - If not, implement UI fallback and create backend follow-up.

## Acceptance summary

The work is complete when:

- sidebar has `INTELLIGENCE` with `Access`, `Automations`, and `Semantic`;
- old `/inference` and `/semantic` routes redirect correctly;
- Access preserves existing inference setup behavior;
- Automations provides global graph automation list/manage/usage surfaces;
- Semantic provides global semantic rule/index/manage/usage surfaces;
- space pages provide contextual shortcuts into the global pages;
- usage is visible at the best available granularity;
- console tests and build pass.
