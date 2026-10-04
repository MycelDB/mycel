# Graph Procedures and Automation Bindings Implementation Plan

## Status

In progress. GPAB0-GPAB11 have an initial implementation: fixtures, model split,
file storage accessors, legacy expansion, binding runtime-principal derivation,
procedure+binding execution, delegated inference hardening tests, public
protobuf/API/SDK surfaces, CLI commands, console list/detail diagnostics,
legacy migration tooling, and Knot PKM page-summary integration.

This plan tracks the design in
[Graph procedures and automation bindings](../../design/automation/graph-procedures-and-automation-bindings.md).

The motivating production issue is platform-managed per-user automation. Today an
operator/admin client can create a graph automation for a user's domain, causing
later background runs to inherit the operator/admin owner as the on-behalf
principal. Per-user automations should instead execute as the automation worker
on behalf of the user's principal, under explicit domain-scoped grants/policies.

## Goals

- Split reusable graph work from runtime bindings:
  - `GraphProcedure`: reusable logic/workflow: input assembly, prompt, inference
    operation defaults, output schema/actions, local safety ceilings.
  - `GraphAutomationBinding`: trigger/schedule/manual binding, scope, status,
    debounce/idempotency, runtime principal, and inference profile selection.
- Store explicit runtime security context on bindings:
  - `actor_principal_id`;
  - `owner_principal_id`;
  - `on_behalf_of_principal_id`;
  - inference profile reference/ID.
- Ensure invocations and runs use binding runtime context instead of inferring
  on-behalf principal from the creator/operator.
- Keep inference authorization fail-closed through existing profiles,
  capabilities, credential grants, policies, and usage telemetry.
- Preserve backwards compatibility for existing combined automation definitions.
- Provide CLI/admin API support for procedure and binding lifecycle.
- Update console and run diagnostics so operators can see procedure, binding,
  runtime principals, and authorization decisions.

## Non-goals

- Do not add arbitrary user-supplied server-side code.
- Do not make the built-in `automation` principal globally privileged.
- Do not bypass inference credential grants/policies.
- Do not execute graph automations synchronously inside graph write
  transactions.
- Do not add unbounded scans or implicit full-domain backfills.
- Do not make product billing decisions inside Mycel; Mycel remains neutral
  execution/telemetry infrastructure.
- Do not hand-edit generated protobuf files. If public API changes are needed,
  update source protobufs in `github.com/myceldb/mycel-api` and regenerate SDKs.

## Current baseline and assumptions

- Current automation definitions are domain-scoped canonical JSON documents.
- Current definitions include trigger, condition, input, inference, prompt,
  output, safety, and optional workflow fields in one document.
- Invocations are queued from graph events/schedules/scans and runs are concrete
  attempts.
- Current runs use actor `automation`; on-behalf currently falls back through
  invocation actor/definition owner in some paths.
- Current inference resolver supports actor and on-behalf principal checks using
  credential grants and policies.
- Graph context automations can select aliases, render bounded GQL context, and
  update aliases other than `changed`.
- Standalone inference profiles/grants/policies exist and are enforced.

## Implementation phases

## GPAB0: Baseline audit and compatibility fixtures

Status: initial implementation complete.

### Feature scope

Document current behavior and add fixtures that lock the intended compatibility
and delegated-runtime semantics before changing storage/API behavior.

### Tasks

1. Inventory existing automation domain model types:
   - definition;
   - trigger;
   - condition;
   - input/context rendering;
   - inference ref;
   - output/actions;
   - safety;
   - invocation;
   - run;
   - workflow instance/step/proposal records.
2. Add test fixtures for:
   - current combined automation JSON;
   - proposed standalone `GraphProcedure` JSON;
   - proposed `GraphAutomationBinding` JSON;
   - legacy combined definition expanded to generated procedure + binding.
3. Add a failing/skipped acceptance test for the motivating scenario:
   - operator creates binding for user domain;
   - binding runtime says `actor=automation`, `on_behalf=user`;
   - run uses user as on-behalf, not operator;
   - inference grant scoped to `automation` on behalf of user matches.
4. Document current operational limitation in automation docs until fixed.

### Tests

- Existing automation model/service/action tests continue to pass.
- New fixtures parse and validate as testdata where supported.

### Acceptance

```sh
go test ./internal/automation/model ./internal/automation/service ./internal/automation/actions -count=1
git diff --check
```

## GPAB1: Internal model split

Status: initial implementation complete.

### Feature scope

Introduce internal graph procedure and graph automation binding domain types while
preserving legacy definition support.

### Tasks

1. Add model types, conceptually:

   ```go
   type Procedure struct {
       ID string
       Name string
       Version int
       Status string
       Description string
       Input Input
       Inference InferenceRef
       Prompt string
       Output Output
       Workflow *Workflow
       Safety Safety
       CreatedAt time.Time
       UpdatedAt time.Time
   }

   type Binding struct {
       ID string
       Name string
       ProcedureID string
       ProcedureVersion int
       Status string
       Scope BindingScope
       Trigger BindingTrigger
       Runtime RuntimeContext
       Debounce *Debounce
       Idempotency Idempotency
       CreatedByPrincipalID string
       UpdatedByPrincipalID string
       CreatedAt time.Time
       UpdatedAt time.Time
   }

   type RuntimeContext struct {
       ActorPrincipalID string
       OwnerPrincipalID string
       OnBehalfOfPrincipalID string
       InferenceProfile string
       InferenceProfileID string
   }
   ```

   Exact package names should follow existing automation model conventions.
2. Move trigger-specific fields out of procedure model and binding-specific
   fields out of reusable procedure model.
3. Add validation rules:
   - procedure ID/version required;
   - procedure cannot define trigger/scope/runtime principals;
   - binding requires procedure ID;
   - binding requires scope domain;
   - binding runtime principals are normalized;
   - inference procedures require binding or procedure inference operation;
   - binding `runtime.inference_profile_id` or procedure default must be present
     before enablement when inference is required.
4. Add compatibility adapter:
   - current combined definition -> `Procedure` + `Binding` view;
   - generated procedure ID can be deterministic, e.g.
     `<automation-id>.procedure` for embedded legacy procedures;
   - legacy automation ID remains the binding ID.
5. Ensure canonical JSON normalization is stable for both new and legacy forms.

### Tests

- Unit: valid procedure passes validation.
- Unit: valid event binding passes validation.
- Unit: procedure with trigger/runtime fails validation.
- Unit: enabled inference binding without profile/default fails validation.
- Unit: legacy combined definition expands deterministically.
- Regression: all existing example automations still validate.

### Acceptance

```sh
go test ./internal/automation/model -count=1
git diff --check
```

## GPAB2: Storage layout and migration-compatible accessors

Status: initial implementation complete.

### Feature scope

Add procedure and binding storage while keeping existing automation storage/API
usable.

### Tasks

1. Extend file-backed storage layout:

   ```text
   automations/
     procedures/<domain-id>/<procedure-id>.json
     bindings/<domain-id>/<binding-id>.json
     definitions/<domain-id>/<automation-id>.json   # legacy compatibility
   ```

2. Add storage interfaces:
   - `CreateProcedure`, `UpdateProcedure`, `GetProcedure`, `ListProcedures`,
     `DeleteProcedure`;
   - `CreateBinding`, `UpdateBinding`, `GetBinding`, `ListBindings`,
     `DeleteBinding`;
   - compatibility `CreateDefinition`, `UpdateDefinition`, `GetDefinition`,
     `ListDefinitions` backed by expansion/embedding.
3. Decide legacy persistence mode:
   - Option A: keep writing legacy definitions under `definitions/` and generate
     procedure/binding views at read time.
   - Option B: when a legacy definition is created/updated, persist generated
     procedure+binding and optionally a legacy shadow file.
4. Preserve existing invocation/run file locations initially to reduce churn.
5. Update storage tests for round-trip, update, delete, and list behavior.
6. Add corruption/invalid JSON handling with actionable but sanitized errors.

### Tests

- Unit: procedure round-trip.
- Unit: binding round-trip.
- Unit: legacy definition round-trip remains unchanged for old callers.
- Unit: listing bindings includes migrated/generated legacy bindings.
- Unit: deleting a binding does not delete a shared procedure unless explicitly
  requested.

### Acceptance

```sh
go test ./internal/automation/storage ./internal/automation/model -count=1
git diff --check
```

Use the real package paths present in the codebase if storage is currently under
`service` or another package.

## GPAB3: Runtime invocation context derivation

Status: initial implementation complete.

### Feature scope

Make event/schedule/manual invocation creation derive actor/owner/on-behalf from
binding runtime context rather than from creator identity.

### Tasks

1. Change candidate selection to produce a binding reference and resolved
   procedure reference.
2. When creating invocation records, copy runtime principals from the binding:
   - `ActorPrincipalID` defaults to `automation` if omitted for background
     bindings;
   - `OwnerPrincipalID` defaults from binding runtime owner;
   - `OnBehalfOfPrincipalID` defaults from binding runtime on-behalf;
   - event origin principal is stored separately.
3. Add optional event-origin override policy, default disabled:

   ```json
   "runtime": {
     "event_origin_override": "disabled"
   }
   ```

   Future modes may include `if_allowed_by_binding` or `if_matches_owner`.
4. Ensure retry preserves the original invocation runtime principals unless an
   explicit privileged repair operation requests rebinding.
5. Update run initialization to use invocation runtime fields, not legacy
   definition owner fallback, when present.
6. Persist binding/procedure IDs and versions on invocation/run records.
7. Preserve legacy behavior when invocation has no binding runtime context.

### Tests

- Unit: operator-created binding invokes as `automation` on behalf of user.
- Unit: event origin is recorded separately and does not override by default.
- Unit: retry preserves original on-behalf principal.
- Regression: legacy combined automation still uses previous owner fallback.
- Unit: scheduled binding uses configured owner/on-behalf even without event
  origin.

### Acceptance

```sh
go test ./internal/automation/service -run 'Invocation|Runtime|Retry|Schedule' -count=1
git diff --check
```

## GPAB4: Execution pipeline uses procedure + binding

Status: initial implementation complete.

### Feature scope

Execute a run by combining the binding's trigger/runtime/scope with the
procedure's reusable logic.

### Tasks

1. Resolve `Procedure` for every runnable invocation.
2. Evaluate binding trigger/condition against graph event context.
3. Render procedure input using procedure input/context definitions and aliases
   selected by the binding condition.
4. Resolve inference configuration:
   - operation from procedure inference;
   - profile/profile ID from binding runtime if set;
   - procedure default profile only as fallback where allowed;
   - binding/runtime parameters may merge or override procedure defaults if
     supported.
5. Apply procedure output/actions.
6. Apply debounce/idempotency from binding first, falling back to procedure
   safety where appropriate.
7. Record diagnostics:
   - binding ID/version;
   - procedure ID/version;
   - runtime principals;
   - resolved inference profile;
   - policy decision;
   - credential grant;
   - target alias/node ID;
   - context row counts.

### Tests

- Integration: event binding references page-summary procedure and writes target
  page summary.
- Integration: one procedure has two bindings with different triggers.
- Integration: two bindings for two users share procedure but use distinct
  runtime principals/profiles.
- Unit: binding profile overrides procedure profile.
- Regression: existing combined automation execution still works.

### Acceptance

```sh
go test ./internal/automation/service ./internal/automation/actions ./internal/automation/render -count=1
git diff --check
```

## GPAB5: Inference authorization and delegated execution hardening

Status: initial implementation complete.

### Feature scope

Ensure explicit binding runtime context interacts correctly with standalone
inference profiles, grants, policies, and usage telemetry.

### Tasks

1. Add an end-to-end inference fixture:
   - built-in automation actor;
   - user principal;
   - operator creator;
   - binding runtime on behalf of user;
   - grant grantee `automation` and allow-on-behalf user;
   - policy allows summarize in user domain.
2. Verify inference resolver sees:
   - `ActorPrincipalID == automation`;
   - `OnBehalfOfPrincipalID == user`;
   - profile/model/capability constraints from binding/procedure;
   - domain/node scope from binding/run target.
3. Verify telemetry records binding/procedure metadata without secrets.
4. Add negative tests:
   - no on-behalf grant -> denied;
   - wrong domain -> denied;
   - wrong operation -> denied;
   - disabled binding -> no invocation;
   - disabled procedure -> no run or validation failure.
5. Consider adding an optional grant/profile validation endpoint for binding
   enablement so callers can detect misconfiguration before first event.

### Tests

- Service integration with fake connector succeeds only with matching delegated
  grant.
- Negative tests fail closed with actionable sanitized errors.
- Usage event includes actor/on-behalf/binding/procedure metadata.

### Acceptance

```sh
go test ./internal/automation/service ./internal/inference/service -run 'Automation|Delegated|Grant|Policy' -count=1
git diff --check
```

## GPAB6: Admin API, protobuf, and SDK changes

Status: initial implementation complete.

### Feature scope

Expose first-class procedures and bindings through daemon admin APIs while
keeping legacy automation APIs as compatibility wrappers.

### Tasks

1. Update `mycel-api` protobufs with procedure and binding messages/services.
2. Add request/response messages for:
   - validate procedure;
   - create/update/get/list/delete procedure;
   - validate binding;
   - create/update/get/list/enable/disable/delete binding;
   - invoke procedure/binding manually if included in this tranche.
3. Include runtime principal fields on binding messages.
4. Include procedure/binding IDs on invocation/run summaries and details.
5. Regenerate Go SDK via the repo script.
6. Implement daemon admin service handlers.
7. Keep existing `AdminAutomationService` methods:
   - create/update combined definitions as legacy compatibility;
   - list/get returns old shape unless a new endpoint is used;
   - run listing may include new IDs where backwards compatible.
8. Ensure auth checks distinguish creator/updater from runtime owner.

### Tests

- Proto generation/build passes.
- Admin service unit/integration tests for procedure CRUD.
- Admin service unit/integration tests for binding CRUD and enable/disable.
- Compatibility tests for existing automation API and CLI.

### Acceptance

```sh
# in mycel-api
make generate # or repo-specific proto generation command

# in mycel-go-sdk
./scripts/generate-proto.sh
go test ./...

# in mycel
go test ./internal/daemon/api/admin ./internal/automation/... -count=1
git diff --check
```

Adjust commands to the actual repository Makefiles.

## GPAB7: CLI support

Status: initial implementation complete.

### Feature scope

Add CLI commands for procedure and binding lifecycle while preserving existing
`mycel automation` commands.

### Tasks

1. Add commands:

   ```text
   mycel procedure validate FILE
   mycel procedure create FILE --domain <domain>
   mycel procedure update <procedure-id> FILE --domain <domain>
   mycel procedure get <procedure-id> --domain <domain>
   mycel procedure list --domain <domain>
   mycel procedure delete <procedure-id> --domain <domain>

   mycel automation-binding validate FILE
   mycel automation-binding create FILE --domain <domain>
   mycel automation-binding update <binding-id> FILE --domain <domain>
   mycel automation-binding get <binding-id> --domain <domain>
   mycel automation-binding list --domain <domain>
   mycel automation-binding enable <binding-id> --domain <domain>
   mycel automation-binding disable <binding-id> --domain <domain>
   mycel automation-binding delete <binding-id> --domain <domain>
   ```

2. Consider aliases:
   - `mycel graph procedure ...`;
   - `mycel graph automation ...`;
   - or retain `mycel automation` for bindings and add
     `mycel automation procedure` subcommands.
3. Add output summaries showing:
   - binding status;
   - procedure ref/version;
   - trigger type;
   - runtime actor/on-behalf/owner;
   - scope.
4. Update examples under `examples/automations/` or add
   `examples/procedures/` and `examples/automation-bindings/`.
5. Keep `mycel automation create/update/put` working with legacy combined JSON.

### Tests

- CLI command tests for procedure CRUD.
- CLI command tests for binding CRUD.
- CLI compatibility tests for current automation commands.
- Example JSON validates through CLI.

### Acceptance

```sh
go test ./internal/cli/... -run 'Procedure|Binding|Automation' -count=1
git diff --check
```

## GPAB8: Console UI support

Status: initial implementation complete.

### Feature scope

Expose procedures, bindings, invocations, and runs in `mycel-console` with clear
runtime-principal diagnostics.

### Tasks

1. Update Automations tab model:
   - procedures list/detail;
   - bindings list/detail;
   - legacy combined definitions clearly marked as legacy/compatibility.
2. Binding list columns:
   - ID/name;
   - status;
   - procedure ID/version;
   - trigger type;
   - labels/schedule;
   - owner principal;
   - on-behalf principal;
   - last invocation/run status.
3. Binding detail:
   - JSON definition;
   - runtime principal panel;
   - inferred authorization checklist if API available;
   - enable/disable controls.
4. Run detail:
   - procedure/binding IDs;
   - actor/on-behalf/owner/event-origin principals;
   - profile/capability/credential grant/policy decision;
   - context row counts;
   - mutation/output hashes;
   - sanitized error.
5. Avoid displaying secrets, provider payloads, prompts if hidden by policy,
   private graph text beyond existing admin visibility rules, or raw credential
   data.

### Tests

- UI tests for rendering procedure/binding lists.
- UI tests for runtime-principal details.
- UI tests for run diagnostics.
- Regression tests for existing automations tab behavior.

### Acceptance

```sh
npm test -- --runInBand <automation-console-tests>
npm run build
git diff --check
```

Use the console repo commands and paths present in the project.

## GPAB9: Migration and compatibility rollout

Status: initial implementation complete.

### Feature scope

Roll out the split model without breaking existing automations.

### Tasks

1. Add versioned storage/API compatibility behavior.
2. Provide a migration command or lazy migration path:

   ```text
   mycel automation migrate-combined --domain <domain> --dry-run
   mycel automation migrate-combined --domain <domain>
   ```

3. Migration output should show:
   - source automation ID;
   - generated procedure ID;
   - generated binding ID;
   - runtime principal derived from legacy owner;
   - warnings when owner/on-behalf is likely operator/admin.
4. Keep legacy files readable indefinitely or until an explicit deprecation
   version.
5. Add release notes with operational guidance:
   - how to create per-user bindings;
   - how to provision delegated grants;
   - how to diagnose on-behalf mismatches;
   - how to migrate old combined automations.
6. Update examples and docs:
   - design doc;
   - graph automation docs;
   - inference-for-graph-automations docs;
   - CLI docs;
   - operations docs.

### Tests

- Migration dry-run test.
- Migration apply test.
- Old automation JSON still creates/runs.
- New procedure+binding JSON creates/runs.
- Mixed old/new list APIs remain stable.

### Acceptance

```sh
go test ./internal/automation/... ./internal/daemon/api/admin/... ./internal/cli/... -count=1
make docs-check
git diff --check
```

## GPAB10: Knot PKM validation scenario

Status: initial implementation complete for automated code-path validation; live
local data validation remains an operator-run checklist.

### Feature scope

Validate the motivating downstream platform-managed automation scenario end to
end.

### Tasks

1. In a local Knot/Mycel workspace, create one reusable page-summary procedure.
2. For an onboarded user, create two bindings:
   - page-created/updated binding;
   - page-entry-created/updated binding.
3. Bind runtime context:

   ```text
   actor_principal_id: automation
   owner_principal_id: <PKM user's Mycel principal>
   on_behalf_of_principal_id: <PKM user's Mycel principal>
   inference_profile_id: <user generation profile>
   ```

4. Provision generation grant:

   ```text
   grantee_principal_ids:
     - automation
   allow_on_behalf_of_principal_ids:
     - <PKM user's Mycel principal>
   scope:
     user space/domain
   operations:
     - summarize
   ```

5. Create pages and entries.
6. Verify runs succeed and write `properties.summary` on page nodes.
7. Verify run records show:
   - actor `automation`;
   - on-behalf user principal;
   - owner user principal;
   - policy decision allowed;
   - matching credential grant;
   - provider-reported token usage if provider reports it.
8. Verify an operator-created binding does not run on behalf of operator unless
   explicitly configured to do so.

### Tests / validation commands

Example manual validation:

```sh
mycel procedure create examples/procedures/knot-page-summary.json \
  --space-id <space-id> --domain <domain>

mycel automation-binding create examples/automation-bindings/knot-page-summary-entry.json \
  --space-id <space-id> --domain <domain>

mycel automation runs --domain <domain-id> --automation <binding-id> --limit 20
mycel automation run get <run-id> --domain <domain-id>
mycel query gql 'MATCH (p:pkm.page) RETURN p FETCH FIRST 5 ROWS ONLY' \
  --space-id <space-id> --domain-id <domain-id> -u <user> -p <password> --output json
```

Automated validation can be added to daemon integration tests once the procedure
and binding APIs are stable.

### Acceptance

- Page summaries are written for a user-owned domain by an operator-provisioned
  binding.
- Runs use user on-behalf context, not operator on-behalf context.
- Removing the delegated grant causes a fail-closed denial.
- Re-adding the grant and retrying succeeds.

## GPAB11: Knot PKM product integration

Status: initial implementation complete.

### Feature scope

Update Knot PKM to provision reusable page-summary procedures and per-user
runtime bindings instead of legacy combined automations.

### Completed tasks

- Knot now creates one reusable `knot-pkm-page-summary` procedure.
- Knot creates page and entry graph-event bindings with runtime context:
  - actor `automation`;
  - owner and on-behalf set to the user's Mycel principal;
  - generation profile ID set on the binding runtime.
- Knot disables split-model bindings and falls back to disabling legacy combined
automations when bindings are not found.
- Knot generation grants allow the `automation` actor on behalf of the user's
  Mycel principal.
- Server tests validate structured output, binding runtime principal projection,
  and procedure/binding upsert behavior.

## Observability and safety requirements

- Errors must be actionable but sanitized.
- Do not log or return provider secrets, ciphertext, plaintext credentials,
  prompt bodies when hidden by policy, provider payloads containing private
  notes, billing secrets, or raw tokens.
- Run detail may include IDs, hashes, counts, status, policy decision IDs,
  credential grant IDs, model/capability IDs, and sanitized provider error
  classes.
- Binding validation should make principal mismatches easy to diagnose:
  - actor principal;
  - on-behalf principal;
  - available matching grants;
  - policy allow/deny reason.

## Risks

- Splitting model/storage may be larger than expected because workflows,
  schedules, scans, proposals, and legacy APIs all touch definitions.
- Existing console/API users may depend on combined JSON. Keep compatibility
  wrappers until downstreams migrate.
- Runtime principal semantics must be precise. Overly broad grants for
  `automation` would create privilege escalation risk.
- Event-origin principal override can become confusing; default it off until a
  strong policy model exists.
- Procedure versioning must avoid surprising behavior when shared procedures are
  updated while old bindings still reference prior semantics.

## Recommended implementation order

1. GPAB0 baseline fixtures.
2. GPAB1 internal model split and validation.
3. GPAB2 storage/accessors with legacy compatibility.
4. GPAB3 runtime principal derivation.
5. GPAB4 execution pipeline using procedure+binding.
6. GPAB5 delegated inference authorization tests.
7. GPAB6 admin API/proto/SDK.
8. GPAB7 CLI.
9. GPAB8 console UI.
10. GPAB9 migration docs/tools.
11. GPAB10 Knot PKM validation scenario.

The smallest useful slice is GPAB0-GPAB5 plus compatibility wrappers. That slice
can fix platform-managed per-user automation ownership without requiring a full
console authoring experience.
