# REPL Space Connection and GQL Execution Implementation Plan

## Status

Planned.

## Goal

Add a psql-like interactive workflow to the `mycel` CLI REPL so an operator or application principal can connect to a space/domain once and then run GQL queries without repeating `--space-id` and `--domain` flags.

Target experience:

```text
mycel> login alice secret
logged in as alice
mycel> connect space Notes
connected to space Notes (space-id) domain default (domain-id)
mycel[Notes/default]> gql MATCH (n) RETURN n FETCH FIRST 10 ROWS ONLY
...
mycel[Notes/default]> disconnect
mycel>
```

This should feel similar to `psql` connecting to a database, while preserving mycel's model:

```text
psql database ~= mycel space + default/current domain
```

## Non-goals

- Do not add a new daemon service.
- Do not change GQL syntax.
- Do not change graph/session/transaction subsystem semantics.
- Do not make REPL state authoritative; it is client-local convenience state only.
- Do not automatically create spaces/domains from `connect`.
- Do not bypass normal principal authentication or space/domain authorization.

## Current state

The CLI already has a basic REPL in:

```text
internal/cli/cmd/repl.go
```

The app state already tracks a current space ID:

```text
internal/cli/app/app.go
App.CurrentSpaceID
```

Several commands already default `--space-id` from `App.CurrentSpaceID`, including:

```text
internal/cli/cmd/domain.go
internal/cli/cmd/query.go
```

The current gaps are:

- REPL prompt does not show connected space/domain.
- Current state does not track domain ID/key/name.
- `space set` only accepts space ID; it does not resolve names.
- There is no `connect`/`disconnect` mental model.
- GQL must be invoked as `query gql ...`; there is no REPL shortcut.
- Multi-word GQL text is awkward because the REPL currently tokenizes by shell-like arguments.

## Proposed UX

### REPL prompt

Prompt states:

```text
mycel>                         # disconnected
mycel[Notes]>                  # connected to a space only
mycel[Notes/default]>          # connected to a space and domain
mycel[space-id/default]>       # fallback when display name is unavailable
```

### Connect commands

Add REPL-native commands:

```text
connect space <space-id-or-name>
connect domain <domain-id-or-key>
connect <space-id-or-name>
connect <space-id-or-name>/<domain-id-or-key>
disconnect
```

Aliases:

```text
\c <space-id-or-name>
\c <space-id-or-name>/<domain-id-or-key>
\d                  # optional: list domains in current space
```

Keep existing commands as compatibility/help paths:

```text
space set <space-id>
space unset
```

### GQL shortcut

Add a REPL-native shortcut:

```text
gql MATCH (n) RETURN n FETCH FIRST 10 ROWS ONLY
```

Equivalent command form remains:

```text
query gql --space-id <space-id> --domain <domain-key> "MATCH ..."
```

If the REPL has a connected space/domain, `gql ...` uses them automatically.

If no domain is connected, resolve the default domain for the current space using the same logic as `query gql --domain <key>` currently uses when omitted.

## Data model changes

Extend `internal/cli/app.App` with REPL-local connection metadata:

```go
type App struct {
    CurrentSpaceID   *domainspace.SpaceID
    CurrentSpaceName string
    CurrentDomainID  string
    CurrentDomainKey string
    CurrentDomainName string
}
```

Rules:

- `CurrentSpaceID` remains the canonical local space selection.
- Domain state is optional; when empty, commands may resolve the space default domain.
- Names/keys are display hints and may be refreshed on connect.
- `logout` clears space and domain state.
- `disconnect` clears space and domain state without clearing credentials.

## Resolution behavior

### Space resolution

`connect space <value>` should resolve `<value>` in this order:

1. UUID parse succeeds and `SpaceService.GetSpace` returns a visible space.
2. Exact visible space name match from `SpaceService.ListSpaces`.
3. Case-insensitive visible space name match if it is unique.
4. Error if no match or ambiguous match.

Rationale: UUID-first keeps scripts deterministic; name resolution improves interactive usability.

### Domain resolution

`connect domain <value>` requires a connected space and resolves `<value>` in this order:

1. `DomainService.GetDomain` by domain ID.
2. `DomainService.GetDomain` by key.
3. Exact visible domain name match from `DomainService.ListDomains`.
4. Case-insensitive visible domain name match if it is unique.
5. Error if no match or ambiguous match.

### Default domain resolution

After connecting a space, the REPL should try to auto-select a domain:

1. If the space response exposes a default domain ID/key, use it if available.
2. Else list domains and select the one where `default=true`.
3. Else, if exactly one domain exists, select it.
4. Else leave domain unset and require `connect domain ...` before GQL.

## Implementation phases

### Phase 1 — REPL connection state and prompt

Files:

```text
internal/cli/app/app.go
internal/cli/cmd/repl.go
```

Tasks:

1. Add current domain/name fields to `App`.
2. Add helper methods:
   - `SetCurrentSpace(spaceID, name)`
   - `SetCurrentDomain(domainID, key, name)`
   - `ClearCurrentConnection()`
   - `Prompt() string`
3. Update REPL prompt rendering to call `a.Prompt()`.
4. Ensure `logout` clears the current connection.

Acceptance:

- REPL starts with `mycel>`.
- After setting a test space/domain in app state, prompt renders `mycel[space/domain]>` in unit tests.
- Existing non-REPL commands still compile and pass tests.

Validation:

```sh
go test ./internal/cli/cmd ./internal/cli/app
```

### Phase 2 — Space/domain resolver helpers

Files:

```text
internal/cli/cmd/repl.go
internal/cli/cmd/space.go
internal/cli/cmd/domain.go
```

Tasks:

1. Add resolver helpers that operate on daemon client APIs:
   - `resolveVisibleSpace(ctx, spaceClient, value)`
   - `resolveVisibleDomain(ctx, domainClient, spaceID, value)`
   - `resolveDefaultDomain(ctx, domainClient, spaceID)`
2. Use existing authenticated principal login path.
3. Prefer exact ID/key matches before name matches.
4. Return clear ambiguity errors.

Acceptance:

- Resolver unit tests cover ID, exact name, case-insensitive unique name, not found, and ambiguous name cases.
- Resolver helpers do not create or modify daemon state.

Validation:

```sh
go test ./internal/cli/cmd
```

### Phase 3 — REPL `connect` and `disconnect`

Files:

```text
internal/cli/cmd/repl.go
```

Tasks:

1. Add REPL-native command parsing for:
   - `connect space <value>`
   - `connect domain <value>`
   - `connect <space>`
   - `connect <space>/<domain>`
   - `disconnect`
   - `\c <target>` alias
2. On space connect, set current space and attempt default domain resolution.
3. On domain connect, require current space.
4. Print connection summary:

```text
connected to space Notes (<space-id>) domain default (<domain-id>)
```

5. Update REPL help banner to include `connect`, `disconnect`, and `gql`.

Acceptance:

- Existing `space set` continues to work.
- `connect` by UUID works.
- `connect` by name works for unique visible space names.
- `disconnect` clears state and prompt returns to `mycel>`.

Validation:

```sh
go test ./internal/cli/cmd
```

### Phase 4 — GQL REPL shortcut

Files:

```text
internal/cli/cmd/repl.go
internal/cli/cmd/query.go
```

Tasks:

1. Extract the core `query gql` execution path into a helper callable from both Cobra and REPL:

```go
runGQL(ctx, app, queryText, options)
```

2. Preserve existing CLI behavior for:

```sh
mycel query gql --space-id ... --domain ... "MATCH ..."
```

3. Add REPL-native `gql <query text>` handling that does not require shell quoting for spaces.
4. Use current space/domain from app state.
5. If no space is connected, return:

```text
error: no space connected; use connect space <space-id-or-name>
```

6. If no domain is connected and no default domain can be resolved, return:

```text
error: no domain connected; use connect domain <domain-id-or-key>
```

Acceptance:

- `gql MATCH ...` works in the REPL after `connect`.
- Existing `query gql ...` tests still pass.
- Multi-word GQL does not require quotes inside the REPL.

Validation:

```sh
go test ./internal/cli/cmd ./internal/daemon/api/client
```

### Phase 5 — Domain and query command parity

Files:

```text
internal/cli/cmd/domain.go
internal/cli/cmd/query.go
internal/cli/cmd/space.go
```

Tasks:

1. Make `query gql` prefer `App.CurrentDomainID`/`CurrentDomainKey` when command flags omit `--domain-id` and `--domain`.
2. Make domain commands preserve/update current domain when invoked from REPL and appropriate.
3. Consider adding non-REPL global flags later:

```text
--space <name-or-id>
--domain <key-or-id>
```

Defer non-REPL global flags unless needed for this tranche.

Acceptance:

- REPL state applies consistently to domain and GQL commands.
- Non-REPL commands remain backwards compatible.

Validation:

```sh
go test ./internal/cli/cmd
```

### Phase 6 — Documentation and examples

Files:

```text
docs/operations/cli/README.md
docs/operations/cli/query.md
docs/operations/cli/space.md
docs/operations/procedures/standalone-start.md
```

Tasks:

1. Document REPL connection flow.
2. Add examples:

```text
mycel repl
login alice secret
connect space Notes
gql MATCH (n) RETURN n FETCH FIRST 10 ROWS ONLY
```

3. Explain that connection state is local to the CLI process.
4. Explain space/domain authorization errors.

Acceptance:

- `make docs-check` passes.
- CLI reference mentions psql-like connect flow.

## Testing plan

### Unit tests

Add or extend tests in:

```text
internal/cli/cmd/*repl*_test.go
internal/cli/cmd/query_test.go
```

Coverage:

- prompt rendering with no connection, space-only, and space/domain;
- `connect` parsing forms;
- `connect` error cases;
- GQL shortcut without quoting;
- logout clears connection;
- disconnect preserves credentials but clears connection.

### Integration tests

Use existing daemon fixture helpers to test:

1. Start standalone daemon.
2. Login bootstrap principal.
3. Create or use a test space/domain.
4. Run REPL script:

```text
login admin <password>
connect space <name>
gql MATCH (n) RETURN n FETCH FIRST 1 ROW ONLY
exit
```

5. Assert output includes query result and no missing `--space-id` error.

### Manual smoke test

```sh
MYCELD_DATA_DIR=/tmp/mycel-repl MYCELD_GRPC_ADDR=127.0.0.1:9091 make start
make build-cli
./bin/mycel --daemon-addr 127.0.0.1:9091 repl
```

Then in REPL:

```text
login admin <bootstrap-password>
principal list
space add Notes --owner-username admin --default-domain-key default --default-domain-name Default
connect space Notes
gql MATCH (n) RETURN n FETCH FIRST 10 ROWS ONLY
exit
```

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Ambiguous space names produce surprising connections. | ID-first resolution, exact-name before case-insensitive, fail on ambiguity. |
| Domain default selection differs from daemon behavior. | Reuse `DomainService` data and existing `resolveDaemonDomainID` logic where possible. |
| REPL GQL parsing conflicts with shell-like tokenizer. | Treat lines beginning with `gql ` specially and pass the remainder verbatim. |
| Prompt makes stale name assumptions after rename. | Names are display hints; refresh on explicit connect. IDs remain canonical. |
| Write GQL in REPL commits unexpectedly. | Preserve existing `query gql` transaction behavior. Consider a later confirmation flag for write statements if needed. |

## Open questions

1. Should `connect space <name>` match archived spaces when `include_archived` is not set? Proposed answer: no.
2. Should `connect` support creating a missing space? Proposed answer: no.
3. Should prompt include principal username too, e.g. `mycel:alice[Notes/default]>`? Proposed answer: defer until credentials/token state are tracked beyond username/password fields.
4. Should `\c` be documented as stable or best-effort psql compatibility? Proposed answer: document as a stable alias once implemented.
