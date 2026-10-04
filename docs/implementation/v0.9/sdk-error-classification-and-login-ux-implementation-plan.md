# SDK error classification and login UX implementation plan

## Status

Implemented in the initial cross-repo tranche. Follow-ups may broaden structured error consumption beyond Console login and the Knot PKM Mycel-facing handlers migrated in this tranche.

## Context

Mycel clients currently surface many daemon, transport, and authentication
failures as strings. Downstream applications such as `mycel-console` and
`knot_pkm_server` must either show every failure as a generic error or infer
intent from fragile text matching.

This is especially visible on the Console login screen. A bad password, a
principal without sufficient access, a validation problem, and a daemon
connectivity failure are different operator experiences and should produce
different messages and alert severity.

The desired end state is SDK-owned error classification in both Rust and Go,
with downstream applications consuming a structured error envelope instead of
parsing strings.

## Goals

- Add comparable error classification helpers to both Mycel SDKs:
  - `../mycel-rust-sdk`
  - `../mycel-go-sdk`
- Refactor `../mycel-console` Tauri commands and frontend service helpers to use
  the Rust SDK classification.
- Refactor `../knot_pkm/knot_pkm_server` Mycel error handling to use the Go SDK
  classification.
- Update the Console login screen to render appropriate `Alert` variants:
  - bad password / invalid credentials: warning
  - validation problems: warning
  - missing Console capability / permission denied: warning
  - connectivity, timeout, unavailable daemon, TLS/DNS failures: error
  - unknown/internal failures: error
- Avoid introducing new protobuf/API requirements solely for local SDK error
  classification.

## Non-goals

- Do not change daemon public protobuf error contracts in this tranche.
- Do not remove existing human-readable error messages.
- Do not require all Console commands to be migrated at once; login and common
  admin/client invoke paths are the first priority.
- Do not hide diagnostic detail needed by operators; structured errors should
  preserve detail where safe.

## Error model

Use SDK-native representations with equivalent serialized values across SDKs.

### Error kinds

Initial common set:

- `validation`
- `connectivity`
- `authentication`
- `authorization`
- `not_found`
- `conflict`
- `rate_limited`
- `unavailable`
- `timeout`
- `internal`
- `unknown`

`connectivity` is for local transport establishment failures such as DNS,
connection refused, TLS setup, or an unreachable address. `unavailable` is for
server/gRPC service unavailability once a gRPC status can be observed. Login UX
may map both to an error alert.

### Severity hints

Applications may override severity by context, but SDKs can expose a default
hint:

- `info`
- `warning`
- `error`

Suggested default mapping:

| Kind | Default severity |
| --- | --- |
| `validation` | warning |
| `authentication` | warning |
| `authorization` | warning |
| `not_found` | warning |
| `conflict` | warning |
| `rate_limited` | warning |
| `connectivity` | error |
| `unavailable` | error |
| `timeout` | error |
| `internal` | error |
| `unknown` | error |

## Phase 1 — Rust SDK classification

Repository: `../mycel-rust-sdk`

Add SDK-level classification types and helpers, for example:

```rust
pub enum ErrorKind {
    Validation,
    Connectivity,
    Authentication,
    Authorization,
    NotFound,
    Conflict,
    RateLimited,
    Unavailable,
    Timeout,
    Internal,
    Unknown,
}

pub enum ErrorSeverity {
    Info,
    Warning,
    Error,
}

pub struct ClassifiedError {
    pub kind: ErrorKind,
    pub severity: ErrorSeverity,
    pub message: String,
    pub detail: Option<String>,
}
```

Expose helpers such as:

```rust
pub fn classify_error(err: &(dyn std::error::Error + 'static)) -> ClassifiedError;
pub fn classify_status(status: &tonic::Status) -> ClassifiedError;
```

Classification inputs:

- `tonic::Status`
  - `InvalidArgument` → `validation`
  - `Unauthenticated` → `authentication`
  - `PermissionDenied` → `authorization`
  - `NotFound` → `not_found`
  - `AlreadyExists` / `Aborted` → `conflict`
  - `ResourceExhausted` → `rate_limited`
  - `Unavailable` → `unavailable`
  - `DeadlineExceeded` → `timeout`
  - `Internal` / `DataLoss` → `internal`
- `tonic::transport::Error` and wrapped lower-level I/O/TLS/DNS failures
  - connection refused, DNS failure, TLS negotiation, no route, broken pipe →
    `connectivity`
- `tokio::time::error::Elapsed` and timeout wrappers → `timeout`
- fallback → `unknown`

Tests:

- Classify representative `tonic::Status` values.
- Classify transport/connectivity errors where practical.
- Verify wrapped errors are classified.
- Verify message/detail are preserved.

## Phase 2 — Go SDK classification

Repository: `../mycel-go-sdk`

Add equivalent SDK-level classification types and helpers, for example:

```go
type ErrorKind string

const (
    ErrorKindValidation     ErrorKind = "validation"
    ErrorKindConnectivity   ErrorKind = "connectivity"
    ErrorKindAuthentication ErrorKind = "authentication"
    ErrorKindAuthorization  ErrorKind = "authorization"
    ErrorKindNotFound       ErrorKind = "not_found"
    ErrorKindConflict       ErrorKind = "conflict"
    ErrorKindRateLimited    ErrorKind = "rate_limited"
    ErrorKindUnavailable    ErrorKind = "unavailable"
    ErrorKindTimeout        ErrorKind = "timeout"
    ErrorKindInternal       ErrorKind = "internal"
    ErrorKindUnknown        ErrorKind = "unknown"
)

type ErrorSeverity string

type ClassifiedError struct {
    Kind     ErrorKind
    Severity ErrorSeverity
    Message  string
    Detail   string
}

func ClassifyError(err error) ClassifiedError
func ErrorKindOf(err error) ErrorKind
func IsConnectivityError(err error) bool
func IsAuthenticationError(err error) bool
func IsAuthorizationError(err error) bool
```

Classification inputs:

- `status.Code(err)` from `google.golang.org/grpc/status`
  - `codes.InvalidArgument` → `validation`
  - `codes.Unauthenticated` → `authentication`
  - `codes.PermissionDenied` → `authorization`
  - `codes.NotFound` → `not_found`
  - `codes.AlreadyExists`, `codes.Aborted` → `conflict`
  - `codes.ResourceExhausted` → `rate_limited`
  - `codes.Unavailable` → `unavailable`
  - `codes.DeadlineExceeded` → `timeout`
  - `codes.Internal`, `codes.DataLoss` → `internal`
- `context.DeadlineExceeded` / timeout interfaces → `timeout`
- `net.Error`, `net.OpError`, connection refused, DNS errors, TLS handshake
  errors → `connectivity`
- fallback → `unknown`

Tests:

- Classify gRPC status errors.
- Classify wrapped gRPC status errors.
- Classify `context deadline exceeded`.
- Classify representative network errors.
- Verify helper predicates.

## Phase 3 — Console structured command errors

Repository: `../mycel-console`

Define a serializable Tauri command error envelope backed by Rust SDK
classification:

```rust
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsoleCommandError {
    pub kind: String,
    pub severity: String,
    pub message: String,
    pub detail: Option<String>,
}
```

Use it for login first:

- `src-tauri/src/commands/auth.rs`
  - `admin_login`
  - optionally `admin_connection_diagnostics`

Replace direct string conversion such as:

```rust
.map_err(|err| err.to_string())?
```

with a conversion through Rust SDK classification.

If Tauri command generics make `Result<T, ConsoleCommandError>` inconvenient,
return the envelope as a JSON string temporarily and parse it in the frontend
invoke wrapper. Prefer a true serializable error type if supported cleanly by the
current Tauri setup.

Tests:

- Rust command/helper tests for mapping classified SDK errors into
  `ConsoleCommandError`.
- Frontend service tests for parsing structured errors and falling back from
  legacy string errors.

## Phase 4 — Console frontend error handling and login UX

Repository: `../mycel-console`

Add frontend types, likely under `src/types` or `src/services`:

```ts
export type AppErrorKind =
  | "validation"
  | "connectivity"
  | "authentication"
  | "authorization"
  | "not_found"
  | "conflict"
  | "rate_limited"
  | "unavailable"
  | "timeout"
  | "internal"
  | "unknown";

export type AppErrorSeverity = "info" | "warning" | "error";

export type AppError = {
  kind: AppErrorKind;
  severity: AppErrorSeverity;
  message: string;
  detail?: string;
};
```

Update the invoke wrapper in `src/services/adminService.ts` to normalize Tauri
errors into `AppError`.

Update login state in:

- `src/features/auth/pages/LoginPage.tsx`
- `src/features/auth/components/LoginForm.tsx`

Desired behavior:

- validation error → `<Alert variant="warning">...`
- bad username/password → `<Alert variant="warning">...`
- permission denied / missing Console capability → `<Alert variant="warning">...`
- daemon unreachable / refused / DNS / TLS / timeout → `<Alert variant="error">...`
- unknown/internal → `<Alert variant="error">...`

Connectivity-specific UX:

- Show a concise message such as `Could not connect to the Mycel daemon.`
- Preserve detail below or in expandable text if useful.
- Suggest or surface the existing **Run connection diagnostics** action.

Tests:

- Bad password classified as `authentication` renders warning alert.
- Validation renders warning alert.
- Permission denied renders warning alert.
- Connection refused / unavailable / timeout renders error alert.
- Unknown string errors still render a safe generic error alert.

## Phase 5 — Knot PKM server structured Mycel errors

Repository: `../knot_pkm/knot_pkm_server`

Refactor Mycel daemon/SDK error handling to use Go SDK classification.

Likely target areas:

- daemon runtime bootstrap/open-session paths
- PKM user provisioning and tenant access repair
- Mycel-backed login/session flows
- semantic provisioning and maintenance calls
- handlers that currently stringify Mycel SDK errors into JSON responses

Introduce or extend an app-level structured error response:

```json
{
  "error": {
    "kind": "connectivity",
    "severity": "error",
    "message": "Could not connect to the Mycel daemon",
    "detail": "connection refused"
  }
}
```

Suggested HTTP mapping:

| Kind | HTTP status |
| --- | --- |
| `validation` | 400 |
| `authentication` | 401 |
| `authorization` | 403 |
| `not_found` | 404 |
| `conflict` | 409 |
| `rate_limited` | 429 |
| `connectivity` | 503 |
| `unavailable` | 503 |
| `timeout` | 504 |
| `internal` | 500 |
| `unknown` | 500 |

Tests:

- Connectivity errors produce structured `503` responses.
- Unauthenticated errors produce structured `401` responses.
- Permission denied errors produce structured `403` responses.
- Existing PKM handler error responses remain compatible where needed.

## Phase 6 — Cross-repo validation

Run targeted tests after each repo change, then full checks before merging.

Suggested commands:

```sh
cd ../mycel-rust-sdk && cargo test
cd ../mycel-go-sdk && go test ./...
cd ../mycel-console && npm test -- --runInBand
cd ../mycel-console && npm run build
cd ../knot_pkm/knot_pkm_server && go test ./...
cd mycel && make docs-check
```

If Console Tauri code changes consume regenerated Rust SDK APIs, also run the
Console Tauri check/build command used by the project, for example:

```sh
cd ../mycel-console && MYCEL_API_ROOT="$(cd ../mycel-api && pwd)" cargo check --manifest-path src-tauri/Cargo.toml --no-default-features
```

## Acceptance criteria

- Both Rust and Go SDKs expose documented error classification helpers.
- Console login no longer relies on string matching for normal SDK errors.
- Console login displays bad credentials as a warning alert.
- Console login displays connectivity failures as an error alert and points the
  user toward connection diagnostics.
- Knot PKM server maps Mycel SDK failures to structured app errors and suitable
  HTTP status codes.
- Legacy string errors still render safely as `unknown` errors.
- Tests cover SDK classification, Console login UX, and Knot PKM server error
  response mapping.

## Risks and follow-ups

- Some transport errors may arrive already flattened as strings by lower layers;
  classification should use typed sources first and only fall back to string
  matching for legacy compatibility.
- Tauri command error serialization may require an incremental adapter if the
  current command signatures assume `Result<T, String>`.
- SDK classification should not overpromise exact root cause for all network
  failures; prefer stable categories plus preserved detail.
- Future daemon API improvements could add structured gRPC error details, but
  that is intentionally outside this plan.
