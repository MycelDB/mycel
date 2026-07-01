# Auth session renewal implementation plan

Status: in progress. Phases 1 through 4 are implemented on the `session_renewal` branch. This plan documents how MycelDB could add native durable auth/session renewal primitives if applications need Mycel-owned long-lived sessions. Knot PKM currently implements browser-session renewal at the application layer; see its `session_renewal` work for the immediate product implementation.

Related docs:

- `docs/architecture.md` — current auth/session architecture and package boundaries.
- `docs/cli.md` — current auth token TTL configuration.
- Knot PKM server `docs/design/session-renewal-auth.md` — app-owned refresh-session design.

## Current state

MycelDB currently provides short-lived access tokens via `engine.Authenticate`.

Important properties:

- Access tokens are opaque `engine.AccessToken` values.
- Access-token claims live in the engine's in-memory auth cache.
- Access tokens expire according to `auth.access_token_ttl` / `MYCELDB_AUTH_ACCESS_TOKEN_TTL`.
- Access tokens are not sliding.
- Engine restart clears issued access tokens.
- MycelDB does not yet provide durable refresh sessions, refresh-token rotation, session listing/revocation, token introspection, or service-role token minting.

Knot PKM now owns browser refresh sessions itself. It stores hashed refresh tokens in its protected system graph and re-authenticates Mycel users with PKM-owned credentials to obtain fresh short-lived Mycel access tokens.

## Goals

If MycelDB adds native session renewal, it should:

- Keep Mycel access tokens short-lived.
- Add durable refresh/session records that survive engine restart.
- Store refresh tokens only as cryptographic hashes.
- Rotate refresh tokens on every successful refresh.
- Detect old-token reuse and revoke the token family.
- Support individual session revocation and all-other-session revocation.
- Support session listing with coarse metadata only.
- Provide token introspection/expiry metadata for clients and embedding applications.
- Provide audit events for auth/session lifecycle.
- Preserve clear package boundaries and public API stability.

## Non-goals for the first Mycel-native implementation

- Browser cookie management. Cookie policy remains an application/web-server concern.
- OAuth/OIDC/SAML identity-provider support.
- Multi-factor auth.
- Device fingerprinting beyond caller-provided coarse metadata.
- Replacing application-owned sessions when the application needs product-specific signup/onboarding semantics.
- Long-lived access tokens.

## Design principles

1. Access tokens remain short-lived engine credentials.
2. Refresh sessions are durable records, not browser-readable access tokens.
3. Public API additions live in `engine.Engine`; storage details remain in `store/*` and `engine/internal`.
4. Session APIs should be usable by embedded Go applications without assuming HTTP.
5. Sensitive token material is never logged or returned.
6. Token reuse is treated as a likely compromise and revokes the token family.
7. Revocation and audit semantics should be deterministic and testable.

## Proposed public API

Add types under `engine/internal` and re-export them from `engine/engine.go`.

```go
type RefreshSessionID string
type RefreshToken string

type RefreshSessionStatus string

const (
    RefreshSessionStatusActive  RefreshSessionStatus = "active"
    RefreshSessionStatusRevoked RefreshSessionStatus = "revoked"
    RefreshSessionStatusExpired RefreshSessionStatus = "expired"
)

type LoginSessionInput struct {
    UserRef identity.UserRef
    Password string
    Metadata RefreshSessionMetadata
}

type LoginSessionResult struct {
    AccessToken AccessToken
    AccessTokenExpiresAt time.Time
    RefreshToken RefreshToken
    RefreshSession RefreshSessionInfo
}

type RefreshSessionInput struct {
    RefreshToken RefreshToken
    Metadata RefreshSessionMetadata
}

type RefreshSessionResult struct {
    AccessToken AccessToken
    AccessTokenExpiresAt time.Time
    RefreshToken RefreshToken
    RefreshSession RefreshSessionInfo
}

type RevokeRefreshSessionInput struct {
    AccessToken AccessToken
    SessionID RefreshSessionID
    Reason string
}

type RevokeOtherRefreshSessionsInput struct {
    AccessToken AccessToken
    CurrentSessionID RefreshSessionID
    Reason string
}

type ListRefreshSessionsInput struct {
    AccessToken AccessToken
}

type RefreshSessionMetadata struct {
    UserAgentHash string
    IPPrefixHash string
    ClientName string
}

type RefreshSessionInfo struct {
    ID RefreshSessionID
    UserID identity.UserID
    UserRef identity.UserRef
    Status RefreshSessionStatus
    TokenFamilyID string
    RotationCounter int
    CreatedAt time.Time
    LastUsedAt time.Time
    IdleExpiresAt time.Time
    AbsoluteExpiresAt time.Time
    RevokedAt time.Time
    RevokedReason string
    Metadata RefreshSessionMetadata
}
```

Add methods to `engine.Engine`:

```go
LoginSession(ctx context.Context, in LoginSessionInput) (LoginSessionResult, error)
RefreshSession(ctx context.Context, in RefreshSessionInput) (RefreshSessionResult, error)
ListRefreshSessions(ctx context.Context, in ListRefreshSessionsInput) ([]RefreshSessionInfo, error)
RevokeRefreshSession(ctx context.Context, in RevokeRefreshSessionInput) error
RevokeOtherRefreshSessions(ctx context.Context, in RevokeOtherRefreshSessionsInput) (int, error)
```

Optional later API:

```go
InspectAccessToken(ctx context.Context, in InspectAccessTokenInput) (AccessTokenInfo, error)
MintUserAccessToken(ctx context.Context, in MintUserAccessTokenInput) (AuthResult, error)
```

`MintUserAccessToken` should require explicit service-role/system authority and audit trails. It is not required for durable refresh sessions if `RefreshSession` can mint a new access token from a valid refresh token.

## Storage design

Add a new store package:

```text
store/session
```

Public store interface shape:

```go
type Manager interface {
    Init(ctx context.Context, root string) error
    Create(ctx context.Context, rec domainauth.RefreshSession) (domainauth.RefreshSession, error)
    GetByID(ctx context.Context, id domainauth.RefreshSessionID) (domainauth.RefreshSession, error)
    FindByTokenHash(ctx context.Context, hash string) (domainauth.RefreshSession, error)
    ListByUser(ctx context.Context, userID identity.UserID) ([]domainauth.RefreshSession, error)
    Update(ctx context.Context, rec domainauth.RefreshSession) (domainauth.RefreshSession, error)
    DeleteExpiredRedacted(ctx context.Context, cutoff time.Time) (int, error)
}
```

Add domain records under a new package or existing identity-adjacent package:

```text
domain/auth
```

File-backed storage layout example:

```text
<meta>/sessions/refresh_sessions.json
```

Security requirements:

- Store only refresh-token hashes, never plaintext refresh tokens.
- Use an algorithm-prefixed hash such as `sha256:<hex>` initially.
- Consider HMAC-SHA256 with an engine secret before production multi-tenant exposure.
- Use constant-time comparison where applicable.
- Redact token hashes after revoked/expired retention when possible.

## Configuration

Add engine config fields and environment bindings:

```yaml
auth:
  access_token_ttl: 1h
  refresh_idle_ttl: 720h
  refresh_absolute_ttl: 2160h
  refresh_audit_retention_ttl: 720h
  refresh_token_bytes: 32
```

Environment variables:

```text
MYCELDB_AUTH_ACCESS_TOKEN_TTL
MYCELDB_AUTH_REFRESH_IDLE_TTL
MYCELDB_AUTH_REFRESH_ABSOLUTE_TTL
MYCELDB_AUTH_REFRESH_AUDIT_RETENTION_TTL
MYCELDB_AUTH_REFRESH_TOKEN_BYTES
```

Validation:

- Access-token TTL must be positive.
- Refresh idle TTL must be positive.
- Refresh absolute TTL must be positive and >= idle TTL.
- Refresh audit retention TTL must be positive.
- Refresh token byte length should be >= 32.

## Audit events

Add auth audit storage, either in `store/session` or a dedicated `store/audit` package.

Minimum events:

- `auth.login_success`
- `auth.login_failure`
- `auth.refresh_success`
- `auth.refresh_failure`
- `auth.refresh_reuse_detected`
- `auth.session_revoked`
- `auth.session_family_revoked`
- `auth.session_cleanup`

Audit records must not include plaintext passwords, access tokens, refresh tokens, or refresh-token hashes.

## Implementation phases

### Phase 0: finalize API and ownership boundaries

- Confirm MycelDB should own durable refresh sessions for generic embedded apps.
- Confirm browser cookie handling remains out of scope.
- Confirm whether Knot PKM will stay app-owned or migrate later.
- Update `docs/architecture.md` with final API decision.

Acceptance:

- API proposal reviewed.
- Security constraints agreed.
- Backward compatibility requirements documented.

### Phase 1: domain and store model — complete

- Add `domain/auth` refresh-session and audit-event records.
- Add `store/session` interface.
- Implement file-backed session store.
- Add tests for create/list/find/update/revoke/redact.

Acceptance:

- Refresh token plaintext is never persisted.
- Store survives engine restart.
- Revoked/expired redaction behavior is tested.

### Phase 2: engine configuration — complete

- Add refresh-session config fields.
- Add env aliases.
- Add validation tests.
- Update `docs/cli.md` and architecture docs.

Acceptance:

- Defaults preserve existing behavior for users who only call `Authenticate`.
- Invalid TTL/token-byte config is rejected.

### Phase 3: token generation and hashing helpers — complete

- Add cryptographically secure refresh-token generation.
- Add hash helpers with algorithm prefixes.
- Add constant-time token hash matching where needed.
- Add tests for entropy, format, and no-plaintext persistence.

Acceptance:

- Refresh tokens are high entropy.
- Stored values are hashes only.

### Phase 4: `LoginSession` — complete

- Implement `Engine.LoginSession`.
- Authenticate username/password as today.
- Mint short-lived access token.
- Create durable refresh session.
- Return plaintext refresh token once to caller.
- Emit audit events.

Acceptance:

- Existing `Authenticate` behavior remains unchanged.
- `LoginSession` creates a durable refresh session.
- Failed login creates no session and emits non-sensitive audit.

### Phase 5: `RefreshSession` with rotation

- Validate refresh token hash.
- Reject missing/revoked/expired sessions.
- Rotate refresh token on success.
- Increment rotation counter.
- Extend idle expiry without exceeding absolute expiry.
- Mint new short-lived access token.
- Emit success/failure audit.

Acceptance:

- Valid refresh returns new access + refresh tokens.
- Old refresh token cannot be reused.
- Engine restart does not invalidate durable refresh session.

### Phase 6: reuse detection and family revocation

- If a refresh token is unrecognized after a previous rotation, treat as reuse where enough metadata exists.
- Revoke token family on reuse detection.
- Emit `auth.refresh_reuse_detected` and `auth.session_family_revoked`.

Implementation note:

- True reuse detection may require storing consumed-token hashes or previous-token markers for a short retention window. If not implemented initially, document that old-token reuse is rejected but not attributable.

Acceptance:

- Reusing an old token fails.
- Family revocation works when reuse can be attributed.

### Phase 7: session listing and revocation APIs

- Implement `ListRefreshSessions` scoped to current access token user.
- Implement `RevokeRefreshSession`.
- Implement `RevokeOtherRefreshSessions`.
- Never return token hashes.

Acceptance:

- Users cannot list/revoke another user's sessions.
- Revoked session cannot refresh.
- Returned session metadata is privacy-conscious.

### Phase 8: cleanup and retention

- Add opportunistic cleanup on auth operations or an explicit engine maintenance method.
- Mark active expired sessions as expired.
- Redact token hashes after retention.
- Optionally delete old fully redacted records after a longer retention.

Acceptance:

- Expired session data has a cleanup/redaction path.
- Cleanup is idempotent.

### Phase 9: CLI support

Add operator/user-facing commands as appropriate:

```text
mycel auth session list
mycel auth session revoke <session-id>
mycel auth session revoke-other
mycel auth session cleanup
```

Acceptance:

- CLI can inspect and revoke sessions without exposing token material.
- CLI docs updated.

### Phase 10: migration and compatibility

- Existing users and apps using `Authenticate` continue to work unchanged.
- New session APIs are opt-in.
- If PKM later migrates from app-owned refresh sessions to Mycel-owned sessions, provide a migration plan to revoke/expire old PKM records safely.

Acceptance:

- No breaking change to current engine users.
- Migration guidance documented.

## Test plan

Unit tests:

- Config defaults and validation.
- Refresh token generation/hash format.
- Store create/list/find/update/revoke/redact.
- Audit records omit sensitive data.

Engine tests:

- `LoginSession` creates durable session.
- Failed login creates no session.
- `RefreshSession` rotates token and mints new access token.
- Old token reuse is rejected.
- Refresh works after engine reopen.
- Idle-expired and absolute-expired sessions fail.
- Revoked session fails refresh.
- User cannot list/revoke another user's sessions.

CLI tests:

- Session list excludes hashes.
- Revoke commands revoke expected sessions.
- Cleanup command redacts old session hashes.

## Security review checklist

- No plaintext refresh token persistence.
- No token/password material in audit logs.
- No token/hash material in list APIs or CLI output.
- Rotation is atomic from the perspective of the store implementation.
- Reuse detection revokes the family where implementable.
- Access token TTL remains short.
- Refresh absolute TTL is enforced.
- Service-role minting, if added later, has strict authorization and audit.

## Relationship to Knot PKM

Knot PKM does not need to wait for this MycelDB work. Its completed session-renewal design is appropriate because PKM owns signup/onboarding semantics and browser UX.

If MycelDB later implements this plan, PKM can either:

1. Keep its PKM-owned refresh sessions indefinitely, or
2. Migrate to Mycel-owned refresh sessions after confirming Mycel can represent PKM's signup-aware auth model, audit needs, and session-management UX.

Any migration should include a compatibility window and server-side revocation of obsolete PKM-owned refresh sessions.
