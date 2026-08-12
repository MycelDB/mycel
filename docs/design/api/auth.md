# Principal Auth API

## Status

Current daemon API design after unified principal identity.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/common/v1/auth.proto
```

## Purpose

`mycel.common.v1.AuthService` is the common authentication API for the mycel daemon. It authenticates principals, issues short-lived access tokens, rotates durable refresh tokens, and lets a principal inspect or revoke its own auth sessions.

The service is intentionally in the Common API namespace. Admin and client/data-plane APIs use the same bearer tokens. Whether a caller can perform system management or graph/data operations is determined by principal roles, capability grants, and scoped authorization checks, not by a separate admin/user identity store.

## Auth sessions vs graph sessions

Mycel daemon mode uses two different session concepts:

- **Auth session**: durable login session used to refresh access tokens.
- **Graph session**: daemon-owned read or write transaction/session used by graph APIs.

`AuthService` only manages auth sessions. Graph sessions belong to the client graph/session API and are discussed separately.

## Scope

`AuthService` includes:

- login
- refresh
- logout
- identity lookup for the current access token
- caller-owned auth session listing
- revocation of one of the caller's auth sessions
- revocation of all of the caller's other auth sessions

`AuthService` does **not** include:

- signup or registration flows
- principal creation/deletion
- password reset administration
- role/capability management
- graph sessions or graph transaction lifecycle

Administrative identity management belongs to `mycel.admin.v1.AdminPrincipalService`.

## Service definition

```protobuf
service AuthService {
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc Refresh(RefreshRequest) returns (RefreshResponse);
  rpc Logout(LogoutRequest) returns (LogoutResponse);
  rpc WhoAmI(WhoAmIRequest) returns (WhoAmIResponse);
  rpc ListAuthSessions(ListAuthSessionsRequest) returns (ListAuthSessionsResponse);
  rpc RevokeAuthSession(RevokeAuthSessionRequest) returns (RevokeAuthSessionResponse);
  rpc RevokeOtherAuthSessions(RevokeOtherAuthSessionsRequest) returns (RevokeOtherAuthSessionsResponse);
}
```

## Authentication and authorization

`Login` and `Refresh` are unauthenticated at the access-token layer but validate username/password or refresh-token credentials respectively.

The following methods require a valid access token or equivalent authenticated request context:

- `WhoAmI`
- `ListAuthSessions`
- `RevokeAuthSession`
- `RevokeOtherAuthSessions`

`Logout` may be authorized by access token, refresh token, or cookie context depending on transport.

Authorization for admin and data-plane operations is performed by the target service using the authenticated `principal_id`, role bindings, capability grants, and resource scope.

## Security requirements

- Access tokens should be short-lived.
- Refresh tokens should be rotated on refresh.
- Refresh token plaintext must not be persisted.
- Stored refresh token values must be hashes.
- Reuse of an already-consumed refresh token should revoke the token family.
- Browser refresh tokens should use HttpOnly cookies.
- Audit logs must not include passwords, refresh token plaintext, or refresh token hashes.
- Session summaries should expose only coarse metadata.

## ClientInfo

`ClientInfo` is client-supplied metadata intended for session labeling and diagnostics. It is not trustworthy and must not be used for security decisions.
