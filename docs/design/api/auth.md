# Client Auth API

## Status

Draft design for the daemon-oriented Client API on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
api/proto/mycel/client/v1/auth.proto
```

## Purpose

`AuthService` is the client-facing authentication API for the Mycel daemon. It authenticates users, issues short-lived access tokens, rotates durable refresh tokens, and lets a user inspect or revoke their own auth sessions.

This API is part of the **Client API**, not the Admin API. It is intended for normal applications and client connectors.

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
- user-owned auth session listing
- revocation of one of the caller's auth sessions
- revocation of all of the caller's other auth sessions

`AuthService` does **not** include:

- signup or registration flows
- admin user creation/deletion
- password reset administration
- daemon administration
- graph sessions or graph transaction lifecycle

At the present time, signup is a higher-level product concern handled by Knot PKM rather than Mycel Client API.

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

## Methods

### Login

Authenticates a user and creates a durable auth session.

Input:

- username
- password
- client metadata

Output:

- short-lived access token
- access token expiration timestamp
- authenticated principal
- optional refresh token for non-browser clients

Browser clients should prefer an HttpOnly refresh cookie set by the HTTP/Connect layer. Non-browser connectors may use the optional `refresh_token` response field and store it securely.

### Refresh

Rotates a refresh token/session and returns a new short-lived access token.

Input:

- optional refresh token
- client metadata

Output:

- short-lived access token
- access token expiration timestamp
- authenticated principal
- optional rotated refresh token for non-browser clients

Browser clients may omit the `refresh_token` field when the transport provides the refresh token through an HttpOnly cookie.

Refresh behavior:

- access tokens remain short-lived
- refresh tokens are durable but rotated
- refresh token plaintext is never persisted by the daemon
- only refresh token hashes are stored
- reuse of a consumed refresh token revokes the token family

### Logout

Revokes the current auth session or the explicitly supplied auth session when allowed.

If `auth_session_id` is omitted, the daemon resolves the current auth session from request context, refresh-token context, or cookie context.

Logout should be idempotent from a client perspective.

### WhoAmI

Returns the authenticated principal associated with the current access token.

This is useful for application boot, connector diagnostics, and validating that token refresh restored the expected identity.

### ListAuthSessions

Lists the caller's own durable auth sessions.

The response is intentionally coarse. It should not expose refresh token hashes, raw request metadata hashes, IP addresses, or user agents unless a later privacy review explicitly allows those fields.

The request supports:

- `page_size`
- `page_token`
- `include_inactive`

By default, implementations should return active sessions only.

### RevokeAuthSession

Revokes one of the caller's own durable auth sessions.

A normal client must not be able to revoke another user's sessions through this API. Admin-wide session revocation belongs in the Admin API.

### RevokeOtherAuthSessions

Revokes all durable auth sessions owned by the caller except the current one.

This supports a common "log out other devices" UX.

## Transport behavior

The API is defined in protobuf and should support generated gRPC client/server stubs.

Target transports:

- gRPC for backend/service connectors
- Connect-Web for browser clients
- optional JSON/HTTP mapping later for debugging and simple integrations

For browser clients, refresh tokens should be transported using HttpOnly cookies at the HTTP layer. The protobuf messages retain optional refresh token fields for non-browser connectors.

## Authentication and authorization

`Login` and `Refresh` are unauthenticated at the access-token layer but validate username/password or refresh-token credentials respectively.

The following methods require a valid access token or equivalent authenticated request context:

- `WhoAmI`
- `ListAuthSessions`
- `RevokeAuthSession`
- `RevokeOtherAuthSessions`

`Logout` may be authorized by access token, refresh token, or cookie context depending on transport.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| invalid credentials | `UNAUTHENTICATED` |
| expired/invalid access token | `UNAUTHENTICATED` |
| expired/invalid refresh token | `UNAUTHENTICATED` |
| refresh token reuse detected | `UNAUTHENTICATED` |
| current principal cannot revoke requested session | `PERMISSION_DENIED` |
| requested auth session does not exist | `NOT_FOUND` |
| malformed request | `INVALID_ARGUMENT` |
| rate limited | `RESOURCE_EXHAUSTED` |
| service unavailable | `UNAVAILABLE` |

## Security requirements

- Access tokens should be short-lived.
- Refresh tokens should be rotated on refresh.
- Refresh token plaintext must not be persisted.
- Stored refresh token values must be hashes.
- Reuse of an already-consumed refresh token should revoke its token family.
- Browser refresh tokens should use HttpOnly cookies.
- Audit logs must not include passwords, refresh token plaintext, or refresh token hashes.
- User-facing session summaries should expose only coarse metadata.

## ClientInfo

`ClientInfo` is client-supplied metadata intended for session labeling and diagnostics. It is not trustworthy and must not be used for security decisions.

Fields:

- `name`
- `version`
- `platform`
- `device_label`

## Open questions

- Should `AuthPrincipal` eventually expose roles or capabilities, or should authorization remain entirely server-side?
- Should `Logout` support "all sessions" directly, or should that remain `RevokeOtherAuthSessions` plus current logout?
- Should refresh-token family identifiers ever be exposed to clients? Current recommendation: no.
- Should inactive auth sessions be retained for user display, audit only, or immediately cleaned up?
