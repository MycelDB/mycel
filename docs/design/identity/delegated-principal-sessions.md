# Delegated Principal Session Lifecycle

Mycel supports user-scoped service access by allowing an authorized operator/service
principal to create a durable auth session for another principal through
`mycel.admin.v1.AdminPrincipalService.CreatePrincipalSession`.

This document clarifies the lifecycle for those delegated sessions and the error
contract that long-running service applications should use to evict stale cached
clients.

## Concepts

Mycel uses two session concepts that are intentionally separate:

- **Auth session**: durable identity session used to refresh short-lived access
  tokens. Auth sessions are listed by `AuthService.ListAuthSessions` for the
  current principal and by `AdminPrincipalService.ListPrincipalSessions` for
  operators with session-management capability.
- **Graph session**: daemon-owned working context for graph transactions. Graph
  sessions are opened through `mycel.client.v1.SessionService` and do not refresh
  identity credentials.

A delegated principal session is an auth session. It is not a graph session and
it is not a separate authorization bypass. The issued access token still carries
the target principal ID, and every protected admin/data-plane RPC is authorized
against that principal's roles, capabilities, space/domain membership, and
resource scope.

## Creation

`AdminPrincipalService.CreatePrincipalSession` requires
`CAPABILITY_IDENTITY_SESSION_DELEGATE`. The target principal must be active and
login-enabled. On success the daemon returns:

- `access_token`: short-lived bearer token for immediate RPCs;
- `access_token_expire_time`: access-token expiry;
- `refresh_token`: refresh credential for rotating the auth session;
- `auth_session_id`: durable auth-session ID for listing/revocation/auditing.

The session's `ClientInfo` is stored only for diagnostics and audit visibility.
It must not be used for authorization decisions.

## Lifetimes

Default daemon lifetimes are:

| Credential | Default | Behavior |
| --- | ---: | --- |
| Access token | `15m` (`MYCELD_ACCESS_TOKEN_TTL`) | Short-lived bearer token. SDKs refresh before expiry and retry once on expired-token `Unauthenticated`. |
| Refresh-session idle TTL | `30d` | Extended on successful refresh. If no refresh occurs before this deadline, the auth session becomes expired. |
| Refresh-session absolute TTL | `90d` | Upper bound from session creation. Refresh cannot extend beyond this deadline. |

Keep access tokens short-lived. Long-running services should retain the refresh
token and allow the SDK to rotate it rather than increasing access-token TTL.

## Revocation and inactive states

An auth session can become inactive through:

- explicit `AuthService.Logout` or `AuthService.RevokeAuthSession`;
- explicit `AuthService.RevokeOtherAuthSessions`;
- explicit `AdminPrincipalService.RevokePrincipalSession` or
  `RevokePrincipalSessions`;
- principal administration with `revoke_sessions` such as disable, delete, or
  password reset flows that request session revocation;
- refresh-token reuse detection, which revokes the token family;
- idle or absolute expiry;
- session cleanup/reconciliation marking an expired session inactive and later
  redacting retained token hashes.

Session list APIs omit inactive sessions by default. Use `include_inactive=true`
when investigating expired or revoked sessions.

## Enforcement

Protected daemon RPCs validate both layers before reaching service handlers:

1. the bearer access token must be signed, well-formed, and unexpired;
2. the token's `auth_session_id` must still refer to an active durable auth
   session for the authenticated principal.

Therefore an explicitly revoked or expired auth session fails protected RPCs even
if the access token's short TTL has not elapsed yet.

## Stable stale-session error semantics

Authentication failures use gRPC `codes.Unauthenticated`. When the daemon can
identify a stable auth/session condition, it attaches `google.rpc.ErrorInfo`:

```text
domain = "mycel.auth"
reason = "auth_session_revoked" | "auth_session_expired" | "auth_session_invalid" | "refresh_session_invalid" | ...
```

Relevant reasons for long-running delegated clients:

| Reason | Meaning | Recommended service-client response |
| --- | --- | --- |
| `auth_session_revoked` | The durable auth session was explicitly revoked or its token family was revoked. | Discard the cached delegated client/session and create a new delegated session if policy still allows. |
| `auth_session_expired` | The durable auth session exceeded idle or absolute expiry. | Discard the cached delegated client/session and create a new delegated session if policy still allows. |
| `auth_session_invalid` | The access token refers to no usable durable auth session. | Discard the cached delegated client/session; re-mint after confirming the target principal is still valid. |
| `refresh_session_invalid` | The presented refresh token is unknown, malformed, already consumed, or otherwise unusable. | Discard the cached delegated client/session; do not retry the same refresh token. |

Other authentication reasons include `authorization_token_expired` and
`authorization_token_invalid`. SDKs normally handle expired access tokens by
refreshing. Do not treat `codes.PermissionDenied` as a stale-session signal:
`PermissionDenied` means the authenticated principal lacks access. Likewise,
`NotFound` for a workspace/domain/resource is not an auth-session refresh signal.

## Service-client caching guidance

Long-running service applications may cache delegated clients, but the cache must
be self-healing:

1. Store the returned access token, access-token expiry, refresh token, and
   `auth_session_id` together.
2. Use an SDK that refreshes before access-token expiry.
3. Evict the cached client/session on the stale-session reasons above.
4. Recreate a delegated session only after checking application policy still
   allows acting for that principal.
5. Bound cache entries by application policy. Mycel does not require a fresh
   delegated session for every RPC, but background services should prefer short
   units of work or an application max-age well below the daemon's idle refresh
   TTL.
6. Revoke delegated sessions when a bounded operation completes and the client is
   not intended to be reused.

For Go callers, `mycel-go-sdk` exposes `mycel.IsAuthSessionInvalid(err)` to test
for the stale-session reasons above:

```go
if mycel.IsAuthSessionInvalid(err) {
    discardCachedDelegatedClient(principalID)
    client, session, err := admin.DialUserClient(ctx, principalID)
    // retry the bounded operation once if application policy allows
}
```

This predicate is intentionally narrower than `mycel.IsAuthenticationError`: it
classifies only conditions where the cached auth/delegated session should be
discarded. It does not classify true authorization failures, missing workspaces,
or ordinary graph/domain access errors.
