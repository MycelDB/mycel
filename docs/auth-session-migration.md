# Auth session renewal migration and compatibility

MycelDB's durable refresh-session APIs are opt-in. Existing applications that use `engine.Authenticate` continue to receive only short-lived in-memory access tokens and do not create durable refresh-session records.

## Compatibility guarantees

- `engine.Authenticate` behavior is unchanged.
- Existing initialized data directories can be reopened without a pre-existing `meta/sessions/refresh_sessions.json` file.
- The refresh-session store is initialized automatically under:

  ```text
  <data_dir>/meta/sessions/refresh_sessions.json
  ```

- Applications must call `engine.LoginSession` to create a durable refresh session.
- Applications must call `engine.RefreshSession` to rotate a refresh token and mint a new short-lived access token.
- Refresh-token plaintext is returned only once and must be handled by the embedding application. MycelDB persists only token hashes.

## Recommended adoption path for embedding applications

1. Keep existing `Authenticate` flows unchanged while upgrading MycelDB.
2. Add a new product/session layer that calls `LoginSession` only for clients that should receive long-lived refresh-session UX.
3. Store plaintext refresh tokens only in an appropriate client-side secure transport/storage mechanism, such as an HttpOnly cookie in a web application.
4. Use `RefreshSession` to rotate the refresh token on every refresh.
5. Use `ListRefreshSessions`, `RevokeRefreshSession`, and `RevokeOtherRefreshSessions` to build user-facing session management.
6. Run `CleanupRefreshSessions` from an operator/admin maintenance path, or rely on opportunistic cleanup during login/refresh.

## Knot PKM relationship

Knot PKM already has application-owned browser refresh sessions because it owns signup, onboarding, browser cookies, and product-specific session UX. It does not need to migrate immediately.

If Knot PKM later migrates to Mycel-owned refresh sessions:

1. Add compatibility support that accepts both PKM-owned and Mycel-owned refresh sessions during a transition window.
2. Create Mycel refresh sessions on successful PKM login for users that should migrate.
3. Prefer Mycel `RefreshSession` for sessions that have a Mycel refresh token.
4. Continue accepting existing PKM refresh sessions until their configured idle/absolute expiry.
5. Revoke or expire obsolete PKM-owned refresh-session records server-side after the transition window.
6. Keep PKM-specific browser cookie policy in PKM; MycelDB remains HTTP/cookie agnostic.

Do not copy PKM plaintext refresh tokens into MycelDB. If a session is migrated, mint a new Mycel refresh token and store only the Mycel-generated hash.
