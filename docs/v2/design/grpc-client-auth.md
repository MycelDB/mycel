# gRPC Client Auth API

Mycel v2 exposes standard-user authentication through the daemon-owned Client API:

```text
mycel.client.v1.AuthService
```

This is distinct from the Admin API operator login. Operators authenticate to manage the daemon; standard users authenticate to use client APIs.

## Authentication model

- `Login` authenticates daemon-managed users from `internal/daemon/modules/user`.
- Disabled and deleted users cannot log in.
- `Refresh` rotates a durable refresh session and returns a new access token plus replacement refresh token.
- Access tokens are short-lived daemon HMAC tokens with user principals.
- Refresh sessions are durable records stored under:

```text
<MYCELD_DATA_DIR>/users/sessions/refresh_sessions.json
```

## RPCs

Implemented:

```text
Login
Refresh
Logout
WhoAmI
ListAuthSessions
RevokeAuthSession
RevokeOtherAuthSessions
```

`Login` and `Refresh` are unauthenticated. Other methods require:

```text
authorization: Bearer <user-access-token>
```

Admin APIs reject user access tokens and continue to require operator access tokens.

## CLI

The CLI `auth` commands now use daemon gRPC for supported auth flows:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p pass auth login
./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p pass auth whoami
./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p pass auth session list
./bin/mycel --daemon-addr 127.0.0.1:9091 auth refresh --refresh-token '<refresh-token>'
```

`auth session cleanup` is not exposed over daemon gRPC yet.
