# auth session revoke

Revoke one durable refresh session owned by the authenticated user.

```sh
mycel -d ./data -u admin -p change-me auth session revoke <session_id> --reason "lost device"
```

The command cannot revoke another user's session. A revoked refresh session can no longer be used to mint access tokens.
