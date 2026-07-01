# auth session revoke-other

Revoke all other durable refresh sessions owned by the authenticated user while preserving one current session.

```sh
mycel -d ./data -u admin -p change-me auth session revoke-other --current-session-id <session_id> --reason "rotate devices"
```

The `--current-session-id` value must identify a session owned by the authenticated user.
