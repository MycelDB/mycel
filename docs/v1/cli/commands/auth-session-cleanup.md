# auth session cleanup

Clean up expired/revoked durable refresh sessions by marking expired sessions and redacting old token hashes according to auth retention configuration.

```sh
mycel -d ./data -u admin -p change-me auth session cleanup
```

The authenticated user must have system operation permission. Use `--output json` for the cleanup count.
