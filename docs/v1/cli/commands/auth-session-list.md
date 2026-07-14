# auth session list

List durable refresh sessions for the authenticated user.

```sh
mycel -d ./data -u admin -p change-me auth session list
```

Use `--output json` for structured output. Output includes coarse session metadata only and never includes refresh tokens or refresh-token hashes.
