# mycel admin backup status

Show current backup status and schedule timestamps.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup status
```

Text output includes:

- backup state
- current/last backup id
- `last_success_at`
- `next_run_at`

Use JSON for the full `GetBackupStatusResponse`, including quiesce participant status:

```sh
mycel ... --output json admin backup status
```

The backup/status RPCs remain available during backup quiesce after operator authentication, so operators can inspect progress while the daemon is temporarily rejecting other non-exempt RPCs, including reads unless explicitly exempted/proven safe.
