# mycel admin backup policy get

Show the effective daemon backup policy.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy get
```

Use JSON for automation:

```sh
mycel ... --output json admin backup policy get
```

The policy is daemon-owned and persisted under `meta/backup/policy.json` after Admin API updates. Scheduled backups are disabled by default.

See also: [`docs/v2/design/admin/backup.md`](../../../v2/design/admin/backup.md).
