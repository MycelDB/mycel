# mycel admin backup policy set

Update daemon backup policy.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy set \
  --enabled \
  --dir /var/backups/mycel \
  --interval 24h \
  --keep 7 \
  --include-logs=false
```

Flags:

| Flag | Description |
|---|---|
| `--enabled` / `--disabled` | Enable or disable scheduled backups. Manual triggers remain available. |
| `--dir` | Backup directory. Must not equal or be inside `MYCELD_DATA_DIR`; symlinks are resolved for validation. |
| `--interval` | Schedule interval, e.g. `24h`, `30m`, or seconds. |
| `--keep` | Number of newest complete backups to retain. |
| `--include-logs` | Include daemon logs in the archive. |
| `--allow-reads-during-backup` | Reserved safe-read behavior; default false. |
| `--quiesce-timeout` | Maximum time to stop new work and drain active participants. |
| `--backup-timeout` | Maximum time for a backup operation. |
| `--retry-after` | Scheduler retry delay after transient failure/conflict. |
| `--history-limit` | Number of status history entries to retain. |

Defaults include interval `24h`, retention count `7`, compression `zip`, quiesce timeout `2m`, backup timeout `30m`, retry delay `5s`, and history limit `20`.

Use `--output json` for structured output.
