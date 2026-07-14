# mycel admin backup policy set

Update daemon backup policy.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy set \
  --enabled \
  --dir /var/backups/mycel \
  --schedule daily \
  --time-of-day 22:00 \
  --timezone America/Toronto \
  --keep 7 \
  --archive-format zip \
  --include-logs=false
```

Interval schedules are still supported:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy set --schedule interval --interval-hours 24
```

Weekly schedules can specify multiple weekdays:

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup policy set \
  --schedule weekly \
  --time-of-day 02:00 \
  --timezone UTC \
  --weekday Sunday \
  --weekday Wednesday
```

Flags:

| Flag | Description |
|---|---|
| `--enabled` / `--disabled` | Enable or disable scheduled backups. Manual triggers remain available. |
| `--dir` | Backup directory. Must not equal or be inside `MYCELD_DATA_DIR`; symlinks are resolved for validation. |
| `--schedule` | Schedule kind: `interval`, `daily`, or `weekly`. Empty/default is `interval`. |
| `--interval-hours` | Interval period in whole hours for interval schedules, e.g. `24`. |
| `--time-of-day` | Wall-clock backup time for daily/weekly schedules in `HH:MM` 24-hour format. |
| `--timezone` | IANA timezone for daily/weekly schedules, e.g. `UTC` or `America/Toronto`. |
| `--weekday` | Weekday for weekly schedules. Repeat for multiple days. Accepts `sun`..`sat`, full names, or `0`..`6` where `0=Sunday`. |
| `--run-missed` / `--no-run-missed` | Enable or disable running a missed calendar backup after daemon restart. |
| `--keep` | Number of newest complete backups to retain. |
| `--archive-format` | Backup archive format: `zip`, `tar`, `tar.gz`, or `tar.zst`. |
| `--include-logs` | Include daemon logs in the archive. |
| `--allow-reads-during-backup` | Reserved safe-read behavior; default false. |
| `--quiesce-timeout` | Maximum time to stop new work and drain active participants. |
| `--backup-timeout` | Maximum time for a backup operation. |
| `--retry-after` | Scheduler retry delay after transient failure/conflict. |
| `--history-limit` | Number of status history entries to retain. |

Defaults include schedule `interval`, interval `24h`, timezone `UTC`, retention count `7`, archive format `zip`, quiesce timeout `2m`, backup timeout `30m`, retry delay `5s`, and history limit `20`.

Use `--output json` for structured output.
