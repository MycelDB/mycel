# `mycel accounting usage export`

Exports inference usage events. Requires system access management privileges.

## Examples

```sh
mycel accounting usage export --from 2026-06-01 --to 2026-06-30 --format csv --output usage.csv
mycel accounting usage export --from 2026-06-01 --to 2026-06-30 --format json --user martin
```

## Formats

Target formats: `json`, `jsonl`, and `csv`.
