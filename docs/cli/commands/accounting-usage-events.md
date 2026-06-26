# `mycel accounting usage events`

Lists raw inference usage events from the accounting ledger for audit/debug workflows.

## Example

```sh
mycel accounting usage events --from 2026-06-01 --to 2026-06-30 --user martin --space personal-pkm --limit 100
```

## Notes

Events come from the append-only ledger under `meta/accounting/inference-usage-*.kusage`.
