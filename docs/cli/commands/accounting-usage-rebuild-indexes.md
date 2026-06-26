# `mycel accounting usage rebuild-indexes`

Rebuilds derived accounting indexes from the authoritative usage ledger. Requires system access management privileges.

## Example

```sh
mycel accounting usage rebuild-indexes
```

## Notes

Indexes are derived from `meta/accounting/inference-usage-*.kusag` and can be rebuilt if missing or corrupt.
