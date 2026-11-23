# `mycel admin`

Admin management surfaces.

Authentication mode: **principal auth with admin capabilities**.

## Common tasks

- Legacy aliases for principal identity management.
- Create/delete spaces as an admin-capable principal.
- Run node-local daemon backup, raft-storage-safe coordinated cluster backup, and principal-scoped backup/restore commands.
- Inspect and append curated Activity Events with `mycel admin activity`.
- Inspect quota-safe usage accounting snapshots and health with `mycel admin usage`.

## Examples

```sh
mycel --output json admin list
```

```sh
mycel admin backup cluster start \
  --reason "before maintenance" \
  --output-dir /mnt/mycel-backups \
  --archive-format tar.zst \
  --wait
```

```sh
mycel admin backup cluster validate --backup-set /mnt/mycel-backups
```

```sh
mycel --output json admin backup cluster observability \
  --repository-id repo-prod \
  --include-retention-probe
```

```sh
mycel admin backup cluster restore-plan --backup-set /mnt/mycel-backups/backup-set-...
```

```sh
mycel admin backup cluster restore-local \
  --backup-set /mnt/mycel-backups/backup-set-... \
  --ordinal 0 \
  --data-dir /data/mycel
```

Coordinated cluster backup creates one backup set with one archive per pod/PVC,
records raft barriers, requires raft freeze/checkpoint evidence in the final
`backup-set.json`, and keeps full-system restore offline/operator-driven.
`observability` returns credential-free metric rows and evaluated alert
conditions for backup/restore monitoring; the retention probe is read-only and
opt-in because it lists repository objects.
`restore-plan` and `restore-local` are local/offline primitives and do not
contact a running daemon; run them only while the target cluster is stopped and
the target data directory is empty.

```sh
mycel admin user-backup validate --file user.tar.zst
```

```sh
mycel admin activity list --category lifecycle
```

```sh
mycel admin usage get \
  --space-id <space-id> \
  --require-quota-safe
```

```sh
mycel admin usage list \
  --mode space_with_domain_breakdown \
  --space-id <space-id>
```

```sh
mycel admin usage health --space-id <space-id>
```

```sh
mycel admin usage health \
  --space-id <space-id> \
  --include-reconciliation
```

```sh
mycel admin usage get \
  --space-id <space-id> \
  --include-diagnostic-physical
```

```sh
mycel admin usage limits upsert \
  --space-id <space-id> \
  --domain-id <domain-id> \
  --metric blob.active_logical_bytes \
  --hard-limit 10737418240 \
  --policy-ref plan/prod-10g
```

```sh
mycel admin usage limits list --space-id <space-id>
mycel admin usage limits delete --limit-id <limit-id>
```

Usage snapshot/list/health reads require `usage.read`; reconciliation health also
requires `usage.reconcile`, and diagnostic physical/collector output requires
`usage.diagnostic.read`. Hard usage limit CRUD requires `usage.limit.manage`.
Health with reconciliation may clear restored `RECONCILING`/`REBUILDING`
readiness after counters match durable metadata, so it is not treated as a
backup-quiesce read-only RPC.

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
