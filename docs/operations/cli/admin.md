# `mycel admin`

Admin management surfaces.

Authentication mode: **principal auth with admin capabilities**.

## Common tasks

- Legacy aliases for principal identity management.
- Create/delete spaces as an admin-capable principal.
- Run repository-backed deployment backup and safe restore-planning commands.
- Inspect and append curated Activity Events with `mycel admin activity`.
- Inspect quota-safe usage accounting snapshots and health with `mycel admin usage`.

## Examples

```sh
mycel --output json admin list
```

```sh
mycel admin backup policy set \
  --default-repository-id repo-prod \
  --capture-provider filesystem_immutable_copy \
  --quiesce-read-profile control_only \
  --keep-last-successful 7 \
  --require-complete-marker
```

```sh
mycel admin backup start \
  --reason "before maintenance" \
  --repository-id repo-prod \
  --archive-format tar.zst \
  --wait
```

```sh
mycel admin backup validate \
  --repository-id repo-prod \
  --backup-set-id backup-set-...
```

```sh
mycel --output json admin backup observability \
  --repository-id repo-prod \
  --include-retention-probe
```

```sh
mycel admin backup restore-plan \
  --repository-id repo-prod \
  --backup-set-id backup-set-... \
  --target-mode new-deployment-bootstrap \
  --identity-mode preserve-backup-id
```

Repository-backed backups are the only public durable daemon backup model.
Standalone daemons are represented as one-node backup sets; clustered
deployments include one artifact per authoritative member plus raft convergence
evidence. Local filesystem paths remain internal scratch/staging/restore targets
only and are not public durable backup artifacts.

Before quiescing writes, a clustered `backup start` waits up to
`--convergence-timeout` for verified raft quorum reads and applied-index evidence.
While a quorum read is missing, `mycel admin backup status` reports an
`raft_quorum_read_unverified` blocker naming the affected group. A timeout fails
without starting capture. After quiescing, the daemon still requires fresh
quorum reads for the capture barriers; a missing read there aborts the backup
and releases quiesce instead of reusing the precheck proof.

`observability` returns credential-free metric rows and evaluated alert
conditions for backup/restore monitoring; the retention probe is read-only and
opt-in because it lists repository objects. `restore-plan` is a safe daemon RPC
that validates repository truth and reports blockers. Destructive restore
execution remains explicit/offline or controller-helper driven.

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

Quota-safe graph metrics include `graph.node_count`, `graph.edge_count`, and
`graph.logical_bytes`. `graph.logical_bytes` is the UTF-8 byte sum of scalar
values inside committed node `payload` maps; it excludes payload keys, labels,
properties, metadata, edge payloads, blob payload bytes, and physical graph store
overhead. It is visible in usage snapshots but is not hard-limit eligible in the
first enforcement slice.

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
readiness after counters match durable metadata. It can also persist an upgrade
backfill. Ordinary counter drift is still reported, not automatically repaired,
so this RPC is not treated as a backup-quiesce read-only call.

### Backfill graph payload bytes after upgrading from v0.25

Start with healthy v0.25 usage counters and a verified pre-upgrade backup.
Do not downgrade a data directory that has materialized the new metric: the
v0.25 daemon does not recognize the persisted counter. Use the pre-upgrade
backup if you need to roll back.

After upgrading, run a space-scoped reconciliation against each daemon
serving usage for that space, once it is caught up. A domain-scoped check
backfills only that domain, not the parent space total.

```sh
mycel admin usage health --space-id <space-id> --include-reconciliation
mycel admin usage get --space-id <space-id> --metric graph.logical_bytes --require-quota-safe
```

Graph writes may resume before backfill. Counts continue to update, but the
daemon withholds `graph.logical_bytes` for affected scopes rather than returning
partial totals or a false zero. A successful backfill reports `action=backfill`
with the scan source, scope and record counts, and before/after values. If a
commit is in flight or accounting changes during the scan, retry when writes
are idle. For a truncated scan or reported drift, investigate the blockers;
neither is evidence of a completed backfill.

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
