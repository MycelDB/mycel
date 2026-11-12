# Projection materialization operations

This procedure covers read-only inspection and explicit rebuild actions for
common graph, lexical, and semantic materializations. Graph and user-visible
records remain authoritative; a local derived artifact is never promoted by
its existence alone.

## Safety rules

- Do not edit projection metadata, checkpoint files, Raft state, or manifests by
  hand.
- Do not decode or replay opaque graph-change cursors.
- Do not delete graph data to repair a derived index.
- Missing, corrupt, stale, or generation-mismatched artifacts must remain
  unavailable until the owning subsystem rebuilds and republishes a checkpoint.
- Use dry-run or read-only inspection first. Rebuilds are explicit maintenance
  operations and may hold the subsystem's quiesce gate.

## Cluster and readiness inspection

Start with cluster-level readiness:

```sh
mycel cluster status
mycel cluster readiness check
```

If readiness is blocked, record only the bounded blocker names, node/leader
identities, term, and revision values. Do not copy graph payloads, provider
bodies, credentials, or local filesystem paths into incident artifacts.

For a backup or restore qualification, validate the cluster backup set before
extracting any ordinal:

```sh
mycel admin backup cluster validate --backup-set <backup-set>
mycel admin backup cluster restore-plan --backup-set <backup-set>
```

The restore preflight must reject mismatched projection generations, source
revisions, configuration epochs, materializer versions, fencing epochs, or
artifact checksums. A missing local artifact is rebuildable; it must not be
reported ready.

## Graph checkpoint and persisted query indexes

Inspect the local graph checkpoint for one scope:

```sh
mycel cluster graph-checkpoint status \
  --space-id <space-id> \
  --domain-id <domain-id>
```

A graph checkpoint or persisted query-index artifact is local and rebuildable.
Create a checkpoint only as an explicit maintenance action after cluster
readiness is healthy:

```sh
mycel cluster graph-checkpoint create \
  --space-id <space-id> \
  --domain-id <domain-id>
```

Do not infer query-index readiness from a file listing. The common registry
checkpoint, local index validation, source revision, configuration generation,
and current fencing term must agree.

## Lexical projection

Inspect one scope:

```sh
mycel search lexical status --space-id <space-id> --domain <domain-id>
```

Check `state`, `indexed_graph_revision`, `latest_graph_revision`,
`revision_lag`, and segment/tombstone counts. Rebuild only after confirming
that graph data and the owning partition are healthy:

```sh
mycel search lexical rebuild \
  --space-id <space-id> \
  --domain <domain-id> \
  --dry-run

mycel search lexical rebuild \
  --space-id <space-id> \
  --domain <domain-id>
```

Use `--force` only when intentionally replacing an otherwise fresh local
artifact. A rebuild must finish its local validation before the common
checkpoint can advance.

## Semantic projection

Inspect reliability and retained work without triggering provider calls:

```sh
mycel semantic maintenance status --space-id <space-id>
mycel semantic maintenance list \
  --space-id <space-id> \
  --rule <rule-key> \
  --binding <binding-key>
```

A semantic search readiness failure should be classified using the bounded
readiness metadata: leader node/term, semantic generation, rebuild epoch,
fresh-through revision, rebuild state, and enumerated reasons. Do not treat
queue depth or a node-local vector file as readiness evidence.

Use explicit maintenance only after checking the rule, binding, model/vector
configuration, and owning leader:

```sh
mycel semantic rule backfill <rule-key> \
  --space-id <space-id> \
  --binding <binding-key> \
  --limit <bounded-limit>
```

Use `--force` only for an intentional regeneration. If the readiness state is
`rebuild_required` or `failed`, preserve the diagnostic evidence and perform a
complete rebuild/backfill through the admin maintenance path rather than
retrying an opaque cursor.

## Backup and restore evidence

Projection backup catalogs are evidence, not a second authority. During
inspection, verify that each artifact entry agrees with its projection
checkpoint on:

- stable projection identity and scope;
- configuration hash and epoch;
- source revision;
- materializer version;
- fencing epoch;
- artifact generation and checksum.

After restore, wait for the owning partition leader and local materializer to
validate the physical artifact. If any comparison fails, expect a bounded
rebuild-required/unavailable state and use the subsystem-specific rebuild
procedure above.

## Qualification evidence

Retain sanitized suite results, readiness diagnostics, backup/restore manifest
validation, image/source identities, and cleanup/redaction outcomes. Do not
retain raw provider payloads, graph contents, credentials, tokens, or host
paths in release evidence.
