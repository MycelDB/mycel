# `mycel cluster`

Inspect raft cluster status, health, consistency, forensics, and local graph checkpoints.

Authentication mode: **operator**.

## Common tasks

- Check cluster identity and health.
- List raft groups.
- Check local application-level readiness for Kubernetes probes, including clustered write readiness.
- Run graph consistency reports and local forensic exports.
- Create and inspect local domain graph checkpoints.

## Examples

```sh
mycel --output json cluster status
```

```sh
mycel cluster readiness check
```

In clustered Raft mode the readiness check waits for `client_ready=true`,
`read_ready=true`, and `write_ready=true`. `partition_groups_started=true` only
means the local group processes exist; it does not mean schema or graph writes
can route to Raft leaders yet.

```sh
mycel cluster consistency-report --space-id <space-id> --domain-id <domain-id>
```

```sh
mycel cluster graph-checkpoint create --space-id <space-id> --domain-id <domain-id>
mycel cluster graph-checkpoint status --space-id <space-id> --domain-id <domain-id>
```

Graph checkpoint commands operate on the local daemon only. They create or read a
derived domain graph checkpoint for fast-open/recovery; they are not Raft
snapshots or backups. Checkpoint status also reports local persistent graph index
status under `persistent_index`, including whether the latest index set is
present, whether the last open used it or fell back, and any fallback reason.
When persistent schema/query index payloads are present, JSON output includes
`persistent_index.query_indexes[]` entries with each index name, target kind,
schema hash, definition fingerprint, key encoding version, entry count, and load
result.

Automatic local checkpointing is disabled by default. Enable it with:

```sh
MYCELD_GRAPH_CHECKPOINT_AUTO_ENABLED=true
MYCELD_GRAPH_CHECKPOINT_AUTO_INTERVAL=1m
MYCELD_GRAPH_CHECKPOINT_AUTO_REVISIONS=10000
MYCELD_GRAPH_CHECKPOINT_AUTO_TIMEOUT=30s
```

The automatic policy checks opened local domain stores and writes a checkpoint
when tail revisions since the latest checkpoint reach the configured threshold.

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
- [Cluster readiness contract](../../design/clustering/readiness.md)
