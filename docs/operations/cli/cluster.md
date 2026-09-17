# `mycel cluster`

Inspect raft cluster status, health, consistency, and forensics.

Authentication mode: **operator**.

## Common tasks

- Check cluster identity and health.
- List raft groups.
- Check local application-level readiness for Kubernetes probes, including clustered write readiness.
- Run graph consistency reports and local forensic exports.

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

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
- [Cluster readiness contract](../../design/clustering/readiness.md)
