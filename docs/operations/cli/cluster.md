# `mycel cluster`

Inspect raft cluster status, health, consistency, and forensics.

Authentication mode: **operator**.

## Common tasks

- Check cluster identity and health.
- List raft groups.
- Check local application-level readiness for Kubernetes probes.
- Run graph consistency reports and local forensic exports.

## Examples

```sh
mycel --output json cluster status
```

```sh
mycel cluster readiness check
```

```sh
mycel cluster consistency-report --space-id <space-id> --domain-id <domain-id>
```

## Related docs

- [CLI index](README.md)
- [Operations](../README.md)
- [Design](../../design/README.md)
