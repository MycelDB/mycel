# Split-Brain Recovery Procedure

This procedure is for operator-selected trusted-source recovery. mycel does
not automatically repair divergent PVCs or choose an authoritative pod.

## High-level workflow

1. Stop writes or otherwise quiesce the affected deployment.
2. Inspect cluster diagnostics and forensic exports.
3. Select an authoritative source pod/node explicitly.
4. Export user-scoped backups from that source.
5. Validate all archives.
6. Recreate a fresh cluster or otherwise quarantine/delete divergent PVCs.
7. Restore selected users into the fresh target cluster.
8. Validate user, space, domain, graph, blob, and cluster consistency.

## Important constraints

- Do not assume deleting pods alone removes divergent data; PVCs must be handled
  explicitly.
- Do not merge divergent PVCs automatically.
- Do not export or restore plaintext passwords or active sessions.
- Use dry-run restore plans before `--execute`.

Related procedures:

- [Backup and restore](backup-restore.md)
- [Raft manual repair workflows](raft-cluster-manual-repair-workflows.md)
- [Raft cluster operations](raft-cluster-operations.md)
