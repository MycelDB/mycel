# WAL Snapshot Resync Atomic Install Rollback Implementation Plan

## Status

Implemented initial rollback hardening. Snapshot materialized install now backs up replaced files in the operation staging directory and rolls back installed files if install or reload fails. Progress reset and receive-log clear still occur only after successful install/reload.

## Phases

1. Define managed install roots.
2. Build install transaction model.
3. Stage existing data backup.
4. Commit staged snapshot by file/subtree.
5. Rollback on install/reload failure.
6. Integrate with `SnapshotInstaller`.
7. Tests.
8. E2E validation updates.
9. Documentation updates.

## Acceptance criteria

- Snapshot install has rollback for replaced/new materialized files.
- Reload failure rolls back materialized files.
- Progress is not reset on failure.
- Receive log is not cleared on failure.
- Snapshot resync e2e still passes without follower restart.
