# Compose User Backup/Restore Validation

## Command

```sh
make test-compose-user-backup-restore
```

Run from the `mycel/` directory.

## What it does

This destructive Docker Compose system integration test validates user-scoped
backup and restore across a fresh cluster lifecycle. It:

1. starts or resets a local Compose cluster;
2. creates fixtures for multiple users/principals;
3. exports principal-scoped backups;
4. verifies backup contents and safety expectations;
5. wipes and recreates the cluster;
6. imports backups into the fresh cluster;
7. verifies restored spaces, domains, graph data, and blob payloads through every
   node.

Backups are explicit operator tooling. The test must not export plaintext
passwords or active sessions/tokens.

## Parameters

The target executes `scripts/testComposeUserBackupRestore.sh`. By default it uses the Mycel-owned Compose fixture at `tests/compose/cluster/compose.yml`. Common local knobs include `MYCEL_COMPOSE_FILE`, `MYCEL_COMPOSE_SERVICES`, `MYCEL_IMAGE`, and `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN`.

## How to interpret results

The test passes when the make target exits `0` and all restore verification steps
succeed.

Investigate failures as follows:

- export failure: check user/principal permissions and backup safety filters;
- import conflict: verify the target cluster was wiped as expected;
- missing graph/blob data after restore: inspect per-node verification output and
  retained script artifacts;
- secret leakage assertion failure: treat as security-critical and block release.

## Cleanup

The script manages its own destructive Compose lifecycle. If interrupted, clean up the local Compose cluster before rerunning:

```sh
docker compose -f tests/compose/cluster/compose.yml down -v --remove-orphans
```
