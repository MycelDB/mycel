# Compose User Backup/Restore Validation

## Command

```sh
make test-compose-user-backup-restore
```

Run from the `mycel/` directory.

## What it does

This destructive Docker Compose system integration test delegates to the native
Mycel Lab `compose-user-backup-restore` suite. Mycel Lab owns the Compose
environment lifecycle, backup operation events, run tracking, and artifacts.

It validates user-scoped backup and restore across a fresh cluster lifecycle. It:

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

The target executes `mycel-lab run suite compose-user-backup-restore
--confirm-destructive` from `MYCEL_LAB_ROOT` (default `../mycel-lab`). The suite
uses Mycel Lab Compose driver options to run the Mycel-owned Compose fixture at
`tests/compose/cluster/compose.yml` with fixed local gRPC ports from the Lab
override file. Common local knobs include `MYCEL_LAB_ROOT`, `MYCEL_IMAGE`, and
`MYCELD_CLUSTER_BACKEND_AUTH_TOKEN`.

## How to interpret results

The test passes when the make target exits `0` and all restore verification steps
succeed.

Investigate failures as follows:

- export failure: check user/principal permissions and backup safety filters;
- import conflict: verify the target cluster was wiped as expected;
- missing graph/blob data after restore: inspect Mycel Lab runtime events,
  per-node verification output, and retained artifacts;
- secret leakage assertion failure: treat as security-critical and block release.

## Cleanup

Mycel Lab manages the destructive Compose lifecycle. If interrupted, clean up the
local Compose project before rerunning. The project name is emitted in Mycel Lab
environment artifacts; use the Compose files from the suite and run:

```sh
docker compose -p <project-name> -f tests/compose/cluster/compose.yml down -v --remove-orphans
```
