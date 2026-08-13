# Start mycel in Standalone Mode

This procedure starts one `myceld` daemon that owns one local data directory and serves gRPC on one address. Standalone mode is useful for local development, smoke tests, and small single-node deployments. It is not a raft cluster and does not provide multi-node replication.

## Prerequisites

Run commands from the `mycel/` repository unless noted otherwise.

Required tools:

- Go toolchain compatible with this repository.
- A sibling `../mycel-api` checkout, or `MYCEL_API_ROOT` pointing at a mycel-api checkout, when using `make` targets that regenerate protobuf stubs.

Choose a data directory that is owned by the daemon and is not shared with another running `myceld` process.

```sh
export MYCELD_DATA_DIR=/tmp/mycel-standalone
export MYCELD_GRPC_ADDR=127.0.0.1:9091
export MYCELD_MODE=standalone
```

Standalone is the default mode, but setting `MYCELD_MODE=standalone` makes the runbook explicit.

## Start with generated bootstrap credentials

For a fresh data directory, mycel creates one active system-admin principal when the identity store is empty. If no bootstrap password is configured, the daemon generates one and writes it once to the daemon log.

```sh
rm -rf "$MYCELD_DATA_DIR"
make start \
  MYCELD_DATA_DIR="$MYCELD_DATA_DIR" \
  MYCELD_GRPC_ADDR="$MYCELD_GRPC_ADDR"
```

Build the CLI used by the smoke-test commands. `make start` builds the daemon binary but does not rebuild `bin/mycel`:

```sh
make build-cli
```

Inspect the daemon log for the generated bootstrap credential:

```sh
grep -n "default standalone principal created" "$MYCELD_DATA_DIR/log/myceld.log"
```

The log entry includes:

- `username`, usually `admin`;
- the generated one-time `password`;
- `change_password_required=true`.

Protect the log file because it may contain the bootstrap password.

## Start with explicit bootstrap credentials

For repeatable local environments, configure both bootstrap variables before the first start:

```sh
rm -rf "$MYCELD_DATA_DIR"
export MYCELD_BOOTSTRAP_ADMIN_USERNAME=admin
export MYCELD_BOOTSTRAP_ADMIN_PASSWORD='change-me-before-real-use'

make start \
  MYCELD_DATA_DIR="$MYCELD_DATA_DIR" \
  MYCELD_GRPC_ADDR="$MYCELD_GRPC_ADDR"
```

If `MYCELD_BOOTSTRAP_ADMIN_USERNAME` is set, `MYCELD_BOOTSTRAP_ADMIN_PASSWORD` must also be set. The configured password is not logged as plaintext; the daemon logs only that configured bootstrap credentials were used.

Build the CLI if you have not already done so:

```sh
make build-cli
```

The bootstrap principal is only created when there is no active system-admin principal. Restarting with an existing identity store does not recreate or overwrite it.

## Start directly without `make start`

You can also build and run the daemon directly:

```sh
make build-daemon build-cli
MYCELD_MODE=standalone \
MYCELD_DATA_DIR="$MYCELD_DATA_DIR" \
MYCELD_GRPC_ADDR="$MYCELD_GRPC_ADDR" \
  ./bin/myceld
```

In another terminal, use `./bin/mycel` for CLI checks.

## Smoke test the daemon

Log in and verify the authenticated principal:

```sh
./bin/mycel \
  --daemon-addr "$MYCELD_GRPC_ADDR" \
  --username admin \
  --password '<bootstrap-password>' \
  auth whoami
```

List principals through the admin principal API:

```sh
./bin/mycel \
  --daemon-addr "$MYCELD_GRPC_ADDR" \
  --username admin \
  --password '<bootstrap-password>' \
  principal list
```

Create an application principal for data-plane work:

```sh
./bin/mycel \
  --daemon-addr "$MYCELD_GRPC_ADDR" \
  --username admin \
  --password '<bootstrap-password>' \
  principal create \
  --principal-username alice \
  --new-password 'alice-password' \
  --login-enabled \
  --role space.admin
```

Then verify that the new principal can authenticate:

```sh
./bin/mycel \
  --daemon-addr "$MYCELD_GRPC_ADDR" \
  --username alice \
  --password 'alice-password' \
  auth whoami
```

## Stop the daemon

If started with `make start`, stop it with the matching data directory:

```sh
make stop MYCELD_DATA_DIR="$MYCELD_DATA_DIR"
```

If started directly in a terminal, stop it with `Ctrl-C`.

## Important operational notes

- Do not run multiple `myceld` processes against the same standalone data directory.
- Do not copy a live data directory for backup. Use daemon-owned backup commands; see [Backup and restore](backup-restore.md).
- Full-system restore is offline/operator-driven: stop `myceld`, restore into an empty data directory, then start `myceld` against that directory.
- Standalone mode is not a raft cluster. To validate raft behavior, use the cluster procedures instead of this runbook.
- For real deployments, configure TLS/mTLS and keep bootstrap credentials out of shell history where possible.
