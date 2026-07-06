# MycelDB CLI

`mycel` is a Cobra-based command-line client for the `myceld` daemon. The old
embedded-engine CLI mode (`-d/--data-dir`, `MYCELDB_DATA_DIR`, local engine
initialization) has been removed.

Build from the module root:

```sh
cd mycel
make build
```

Or run without installing:

```sh
go run ./cmd/mycel --help
```

## Configuration

The CLI connects to a running daemon. Connection settings use `MYCELD_*` daemon
connection environment variables and matching flags, for example:

```sh
export MYCELD_GRPC_ADDR=127.0.0.1:9091
mycel --daemon-addr "$MYCELD_GRPC_ADDR" --help
```

Optional CLI configuration is loaded from `--config` or `MYCEL_CONFIG`.

`myceld`, not the CLI, owns runtime/storage configuration such as
`MYCELD_DATA_DIR` and daemon TLS settings.

## Authentication

Admin APIs require operator/admin credentials. Client APIs require a standard
user login/session. Initialize and run `myceld` before using normal commands.

## REPL

See [repl](commands/repl.md).

## Command Reference

Each command or command family is documented in its own file under
[`commands/`](commands/). Some v1 command pages describe historical embedded
flows; prefer v2 design docs and current `mycel --help` output when in doubt.

Removed MVP embedding-profile commands:

- The old `mycel embeddings ...` command tree has been removed. Use daemon-backed semantic indexes, inference credentials/grants, semantic maintenance, and `semantic search` instead. Existing legacy profile/key data can be converted with [semantic migrate legacy embeddings](commands/semantic-migrate-legacy-embeddings.md) while that migration path remains available.

Daemon semantic/inference commands include:

- [inference package apply](commands/inference-package-apply.md)
- [inference capability add](commands/inference-capability-add.md)
- [inference credential add](commands/inference-credential-add.md)
- [inference credential grant](commands/inference-credential-grant.md)
- [inference policy allow](commands/inference-policy-allow.md)
- [inference policy deny](commands/inference-policy-deny.md)
- [inference policy restrict](commands/inference-policy-restrict.md)
- [semantic index add](commands/semantic-index-add.md)
- [semantic index backfill](commands/semantic-index-backfill.md)
- [semantic search](commands/semantic-search.md)
- [semantic maintenance analyze](commands/semantic-maintenance-analyze.md)
- [semantic maintenance process](commands/semantic-maintenance-process.md)
- [semantic migrate legacy embeddings](commands/semantic-migrate-legacy-embeddings.md)

Accounting commands:

- [accounting usage summarize](commands/accounting-usage-summarize.md)
- [accounting usage events](commands/accounting-usage-events.md)
- [accounting usage export](commands/accounting-usage-export.md)
- [accounting usage rebuild-indexes](commands/accounting-usage-rebuild-indexes.md)
- [accounting usage rebuild-rollups](commands/accounting-usage-rebuild-rollups.md)

## JSON Output

Add `--output json` to commands that support structured output.
