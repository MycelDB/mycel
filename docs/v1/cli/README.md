# MycelDB CLI

`mycel` is a Cobra-based command-line client for the embedded MycelDB engine.

It supports:

- one-shot CLI commands
- interactive REPL mode

Build from the module root:

```sh
cd mycel
make build
```

Or run without installing:

```sh
go run ./cmd/mycel --help
```

Set the shared MycelDB data directory, or pass `-d/--data-dir` on each command:

```sh
export MYCELDB_DATA_DIR=~/mycel_data
```

## Configuration

MycelDB CLI configuration precedence is: built-in defaults, optional YAML file, environment variables, then command-line flags.

Use `--config` or `MYCELDB_CONFIG` to load a YAML file.

Blob upload limits use `-1` for unlimited. Exact MIME overrides can use `0` to disallow that MIME type.

Auth/session environment aliases include `MYCELDB_AUTH_ACCESS_TOKEN_TTL`, `MYCELDB_AUTH_REFRESH_IDLE_TTL`, `MYCELDB_AUTH_REFRESH_ABSOLUTE_TTL`, `MYCELDB_AUTH_REFRESH_AUDIT_RETENTION_TTL`, and `MYCELDB_AUTH_REFRESH_TOKEN_BYTES`.

Other common environment aliases include `MYCELDB_DATA_DIR`, `MYCELDB_USER_STORE_ENCRYPTION_KEY_B64`, and `MYCELDB_STORAGE_BLOBS_MAX_*_BYTES`.

Phase 0 advanced semantic implementation work is gated by `semantic.advanced_enabled`, `MYCELDB_SEMANTIC_ADVANCED_ENABLED`, or `--semantic-advanced-enabled`. The flag defaults to `false` and is intentionally no-op until later phases introduce gated implementation paths.

## Authentication

Initialize a data directory once before running normal commands. See [init](commands/init.md).

All other non-REPL commands require an initialized data directory and credentials unless a command explicitly says otherwise.

## REPL

See [repl](commands/repl.md).

## Command Reference

Each command or command family is documented in its own file under [`commands/`](commands/).

Core commands:

- [init](commands/init.md)
- [repl](commands/repl.md)
- [user add](commands/user-add.md)
- [user list](commands/user-list.md)
- [user delete](commands/user-delete.md)
- [acl grant](commands/acl-grant.md)
- [acl revoke](commands/acl-revoke.md)
- [acl list](commands/acl-list.md)
- [auth session list](commands/auth-session-list.md)
- [auth session revoke](commands/auth-session-revoke.md)
- [auth session revoke-other](commands/auth-session-revoke-other.md)
- [auth session cleanup](commands/auth-session-cleanup.md)
- [space add](commands/space-add.md)
- [space list](commands/space-list.md)
- [space delete](commands/space-delete.md)
- [space set](commands/space-set.md)
- [space unset](commands/space-unset.md)
- [template import](commands/template-import.md)
- [template list](commands/template-list.md)
- [node add](commands/node-add.md)
- [node list](commands/node-list.md)
- [node get](commands/node-get.md)
- [node delete](commands/node-delete.md)

Current MVP embedding commands:

- [embeddings catalog](commands/embeddings-catalog.md)
- [embeddings keys add](commands/embeddings-keys-add.md)
- [embeddings profiles add](commands/embeddings-profiles-add.md)
- [embeddings generate](commands/embeddings-generate.md)
- [embeddings search](commands/embeddings-search.md)

Target advanced semantic/inference commands:

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
