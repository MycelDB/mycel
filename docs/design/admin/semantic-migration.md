# Admin Semantic Migration API

## Status

Closed compatibility surface. The public protobuf service remains available for
one compatibility window, but the daemon returns a closed-window error and no
longer contains the legacy embedding-profile reader or migration implementation.

Protobuf source:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/semantic_migration.proto
```

## Service

```text
mycel.admin.v1.AdminSemanticMigrationService
```

Compatibility RPC:

- `MigrateLegacyEmbeddings`

## Behavior

`MigrateLegacyEmbeddings` authenticates the operator and then returns:

```text
FailedPrecondition: legacy embedding migration window is closed; configure Intelligence Access resources and semantic generation rules directly
```

No automatic migration, repair, restore, merge, rebalance, or authoritative-node
selection is performed.

## Authorization

The compatibility RPC still requires an operator bearer token with:

```text
CAPABILITY_SEMANTIC_SEARCH
```

## Replacement path

Configure current semantic/inference resources directly:

- model endpoints;
- inference models and model endpoint capabilities;
- credentials;
- credential grants;
- semantic generation rules;
- inference policies when required.

After configuration, use Admin Semantic Maintenance backfill to generate current
advanced semantic records for each rule binding.

## CLI

The legacy CLI command remains only as a compatibility wrapper around the closed
RPC:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic migrate legacy-embeddings \
  --space-id '<space-id>' \
  --domain default \
  --dry-run
```

It returns the same closed-window error. Use `inference` and `semantic rule`
commands instead.
