# Admin Semantic Migration API

## Status

Implemented daemon-backed MVP for legacy embedding profile migration.

Protobuf source:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/semantic_migration.proto
```

## Service

```text
mycel.admin.v1.AdminSemanticMigrationService
```

Implemented RPCs:

- `MigrateLegacyEmbeddings`

## Purpose

Migrates legacy MVP embedding keys/profiles into the daemon semantic/inference control plane:

- model endpoints
- inference models
- model endpoint capabilities
- inline encrypted inference credentials
- semantic indexes
- background credential grants
- optional allow policies

## Authorization

Requires an operator bearer token with:

```text
CAPABILITY_SEMANTIC_SEARCH
```

## Inputs

`MigrateLegacyEmbeddings` accepts:

- `space_id`
- `domain_id`
- optional `owner_user_id`; defaults to the space owner
- optional legacy `profile_ref` by UUID or name
- `allow_background_use`
- `add_allow_policy`
- `strict`
- `dry_run`
- `limit`

Inline advanced semantic credentials are encrypted by the daemon and require `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` for non-dry-run migration. `dry_run` validates profiles and legacy keys without writing new semantic resources.

## CLI

Daemon mode is used when `--daemon-addr` is supplied:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic migrate legacy-embeddings \
  --space-id '<space-id>' \
  --domain default \
  --dry-run
```

Non-dry-run:

```sh
MYCELD_USER_STORE_ENCRYPTION_KEY_B64='<base64-32-byte-key>' \
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic migrate legacy-embeddings \
  --space-id '<space-id>' \
  --domain default \
  --allow-background-use \
  --add-allow-policy
```

## Limitations

- Currently migrates legacy providers using `openai_embeddings` protocol.
- Non-dry-run migration requires daemon inline secret encryption configuration.
- Existing generated legacy vector records are not copied; use Admin Semantic Maintenance backfill after migration to generate advanced semantic records.
