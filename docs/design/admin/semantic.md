# Admin Semantic API

## Status

Implemented daemon-oriented Admin Semantic API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/semantic.proto
```

## Purpose

`AdminSemanticService` manages semantic index configuration through the operator-facing Admin API.

The current MVP bridges daemon-backed client semantic search with daemon-managed semantic index definitions.

## Scope

Implemented:

- `ListSemanticIndexes`
- `UpsertSemanticIndex`
- `DeleteSemanticIndex`

`DeleteSemanticIndex` is reference-safe: it fails when credential grants or inference policies are scoped to the index unless `purge_references` is set. `purge_vectors` also removes local mycel-file vector records for the index.

Credential/secret management, credential grants, and inference policies are implemented separately in [Admin Inference API](inference.md).

Legacy embedding migration is closed; the compatibility RPC is documented in [Admin Semantic Migration API](semantic-migration.md).

Semantic backfill and maintenance analyze/process are implemented separately in [Admin Semantic Maintenance API](semantic-maintenance.md).

## Authorization

Admin Semantic API methods require an operator bearer token and the semantic admin capability currently represented by:

```text
CAPABILITY_SEMANTIC_SEARCH
```

The built-in `semantic_admin` operator role grants that capability. Bootstrap/system admins also grant it in the current daemon implementation.

## CLI

Daemon-backed index upsert is available when `--daemon-addr` is supplied:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic index add notes-search \
  --space-id '<space-id>' \
  --domain '<domain-id>' \
  --model-endpoint openai \
  --model openai/text-embedding-3-small \
  --vector-store mycel-file \
  --source subtree
```

With AdminInferenceService and AdminDomainService in place, daemon-mode `semantic index add` can resolve domain/model endpoint/model/vector store by key or UUID.

Reference-safe delete:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic index delete notes-search \
  --space-id '<space-id>' \
  --domain default \
  --purge-references \
  --purge-vectors
```

Client-side listing/search remain:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u alice -p '<password>' \
  semantic index list --space-id '<space-id>' --domain '<domain-id>'
```
