# Admin Inference API

## Status

Implemented daemon-oriented Admin Inference API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/inference.proto
```

## Purpose

Admin inference APIs manage daemon inference catalog/configuration resources used by semantic indexes, semantic search, and graph automations. The API is split into themed services: `AdminInferenceCatalogService`, `AdminInferenceProfileService`, `AdminInferenceCredentialService`, `AdminInferenceGrantService`, `AdminInferencePolicyService`, and `AdminInferenceUsageService`.

mycel owns semantic and embedding infrastructure: embedding model endpoints, embedding model definitions, vector stores, semantic indexes, embedding credentials/grants/policies, and semantic search execution. mycel may understand connector types such as `openai-compatible` or `ollama`, but only for operations mycel owns, primarily `embeddings`.

mycel does not own application chat catalogs, chat prompts, chat tools, conversation UX, or browser-user chat credentials. Applications such as Knot PKM own chat orchestration and may maintain their own chat catalog while using mycel for embeddings and semantic search.

The MVP moves inference package application, safe resource discovery, credentials, credential grants, and inference policies behind daemon gRPC so semantic provisioning and semantic execution can be controlled through the daemon.

## Implemented MVP

`AdminInferenceCatalogService`:

- `ApplyInferencePackage`
- `ListInferencePackages`
- `ListModelEndpoints`
- `ListModels`
- `ListVectorStores`
- `ListModelEndpointCapabilities`
- `SetModelEndpointEnabled`
- `SetVectorStoreEnabled`
- `SetModelEndpointCapabilityEnabled`
- reference-safe hard deletes for endpoints, models, vector stores, and endpoint capabilities

`AdminInferenceCredentialService`:

- `CreateCredential`
- `ListCredentials`
- `SetCredentialStatus`
- `DeleteCredential`

`AdminInferenceGrantService`:

- `CreateCredentialGrant`
- `ListCredentialGrants`
- `ExpireCredentialGrant`
- `DeleteCredentialGrant`

`AdminInferencePolicyService`:

- `CreateInferencePolicy`
- `ListInferencePolicies`
- `ExpireInferencePolicy`
- `DeleteInferencePolicy`

## Not yet implemented

- credential/secret update RPCs beyond create/upsert behavior
- endpoint/model/vector-store full update RPCs outside package application
- semantic backfill/maintenance controls are implemented by Admin Semantic Maintenance, not this service
- general chat/completion execution for applications such as Knot PKM
- application-owned chat catalog/package management

## Authorization

Admin Inference API methods require an operator bearer token and the current semantic/inference admin capability:

```text
CAPABILITY_SEMANTIC_SEARCH
```

This is currently granted by the `semantic_admin` role and bootstrap/system admins.

## CLI

Daemon-backed package application:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference package apply examples/inference/standard-openai-embeddings.json
```

Example packages live under `examples/inference/`. mycel examples should focus on semantic/embedding resources, for example:

- `standard-openai-embeddings.json`

Chat catalogs/packages belong in applications such as Knot PKM, not in mycel.

Daemon-backed discovery:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference package list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model-endpoint list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference vector-store list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference capability list
```

Daemon-backed credentials, grants, and policies:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference credential add openai-key \
  --model-endpoint openai \
  --owner-type system \
  --owner-id daemon \
  --external-ref vault://mycel/openai

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference credential grant openai-key \
  --space-id '<space-id>' \
  --domain default \
  --allow-background-use

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference policy allow \
  --space-id '<space-id>' \
  --domain default \
  --privacy-class third_party
```

Inline `--api-key` material is encrypted by the daemon and requires `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` to be configured. Use `--external-ref` for external secret managers or for daemon deployments without an inline secret encryption key.

Daemon-backed soft cleanup/lifecycle commands:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model-endpoint disable openai
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model-endpoint enable openai
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference vector-store disable mycel-file
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference capability disable '<capability-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference credential revoked openai-key
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference credential grant expire '<grant-id>' --space-id '<space-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference policy expire '<policy-id>' --space-id '<space-id>'
```

Daemon-backed hard-delete commands are reference-safe. Endpoint/model/vector-store/capability deletes fail with `FAILED_PRECONDITION` if semantic indexes, capabilities, credentials, grants, or policy decisions still reference them. Credential deletes fail while grants reference the credential unless `--delete-grants` is set; `--delete-secret` deletes the underlying secret only when it is not shared.

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference credential delete openai-key --delete-grants --delete-secret
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference credential grant delete '<grant-id>' --space-id '<space-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference policy delete '<policy-id>' --space-id '<space-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference capability delete '<capability-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model-endpoint delete openai
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model delete openai/text-embedding-3-small
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference vector-store delete mycel-file
```

After applying a package, daemon-mode semantic index creation can use keys for inference resources:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic index add notes-search \
  --space-id '<space-id>' \
  --domain '<domain-id>' \
  --model-endpoint openai \
  --model openai/text-embedding-3-small \
  --vector-store mycel-file
```

Domain lookup for daemon-mode `semantic index add` still requires a domain UUID until an Admin Domain lookup/list API is added or the CLI performs a standard-user domain lookup separately.
