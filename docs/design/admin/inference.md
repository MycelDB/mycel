# Admin Inference API

## Status

Implemented daemon-oriented Admin Inference API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/inference.proto
```

## Purpose

Admin inference APIs manage daemon inference catalog/configuration resources used by semantic generation rules, semantic search, and graph automations. The API is split into themed services: `AdminInferenceCatalogService`, `AdminInferenceProfileService`, `AdminInferenceCredentialService`, `AdminInferenceGrantService`, `AdminInferencePolicyService`, and `AdminInferenceUsageService`.

mycel owns semantic and embedding infrastructure: embedding model endpoints, embedding model definitions, vector stores, semantic generation rules, embedding credentials/grants/policies, and semantic search execution. mycel may understand connector types such as `openai-compatible` or `ollama`, but only for operations mycel owns, primarily `embeddings`.

mycel does not own application chat catalogs, chat prompts, chat tools, conversation UX, or browser-user chat credentials. Applications such as Knot PKM own chat orchestration and may maintain their own chat catalog while using mycel for embeddings and semantic search.

The MVP moves inference package application, safe resource discovery, credentials, credential grants, and inference policies behind daemon gRPC so semantic provisioning and semantic execution can be controlled through the daemon.

Credential-free reliability suites may opt the `fake` connector into the documented [`token-basis-v1` deterministic fixture strategy](deterministic-inference-fixtures.md). The opt-in metadata, exact vector algorithm, finite scripted outcomes, bounds, accounting, and process-reset semantics are part of that fixture contract; the historical fake default remains unchanged.

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
- `RotateCredential`
- `DeleteCredential`

Credential creation, status changes, secret rotation, and credential/secret deletion are conditional semantic-global lifecycle mutations. Each operation is represented by one WAL/Raft record, re-reads current state in the state machine, preserves a concurrent disabled/revoked/expired status during rotation, verifies the expected endpoint/auth/secret identity, and is idempotent under replay. Before replacing either metadata document, storage atomically persists a lifecycle intent containing the complete prospective credential and secret state. Failed writes leave the prior in-memory state visible; the intent must be completed before any later local WAL record advances, before a system Raft snapshot is captured, or during restart initialization. Recovery also rebuilds the derived inference projection before further global publication. Endpoints referenced by credentials cannot be deleted or updated to remove the credential's auth mode.

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

- credential/secret update RPCs beyond status changes and secret rotation
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
- `deterministic-token-basis-v1.json` (test-only, local fake connector)

Chat catalogs/packages belong in applications such as Knot PKM, not in mycel.

Daemon-backed discovery:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference package list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference endpoint list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference vector-store list
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference capability list
```

Daemon-backed credentials, grants, and policies:

```sh
printf '%s' "$OPENAI_API_KEY" | ./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference credential create openai-key \
  --model-endpoint openai \
  --owner-type system \
  --owner-id daemon \
  --secret-stdin

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference grant openai-key \
  --space-id '<space-id>' \
  --domain default \
  --allow-background-use

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  inference policy allow \
  --space-id '<space-id>' \
  --domain default \
  --privacy-class third_party
```

API key material is encrypted by the daemon using the encryption-at-rest envelope service. Deployments that create inline inference credentials must enable encryption at rest with `MYCELD_ENCRYPTION_*` settings; the old `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` path is no longer accepted. Prefer `--secret-stdin`; `--secret-value` is available for controlled automation but may be captured in shell history. Endpoints declaring credential-free authentication may use `auth_type=none`; those credentials must omit `secret_value`, persist no secret reference, reject secret rotation, and never trigger secret resolution. Credential auth modes must be declared by an existing catalog endpoint. API-key rotation overwrites the existing encrypted secret identity so superseded ciphertext is not retained, and unsupported credential statuses are rejected rather than projected active.

Daemon-backed soft cleanup/lifecycle commands:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference endpoint disable openai
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference endpoint enable openai
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference vector-store disable mycel-file
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference capability disable '<capability-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference credential revoke openai-key
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference grant expire '<grant-id>' --space-id '<space-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference policy expire '<policy-id>' --space-id '<space-id>'
```

Daemon-backed hard-delete commands are reference-safe. Endpoint/model/vector-store/capability deletes fail with `FAILED_PRECONDITION` if semantic generation rules, capabilities, credentials, grants, or policy decisions still reference them. Credential deletes fail while grants reference the credential unless `--delete-grants` is set; `--delete-secret` deletes the underlying secret only when it is not shared.

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference credential delete openai-key --delete-grants --delete-secret
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference grant delete '<grant-id>' --space-id '<space-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference policy delete '<policy-id>' --space-id '<space-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference capability delete '<capability-id>'
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference endpoint delete openai
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference model delete openai/text-embedding-3-small
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' inference vector-store delete mycel-file
```

After applying a package, daemon-mode semantic rule creation can use Intelligence Access profile and vector-store keys:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule create --file examples/semantic/notes-rule.json
```

Domain lookup for daemon-mode semantic rule commands accepts domain UUIDs or stable domain keys such as `default`.
