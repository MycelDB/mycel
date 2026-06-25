# Semantic Concepts

This document defines the core concepts in the advanced semantic indexing model.

## Inference Package

A declarative, versioned bundle of inference definitions.

An inference package may contain:

- model endpoint definitions
- model metadata
- vector-store definitions
- optional semantic index templates

It must not contain secret values.

Example package intent:

```text
standard-openai:
  model_endpoint: openai-public
  models: text-embedding-3-small, text-embedding-3-large
```

Packages are explicitly applied by an operator or application; Mycel should not silently enable vendor-specific definitions.

## Model Endpoint

A configured, reachable service endpoint for AI model operations.

It answers:

> Where can Mycel reach a model, and which connector should it use?

Examples:

- OpenAI public API
- Azure OpenAI deployment
- local Ollama instance
- company-private OpenAI-compatible gateway
- local sentence-transformers service

A model endpoint is not a model, credential, semantic index, or vector store.

## Connector Type

`ConnectorType` is the static, code-backed adapter/protocol family Mycel uses to call a model endpoint.

It answers:

> Which connector implementation should Mycel use for requests, auth shape, response parsing, errors, and operation semantics?

Conceptual enum:

```go
type ConnectorType string

const (
    ConnectorOpenAICompatible ConnectorType = "openai-compatible"
    ConnectorAnthropic        ConnectorType = "anthropic"
    ConnectorOllama           ConnectorType = "ollama"
    ConnectorAzureOpenAI      ConnectorType = "azure-openai"
    ConnectorBedrock          ConnectorType = "bedrock"
    ConnectorCustomHTTP       ConnectorType = "custom-http"
    ConnectorLocalProcess     ConnectorType = "local-process"
)
```

Connector types are known by the platform because each one requires connector code. Model endpoint instances are provisioned data that reference one connector type.

For example, OpenRouter can usually be modeled as:

```text
connector_type = openai-compatible
model_endpoint_key = openrouter
endpoint_url       = https://openrouter.ai/api/v1
```

unless OpenRouter-specific behavior becomes large enough to justify a dedicated `openrouter` connector type.

Conceptual model endpoint fields:

```text
id
key
name
connector_type        # ConnectorType enum value
endpoint_url
network_class         # local, private_network, external_https
privacy_class         # local_only, enterprise_private, third_party
auth_modes            # api_key, bearer_token, none, service_account
operations            # embeddings, chat, rerank, classify
enabled
metadata
created_at / updated_at
```

## Inference Model

Metadata for a model that can be executed by compatible model endpoints.

For embeddings, model metadata must identify vector compatibility.

Conceptual fields:

```text
id
key                   # e.g. openai/text-embedding-3-small
operation             # embeddings, chat, rerank
model_name            # model name sent to the endpoint by default
connector_types       # compatible ConnectorType values
dimensions
modality              # text, image, audio, multimodal
vector_space_key
metadata
created_at / updated_at
```

`vector_space_key` is an opaque string and is authoritative for embedding comparability:

```text
same vector_space_key      => vectors are directly comparable
different vector_space_key => vectors are not directly comparable
```

Mycel should validate only that `vector_space_key` is non-empty for embedding models. It should not parse provider, version, or dimensions out of the key initially.

Each `InferenceModel` owns exactly one `vector_space_key`. Capabilities must not override it.

If dimensions, vector-space behavior, or model behavior differs, create a separate `InferenceModel` with its own `vector_space_key`.

If a vendor changes a model behind the same public model name and the change is material, represent that as a new `InferenceModel`. Mycel should not silently reinterpret old embeddings as belonging to the new model.

## Model Endpoint Capability

A capability states that one model endpoint can serve one inference model for one operation.

Connector compatibility is not enough. A model endpoint using the `ollama` connector does not necessarily have every Ollama model installed, and an `openai-compatible` endpoint may expose a private model catalog.

Capability records are required. A semantic index binding is valid only when an enabled capability exists for:

```text
model_endpoint_id + model_id + operation
```

Capabilities are global under `meta/inference/` because they describe technical availability between global endpoint and model definitions. Spaces do not override capabilities; spaces control use through semantic indexes, credential grants, and inference policies.

Mycel trusts provisioned capability definitions. It should not automatically probe endpoints during startup or planning. Optional verification commands can be added later.

Conceptual fields:

```text
id
model_endpoint_id
model_id
operation
enabled
model_name_override
metadata
created_at / updated_at
```

`model_name_override` supports cases where the logical model key differs from the name that must be sent to a particular endpoint, while still producing the same model/vector space.

Capabilities must not override dimensions or vector-space identity. If dimensions, vector space, or model behavior differs, define a separate `InferenceModel`.

## Semantic Index Model Changes

Model changes should not automatically mutate or version existing semantic indexes.

When a model changes materially:

1. provision a new `InferenceModel`
2. provision a new `ModelEndpointCapability`
3. create a new semantic index using the new model
4. backfill the new semantic index
5. switch queries/application defaults to the new index when ready
6. retire the old semantic index explicitly

This avoids mixing incompatible vector spaces and keeps migration/audit behavior explicit.

## Vector Store Type

`VectorStoreType` is the static, code-backed vector storage/search backend type.

Conceptual enum:

```go
type VectorStoreType string

const (
    VectorStoreMycelFile  VectorStoreType = "mycel-file"
    VectorStoreQdrant     VectorStoreType = "qdrant"
    VectorStorePgVector   VectorStoreType = "pgvector"
    VectorStorePinecone   VectorStoreType = "pinecone"
    VectorStoreWeaviate   VectorStoreType = "weaviate"
    VectorStoreChroma     VectorStoreType = "chroma"
    VectorStoreCustomHTTP VectorStoreType = "custom-http"
)
```

`mycel-file` should be built in and initialized as the default local vector store instance by `mycel init`.

## Vector Store Backend

A configured backend instance where vectors are stored and searched.

Examples:

- Mycel embedded file vector store
- Qdrant
- pgvector
- Pinecone
- Weaviate
- custom remote vector API

Conceptual fields:

```text
id
key
name
type                  # VectorStoreType enum value
config                # non-secret backend config
privacy_class
enabled
created_at / updated_at
```

## Inference Credential

Authorization material for one provisioned model endpoint.

A credential answers:

> Which secret can be used to call this model endpoint?

A credential does not decide what content may be processed and does not define a semantic index.

See [credentials.md](credentials.md).

## Credential Grant

An atomic authorization statement:

> This one credential may be used for these operation(s) in this processing scope.

The cardinality is:

```text
one grant -> one credential -> one scope
```

See [credentials.md](credentials.md).

## Inference Policy

A rule controlling whether graph content may be processed by model endpoints.

It answers:

> Is this content allowed to leave the graph and be processed by this kind of model endpoint for this operation?

Policies can block all inference, restrict content to local model endpoints, or forbid specific operations.

See [policies.md](policies.md).

## Semantic Index

A domain-scoped semantic view over graph content.

It describes:

- what graph content is selected
- how source text is extracted
- which endpoint/model/vector-store binding is used
- how refresh/backfill should happen
- which processing policies apply

Examples:

```text
notes-search
tasks-search
chat-rag
page-title-search
private-local-notes
```

Conceptual fields:

```text
id
space_id
domain_id
key
name
purpose               # semantic_search, chat_rag, task_search, autocomplete
enabled
source_policy
binding               # model_endpoint_id, model_id, vector_store_id
refresh_policy
processing_policy_ref
state
created_at / updated_at
```

The semantic index binding intentionally does not include an API key. Credential grants authorize model endpoint calls when required.

## Source Policy

Selects source roots and defines how text is extracted from each root.

A source policy has two main parts:

```text
root_query   # which nodes become source roots
extraction   # how text is assembled from each root
```

### Root Query

`root_query` is a query-like expression. It replaces ambiguous flat selector semantics such as "are tags ANDed or ORed?" with explicit boolean structure.

Example:

```yaml
root_query:
  and:
    - template_key:
        in: [logseq.journal, logseq.page]
    - not:
        tag:
          has: archived
```

The root query selects source root candidates. Any node matching `root_query` can be a source root, but effective roots do not nest for a single semantic index.

If a candidate root is contained within another candidate root for the same semantic index, the ancestor root wins and the descendant is not a separate effective root. This keeps source roots non-overlapping and avoids duplicate subtree embeddings.

### Extraction

`extraction` defines how source text is assembled from each effective root.

Conceptual fields:

```text
mode                   # self, subtree, custom
edge_kind              # usually contains
max_depth              # 0 can mean unlimited by convention
include_root
include_node_content
include_props
derived_sources        # blob_text, transcript, ocr, caption; explicit and deferred from MVP
minimum_text_length
```

For `self`, only the root node content and selected props are included.

For `subtree`, traversal starts at the effective root and follows containment descendants. If traversal enters a descendant subtree whose effective inference policy disallows the semantic index's endpoint/model, analysis of that subtree stops and its content is not included.

Inference policies override source extraction. Source policy describes desired extraction; policy decides what is allowed.

## Embedding Record

A derived vector record produced for a node/source under a semantic index.

Conceptual fields:

```text
id
space_id
domain_id
semantic_index_id
node_id
model_endpoint_id
model_id
vector_store_id
credential_id
credential_grant_id
policy_decision_id
source_mode
source_hash
vector_space_key
dimensions
vector_ref or inline vector
created_at
```

Search should ignore stale records by choosing the latest logical record for a node/index/source identity.

## Migration From Current Model

| Current Concept | Advanced Concept |
| --- | --- |
| embedding provider key | inference credential |
| embedding profile | semantic index source policy + endpoint/model binding |
| profile provider/model/source | semantic index binding/source policy |
| no equivalent | model endpoint definition |
| no equivalent | inference package |
| no equivalent | credential grant |
| no equivalent | inference/content policy |
| embedding record profile ID | embedding record semantic index ID |
| `embeddings generate` | semantic index backfill/refresh |
| `embeddings search` | semantic search over one or more semantic indexes |
