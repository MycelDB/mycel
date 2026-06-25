# Semantic Concepts

This document defines the core concepts in the advanced semantic indexing model.

## Inference Package

A declarative, versioned bundle of inference definitions.

An inference package may contain:

- runtime definitions
- model metadata
- vector-store definitions
- optional semantic index templates

It must not contain secret values.

Example package intent:

```text
standard-openai:
  runtime: openai-public
  models: text-embedding-3-small, text-embedding-3-large
```

Packages are explicitly applied by an operator or application; Mycel should not silently enable vendor-specific definitions.

## Inference Runtime

A configured execution backend for AI operations.

It answers:

> Where and how does Mycel execute an AI operation?

Examples:

- OpenAI public API
- Azure OpenAI deployment
- local Ollama instance
- company-private OpenAI-compatible gateway
- local sentence-transformers service

A runtime is not a model, credential, semantic index, or vector store.

## Connector Type

`ConnectorType` is the static, code-backed adapter/protocol family Mycel uses to call an inference runtime.

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

Connector types are known by the platform because each one requires connector code. Runtime instances are provisioned data that reference one connector type.

For example, OpenRouter can usually be modeled as:

```text
connector_type = openai-compatible
runtime key    = openrouter
endpoint       = https://openrouter.ai/api/v1
```

unless OpenRouter-specific behavior becomes large enough to justify a dedicated `openrouter` connector type.

Conceptual runtime fields:

```text
id
key
name
connector_type        # ConnectorType enum value
endpoint
network_class         # local, private_network, external_https
privacy_class         # local_only, enterprise_private, third_party
auth_modes            # api_key, bearer_token, none, service_account
operations            # embeddings, chat, rerank, classify
enabled
metadata
created_at / updated_at
```

## Inference Model

Metadata for a model that can be executed by compatible runtimes.

For embeddings, model metadata must identify vector compatibility.

Conceptual fields:

```text
id
key                   # e.g. openai/text-embedding-3-small
operation             # embeddings, chat, rerank
model_name            # provider/runtime model name
connector_types       # compatible ConnectorType values
dimensions
modality              # text, image, audio, multimodal
vector_space_key
metadata
created_at / updated_at
```

Embeddings from incompatible `vector_space_key` values must not be compared directly.

## Vector Store Backend

A configured backend where vectors are stored and searched.

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
type                  # mycel-file, qdrant, pgvector, pinecone, custom-http
config                # non-secret backend config
privacy_class
enabled
created_at / updated_at
```

## Inference Credential

Authorization material for one provisioned runtime.

A credential answers:

> Which secret can be used to call this runtime?

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

A rule controlling whether graph content may be processed by inference runtimes.

It answers:

> Is this content allowed to leave the graph and be processed by this kind of runtime for this operation?

Policies can block all inference, restrict content to local runtimes, or forbid specific operations.

See [policies.md](policies.md).

## Semantic Index

A domain-scoped semantic view over graph content.

It describes:

- what graph content is selected
- how source text is extracted
- which runtime/model/vector-store binding is used
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
binding               # runtime_id, model_id, vector_store_id
refresh_policy
processing_policy_ref
state
created_at / updated_at
```

The semantic index binding intentionally does not include an API key. Credential grants authorize runtime calls when required.

## Source Policy

Selects graph content and defines the text extraction mode.

Conceptual fields:

```text
template_keys
tags
property_selectors
source_mode            # self, subtree, custom
max_depth
include_props
minimum_text_length
```

## Embedding Record

A derived vector record produced for a node/source under a semantic index.

Conceptual fields:

```text
id
space_id
domain_id
semantic_index_id
node_id
runtime_id
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
| embedding profile | semantic index source policy + runtime/model binding |
| profile provider/model/source | semantic index binding/source policy |
| no equivalent | inference runtime definition |
| no equivalent | inference package |
| no equivalent | credential grant |
| no equivalent | inference/content policy |
| embedding record profile ID | embedding record semantic index ID |
| `embeddings generate` | semantic index backfill/refresh |
| `embeddings search` | semantic search over one or more semantic indexes |
