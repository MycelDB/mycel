# Semantic Indexing and Embeddings

This directory documents Mycel's semantic indexing and embedding architecture.

Semantic support is broader than vector generation. It covers how graph content is selected, which inference runtimes/models are allowed, which credentials may be used, how embeddings are refreshed, how semantic queries are planned, and where derived vector records are stored.

## Documents

- [current-mvp.md](current-mvp.md): current manual embeddings subsystem and CLI
- [concepts.md](concepts.md): core advanced semantic concepts and resource model
- [provisioning.md](provisioning.md): package/runtime/model/index provisioning responsibilities and CLI flow
- [credentials.md](credentials.md): credentials, credential grants, and grant resolution
- [policies.md](policies.md): inference/content policies and policy resolution
- [embedding-generation.md](embedding-generation.md): source extraction, dirty work, backfill, and refresh flow
- [query-planning.md](query-planning.md): multi-index and multi-vector-space semantic query planning
- [storage.md](storage.md): semantic storage pointers; detailed filesystem structures live in `docs/storage/semantic.md`

## Current vs Advanced Model

The current MVP exposes low-level embedding primitives:

```text
provider key + embedding profile + manual generate/search commands
```

The advanced model moves toward first-class semantic resources:

```text
InferencePackage
InferenceRuntime
InferenceModel
VectorStoreBackend
InferenceCredential
CredentialGrant
InferencePolicy
SemanticIndex
EmbeddingRecord
```

The key architectural change is that applications should provision and query semantic indexes, not raw embedding profiles.

## Responsibility Split

```text
Mycel library:
  schemas, stores, validation, policy checks, query planning, runtime execution contracts

Mycel CLI/operator:
  inference packages, runtimes, models, vector stores, credentials, space-owned grants, space-owned policies

Application using Mycel:
  graph templates, domains, semantic indexes, source policies, refresh behavior, space provisioning defaults

User/organization/deployment:
  credentials, runtime authorization, privacy constraints, local/third-party policy
```

## Query Direction

Semantic queries are planned over semantic indexes and vector spaces. Credentials are resolved only when Mycel needs to call a runtime.

```text
query scope
  -> applicable semantic indexes
  -> compatible vector-space groups
  -> runtime calls
  -> credential resolution
  -> vector searches
  -> merged/ranked results
```

Credential grants do not define the search space; they authorize runtime calls required by selected indexes.

## Storage

Do not duplicate filesystem details in this directory. Low-level file layouts, JSON structures, and `.kvec` block formats are documented in:

```text
docs/storage/semantic.md
```
