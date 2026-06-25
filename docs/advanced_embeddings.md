# Advanced Embeddings Design

Status: design draft  
Branch: `improved_embedding_support`

## Purpose

Mycel should support semantic querying without baking specific commercial AI providers, models, endpoints, or application-level indexing assumptions into the core library.

The library should own the structure, validation rules, execution contracts, and persistence model. Runtime/model/index definitions should be provisioned explicitly by operators, applications, or installed definition packages.

This document proposes a model for:

- inference runtime definitions
- inference packages
- embedding models
- user/API credentials
- credential grants
- inference/content processing policies
- vector store backends
- semantic indexes
- embedding records
- provisioning and query flows

## Design Principles

1. **Mycel owns schema and behavior, not default vendor choices.**
   Mycel should define and validate inference/runtime/index structures, but should not automatically enable OpenAI, Ollama, Azure, or other providers for every installation.

2. **Definitions are provisioned.**
   Operators or applications provision runtime/model/vector-store definitions through the CLI, API, or package manifests.

3. **Credentials are separate from runtime definitions.**
   Runtime definitions describe how to call a service. Credentials authorize a principal to use that runtime.

4. **Semantic indexes are first-class resources.**
   Applications should query semantic indexes, not raw embedding profiles.

5. **Policies must be explicit.**
   Privacy, network, runtime, credential, and content-processing policies must be modeled so Mycel can prevent unsafe processing paths. Credential grants authorize use of a secret; inference policies decide whether graph content may be processed at all.

6. **One semantic query may require multiple vector searches.**
   If selected indexes use incompatible models/vector spaces, Mycel should embed the query once per compatible group and merge results.

## Core Definitions

### Inference Package

An inference package is a declarative, versioned bundle of definitions.

It can contain:

- inference runtime definitions
- embedding/chat/rerank model metadata
- vector store backend definitions
- optional semantic index templates

It must not contain secret values.

Example conceptual manifest:

```yaml
apiVersion: mycel.io/v1
kind: InferencePackage
metadata:
  name: standard-openai
  version: 2026.06
spec:
  runtimes:
    - key: openai-public
      type: openai-compatible
      endpoint: https://api.openai.com/v1
      networkClass: external_https
      privacyClass: third_party
      authModes:
        - api_key
      operations:
        - embeddings
        - chat

  models:
    - key: openai/text-embedding-3-small
      runtimeTypes:
        - openai-compatible
      operation: embeddings
      model: text-embedding-3-small
      dimensions: 1536
      modality: text
      vectorSpace: openai/text-embedding-3-small
```

The package is applied explicitly:

```sh
mycel inference package apply standard-openai.yaml
```

or eventually:

```sh
mycel inference package install standard-openai
```

### Inference Runtime

An inference runtime is a configured execution backend for AI operations.

It answers:

> Where and how does Mycel execute an AI operation?

Examples:

- OpenAI public API
- Azure OpenAI deployment
- local Ollama instance
- company-private OpenAI-compatible gateway
- local sentence-transformers service
- custom HTTP embedding service

A runtime is not a model, not a credential, and not a vector store.

Conceptual structure:

```go
type InferenceRuntime struct {
    ID           RuntimeID
    Key          string
    Name         string
    Type         RuntimeType // openai-compatible, ollama, custom-http, local-process, etc.
    Endpoint     string
    NetworkClass NetworkClass // local, private_network, external_https
    PrivacyClass PrivacyClass // local_only, enterprise_private, third_party
    AuthModes    []AuthMode   // api_key, bearer_token, none, service_account
    Operations   []Operation  // embeddings, chat, rerank, classify
    Enabled      bool
    Metadata     map[string]string
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```

The runtime definition may be provisioned from a package, CLI command, or application bootstrap.

### Inference Model

An inference model describes a model that can be executed by compatible runtimes.

For embeddings, model metadata must include enough information to understand vector compatibility.

Conceptual structure:

```go
type InferenceModel struct {
    ID             ModelID
    Key            string // e.g. openai/text-embedding-3-small
    Operation      Operation
    ModelName      string // provider/runtime model string
    RuntimeTypes   []RuntimeType
    Dimensions     int
    Modality       Modality // text, image, audio, multimodal
    VectorSpaceKey string
    Metadata       map[string]string
    CreatedAt      time.Time
    UpdatedAt      time.Time
}
```

Embeddings from incompatible `VectorSpaceKey` values must not be compared directly.

### Inference Credential

An inference credential stores authorization material for one provisioned runtime.

A credential answers:

> Which secret can be used to call this runtime?

A credential does **not** describe what content may be processed and does **not** define a semantic index. It is only the secret-bearing resource used by runtime calls.

Credentials are owned by a principal. The default BYOK case is user-owned credentials, but other owner types are required for hosted, team, and enterprise deployments.

Possible owner principal types:

- user
- space
- organization/tenant
- system/deployment

A user can own many credentials, including multiple credentials for the same runtime.

Examples:

- Martin's personal OpenAI key
- Martin's work OpenAI key
- an organization Azure OpenAI key
- a system credential for an enterprise-private gateway
- a local runtime token

Conceptual structure:

```go
type InferenceCredential struct {
    ID          CredentialID
    Key         string
    Name        string
    RuntimeID   RuntimeID
    OwnerType   PrincipalType // user, space, organization, system
    OwnerID     string
    AuthType    AuthType // api_key, bearer_token, none, service_account, etc.
    SecretRef   string   // reference into encrypted secret storage
    Status      CredentialStatus // active, revoked, expired, disabled
    IsDefault   bool
    CreatedAt   time.Time
    UpdatedAt   time.Time
    LastUsedAt  *time.Time
}
```

Secret values should be stored separately in an encrypted secret store. Credential metadata should reference secret IDs, and embedding/query audit records should reference credential IDs or grant IDs. Runtime definitions and semantic indexes must never contain raw secret values.

#### Credential Resolution Is Not Index Selection

A semantic query is planned over semantic indexes and vector spaces, not over credential grants.

Credential resolution happens only after Mycel has determined that it needs to call a runtime, for example to:

- generate content embeddings during backfill/refresh
- generate query embeddings for a selected vector space
- call a chat/rerank/summarization runtime

The query-planning direction is:

```text
query scope -> applicable semantic indexes -> vector spaces -> runtime calls -> credential resolution
```

It is not:

```text
query scope -> credential grants -> indexes
```

### Credential Grant

Credential ownership alone is not enough. Mycel also needs to know where a credential is authorized to be used.

A credential grant is an atomic authorization statement:

> This one credential may be used for these operation(s) in this processing scope.

The cardinality should be:

```text
one grant -> one credential -> one scope
```

Many grants may target the same scope, and one credential may have many grants. Keeping grants atomic improves revocation, auditing, priority/default handling, and usage tracking.

Example:

```yaml
credential: martin-openai
scope:
  space: Personal PKM
  domain: personal-pkm
  semanticIndex: notes-search
operations:
  - embeddings
runtime: openai-public
model: openai/text-embedding-3-small
```

Conceptual structure:

```go
type CredentialGrant struct {
    ID           CredentialGrantID
    CredentialID CredentialID
    Scope        ProcessingScope
    Operations   []Operation

    RuntimeID    *RuntimeID // optional constraint; normally recommended
    ModelID      *ModelID   // optional constraint
    Priority     int
    IsDefault    bool

    GrantedBy    PrincipalRef
    CreatedAt    time.Time
    ExpiresAt    *time.Time
}

type ProcessingScope struct {
    SpaceID            *space.ID
    DomainID           *graph.DomainID
    SemanticIndexID    *SemanticIndexID
    NodeID             *graph.NodeID
    IncludeDescendants bool
}
```

This supports BYOK, shared spaces, enterprise credentials, revocation, and auditing.

#### Credential Grant Resolution

When a runtime call requires a credential, Mycel should resolve applicable grants using the processing scope, operation, runtime, and model.

Recommended specificity order:

1. node/subtree grant
2. semantic index grant
3. domain grant
4. space grant
5. owner default credential for the runtime
6. organization/system default credential for the runtime

Rules:

- the grant operation must match the requested operation
- the grant runtime/model constraints must match when present
- expired, revoked, disabled, or inaccessible credentials are ignored
- the most specific compatible grant wins
- same-specificity conflicts should error unless exactly one grant is default or has highest priority
- credential grants never override content inference policy restrictions

Credential resolution should return both the selected credential and the grant that authorized its use so embedding/query records can be audited.

### Vector Store Backend

A vector store backend describes where vectors are stored and searched.

Examples:

- Mycel embedded file vector store
- Qdrant
- pgvector
- Pinecone
- Weaviate
- custom remote vector API

Conceptual structure:

```go
type VectorStoreBackend struct {
    ID           VectorStoreID
    Key          string
    Name         string
    Type         VectorStoreType // mycel-file, qdrant, pgvector, pinecone, custom-http
    Config       map[string]string
    PrivacyClass PrivacyClass
    Enabled      bool
    CreatedAt    time.Time
    UpdatedAt    time.Time
}
```

Mycel may provide an embedded vector store implementation, but installations should still provision or enable the backend explicitly.

### Semantic Index

A semantic index is a first-class, domain-scoped resource that describes what graph content is embedded, how it is embedded, where it is stored, how it is refreshed, and which policies apply.

It answers:

> What semantic view over this graph/domain exists for querying or retrieval?

Examples:

- `notes-search`
- `tasks-search`
- `chat-rag`
- `page-title-search`
- `private-local-notes`

Conceptual structure:

```go
type SemanticIndex struct {
    ID             SemanticIndexID
    SpaceID        space.ID
    DomainID       graph.DomainID
    Key            string
    Name           string
    Purpose        SemanticIndexPurpose // semantic_search, chat_rag, task_search, autocomplete
    Enabled        bool
    SourcePolicy   SourcePolicy
    Binding        IndexRuntimeBinding
    RefreshPolicy  RefreshPolicy
    ProcessingPolicyRef *PolicyID
    State          SemanticIndexState
    CreatedAt      time.Time
    UpdatedAt      time.Time
}

type SourcePolicy struct {
    TemplateKeys      []string
    Tags              []string
    PropertySelectors map[string]string
    SourceMode        SourceMode // self, subtree, custom
    MaxDepth          int
    IncludeProps      []string
    MinimumTextLength int
}

type IndexRuntimeBinding struct {
    RuntimeID     RuntimeID
    ModelID       ModelID
    VectorStoreID VectorStoreID
}

type RefreshPolicy struct {
    Mode             RefreshMode // manual, dirty_debounce, scheduled, disabled
    DebounceDuration time.Duration
    Schedule         string
}
```

The semantic index binding intentionally does not contain an API key. Runtime/model/vector-store binding describes the required processing infrastructure. Credential grants separately authorize which credential may be used when that runtime must be called.

A semantic index can exist before it is fully active. For example, an application can provision `notes-search`, but the index remains `inactive_missing_credentials` until a user adds and grants a compatible credential.

### Inference Policy

An inference policy controls whether graph content may be processed by inference runtimes.

It answers:

> Is this content allowed to leave the graph and be processed by this kind of runtime for this operation?

Inference policy is separate from credential grants:

```text
CredentialGrant = authorization to use a secret
InferencePolicy = authorization/restriction for content to be processed
```

A credential grant may allow Martin's OpenAI key in a space, while a node-level inference policy may still forbid sending a private subtree to any third-party runtime. The content policy must win.

Policies can be attached to:

- space
- domain
- semantic index
- node
- subtree rooted at a node

Common policies:

- `no_inference`: content must not be processed by embeddings, chat, rerank, summarization, or classification
- `local_only`: content may only be processed by local runtimes
- `enterprise_private_only`: content may only be processed by local or enterprise-private runtimes
- `deny_operation`: content may not be processed for a specific operation such as embeddings or chat
- `allow_operation`: content may be processed for specific operation/runtime classes, subject to more specific denies

Conceptual structure:

```go
type InferencePolicy struct {
    ID                 PolicyID
    Scope              ProcessingScope
    Effect             PolicyEffect // allow, deny, restrict
    Operations         []Operation

    NoInference         bool
    AllowedPrivacyClasses []PrivacyClass
    DisallowThirdParty  bool
    RequireLocalRuntime bool

    Reason             string
    CreatedBy          PrincipalRef
    CreatedAt          time.Time
    ExpiresAt          *time.Time
}
```

#### Policy Resolution

Policy resolution should happen before credential resolution and before any runtime call.

Recommended order:

1. resolve the graph content scope being processed
2. collect policies inherited from space, domain, semantic index, node ancestors, and explicit subtree policies
3. apply the most restrictive effective policy
4. remove semantic indexes/runtimes/models that violate the policy
5. resolve credential grants only for the remaining allowed runtime calls
6. execute and record policy/credential provenance

Rules:

- deny beats allow
- `no_inference` excludes content from all inference operations
- `local_only` excludes third-party and enterprise-private remote runtimes unless they are classified as local
- node/subtree policies override broader domain/space allowances
- semantic index source selection must skip nodes disallowed by effective policy
- a semantic query over a broad scope may partially search allowed indexes and return warnings for skipped indexes/content

Example:

```text
space grant allows OpenAI
space policy allows third-party embeddings
node policy marks a subtree local_only
```

Result:

```text
OpenAI semantic indexes must skip that subtree.
A local semantic index may include it if its runtime/model/vector store satisfy policy.
```

Policy evaluation should produce a durable or inspectable decision record for audit/debug flows when content is embedded or skipped:

```go
type PolicyDecision struct {
    ID               PolicyDecisionID
    Scope            ProcessingScope
    Operation        Operation
    RuntimeID        *RuntimeID
    ModelID          *ModelID
    Allowed          bool
    MatchedPolicyIDs []PolicyID
    Reason           string
    CreatedAt        time.Time
}
```

### Embedding Record

An embedding record is a low-level artifact produced for a node/source under a semantic index.

Conceptual structure:

```go
type EmbeddingRecord struct {
    ID              EmbeddingRecordID
    SpaceID         space.ID
    DomainID        graph.DomainID
    SemanticIndexID SemanticIndexID
    NodeID          graph.NodeID
    RuntimeID       RuntimeID
    ModelID         ModelID
    VectorStoreID   VectorStoreID
    CredentialID    *CredentialID
    CredentialGrantID *CredentialGrantID
    PolicyDecisionID *PolicyDecisionID
    SourceMode      SourceMode
    SourceHash      string
    VectorSpaceKey  string
    Dimensions      int
    VectorRef       string // or inline vector for embedded store
    CreatedAt       time.Time
}
```

Search should ignore stale records by choosing the latest logical record for a node/index/source identity. Records should retain credential grant and policy-decision provenance for auditing and debugging.

## File-Backed Storage Layout

The detailed file-backed storage layout for advanced embeddings is documented in [`docs/storage/advanced-embeddings.md`](storage/advanced-embeddings.md).

That storage document defines:

- current embedding metadata and `.kvec` segment structures
- proposed global JSON files for inference packages, runtimes, models, vector stores, secrets, credentials, grants, and policies
- proposed per-space JSON files for semantic indexes, index state, dirty work, and policy decisions
- proposed per-index append-only vector block structure
- recovery, freshness, and compaction behavior

## Provisioning Responsibilities

### Mycel Library

Mycel owns:

- resource schemas
- stores
- validation rules
- connector interfaces
- generation/search execution
- stale record semantics
- policy checks
- query planning contracts

Mycel should not silently create provider-specific runtime/model definitions for every installation.

### Mycel CLI / Operator

The CLI provisions:

- inference packages
- runtime definitions
- model definitions
- vector store backends
- credentials
- credential grants
- inference/content policies
- semantic indexes
- backfill/maintenance jobs

### Application Using Mycel

The application provisions semantic intent:

- graph templates
- spaces/domains with application meaning
- semantic index definitions
- default source policies
- index purposes
- refresh behavior

For Knot PKM, this includes indexes over `logseq.journal`, `logseq.page`, and `app.task` templates.

### User / Organization / Deployment

The user or operator provisions authority and policy:

- API keys
- local/private runtime endpoints
- privacy constraints
- allowed providers/runtimes
- credential grants
- inference/content policies such as local-only or no-inference subtrees

## Example Provisioning Flow

### 1. Initialize Mycel

```sh
mycel -d /data/mycel init
```

This initializes storage and system stores only.

### 2. Apply inference definitions

```sh
mycel inference package apply standard-openai.yaml
mycel inference package apply local-ollama.yaml
```

### 3. Create user/space/domain/templates

```sh
mycel user add martin --password pass
mycel space add "Personal PKM" --owner martin --default-domain personal-pkm
mycel template import logseq-journal.json
```

### 4. Add user credential

```sh
OPENAI_API_KEY=sk-... mycel inference credential add martin-openai \
  --runtime openai-public \
  --owner-user martin \
  --auth api-key \
  --api-key-env OPENAI_API_KEY
```

### 5. Create semantic index

```sh
mycel semantic index add notes-search \
  --space-id <space-id> \
  --domain personal-pkm \
  --purpose semantic_search \
  --template-key logseq.journal \
  --template-key logseq.page \
  --source subtree \
  --runtime openai-public \
  --model openai/text-embedding-3-small \
  --vector-store mycel-file
```

### 6. Grant credential use

```sh
mycel inference credential grant martin-openai \
  --space-id <space-id> \
  --domain personal-pkm \
  --semantic-index notes-search \
  --operation embeddings
```

### 7. Optionally add content policy

For example, block a private subtree from all inference processing:

```sh
mycel inference policy deny \
  --space-id <space-id> \
  --domain personal-pkm \
  --node <private-node-id> \
  --include-descendants \
  --operation embeddings \
  --operation chat
```

Or require local processing for a subtree:

```sh
mycel inference policy restrict \
  --space-id <space-id> \
  --domain personal-pkm \
  --node <private-node-id> \
  --include-descendants \
  --local-only
```

### 8. Backfill index

```sh
mycel semantic index backfill notes-search \
  --space-id <space-id> \
  --domain personal-pkm
```

Backfill must evaluate inference policy before generating each embedding and must resolve a compatible credential grant before each runtime call.

### 9. Search

```sh
mycel semantic search \
  --space-id <space-id> \
  --domain personal-pkm \
  --index notes-search \
  --text "sleep, exercise, and focus"
```

## Query Planning

A single semantic query may target multiple indexes.

Mycel should:

1. resolve the requested domain/index/content scope
2. find semantic indexes covering that scope and purpose
3. evaluate inference policies and remove disallowed content/index/runtime combinations
4. group remaining indexes by compatible vector space/runtime/model
5. resolve a compatible credential grant for each required query-embedding runtime call
6. generate one query embedding per compatible vector-space group
7. search each vector store/index
8. merge and rank results
9. return provenance, including index/model/runtime/credential-grant IDs and warnings for skipped indexes/content

If indexes use incompatible vector spaces, Mycel must not compare raw cosine scores without normalization or reranking.

A query may therefore produce several query embeddings. Different credential grants do not themselves define the search space; they only determine whether Mycel is authorized to call the runtimes needed by the selected semantic indexes.

## Lifecycle and State

Semantic indexes should expose operational state:

- `created`
- `inactive_missing_runtime`
- `inactive_missing_credentials`
- `active`
- `backfilling`
- `degraded`
- `disabled`
- `failed`
- `retired`

Useful operational metadata:

- last backfill timestamp
- last refresh timestamp
- last error
- dirty node count
- current record count
- source policy hash
- runtime/model/vector store binding
- policy exclusion/skipped-node counts
- credential grant resolution failures

## Migration From Current Model

Current Mycel embedding support contains:

- provider API keys
- embedding profiles
- explicit embedding generation
- embedding records
- semantic search over records

The proposed model maps roughly as follows:

| Current Concept | Future Concept |
| --- | --- |
| embedding provider key | inference credential |
| no current equivalent | credential grant |
| no current equivalent | inference/content policy |
| embedding profile | part of semantic index binding/source policy |
| profile provider/model/source | semantic index runtime/model/source policy |
| embedding record profile ID | embedding record semantic index ID |
| embeddings generate | semantic index backfill/refresh |
| embeddings search | semantic search over one or more semantic indexes |

The old profile-oriented CLI can remain temporarily as a compatibility or low-level command, but application-facing provisioning should move toward semantic indexes.

## Open Questions

1. Should Mycel ship with a disabled built-in `mycel-file` vector store definition, or should even that be provisioned explicitly by `mycel init`?
2. Should inference packages be stored globally per data directory, per tenant, or per space?
3. Should model definitions be global while runtime definitions are deployment-specific?
4. How should semantic index templates be namespaced across applications?
5. What is the minimum policy model needed for the first implementation?
6. Should credential grants be required for all credential use, or can user-owned personal-space defaults be resolved automatically?
7. How should external vector store lifecycle and deletion guarantees be represented?
