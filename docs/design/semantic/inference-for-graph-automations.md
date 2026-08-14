# Standalone Inference Model for Graph Automations

## Status

Proposed breaking design for a future mycel inference and graph automation
subsystem iteration. This design intentionally does not preserve current data
structures, APIs, CLI commands, or file formats for compatibility. mycel is not
yet released, so the target is a coherent future-facing model.

## Summary

mycel should provide standalone inference infrastructure that is sufficient for
mycel-owned workloads such as semantic indexing/search and graph automations.
That infrastructure should stay neutral: it can model endpoints, models,
capabilities, credentials, grants, privacy policy decisions, and usage telemetry,
but it must not model application billing, credits, subscriptions, margins, or
product-specific pricing.

Graph automations should not contain provider API keys or full credential
configuration. They should reference stable inference profiles or model
capability references. At runtime mycel resolves the automation, space, domain,
and principal context to an endpoint/model/capability and an authorized
credential, evaluates inference policy, invokes the provider connector, and
records neutral usage telemetry.

Target runtime shape:

```text
automation + space/domain/principal context
  -> inference profile or model ref
  -> endpoint/model/capability
  -> credential + grant + secret material
  -> policy decision
  -> provider connector invocation
  -> usage telemetry
```

## Goals

- Keep mycel standalone and not dependent on Knot PKM or any application billing
  model.
- Provide enough inference infrastructure for graph automations to perform
  LLM-backed work on behalf of principals.
- Use one inference resource model for semantic embeddings and graph automation
  LLM calls where the concepts are shared.
- Keep product/business pricing out of mycel.
- Record neutral usage telemetry such as request status, provider request ID,
  input/output token counts, latency, endpoint/model/capability, credential
  grant, policy decision, automation run ID, and principal context.
- Keep graph automation definitions free of API keys, raw secret refs, and full
  provider credential configuration.
- Allow credential rotation without re-registering or editing automations.
- Support principal-owned, space-owned, and system-owned credentials.
- Resolve model/profile/credential/policy using space, domain, and principal
  scope.
- Prefer space-scoped runtime inference profiles over global runtime defaults.
- Keep low-level connector/adaptor support at system level, because it describes
  what the mycel binary knows how to execute.
- Preserve mycel architectural style: daemon-owned APIs, scoped authorization,
  fail-closed policy enforcement, storage manager boundaries, and explicit CLI
  resource nouns.

## Non-goals

- No application credits, subscription plans, customer billing, pricing margins,
  or chargeback policies in mycel.
- No Knot PKM-specific concepts.
- No compatibility migration for existing semantic inference or graph automation
  stores.
- No generic application chat product in this design. A future application may
  expose chat UX on top of mycel, but this design only requires inference for
  mycel-owned workloads such as graph automations and semantic embeddings.
- No automatic repair if inference catalog, credential, or policy state is
  inconsistent. Configuration should fail closed and require operator action.
- No broad node/subtree authorization redesign beyond the inference-specific
  privacy and grant checks described here.

## Current architecture assessment

### Semantic/inference subsystem

The existing semantic subsystem already contains a richer inference resource
model:

- model endpoints
- inference models
- endpoint/model capabilities
- vector stores
- secrets
- credentials
- credential grants
- inference policies
- policy decisions
- usage events

This is the right conceptual direction, but the current implementation is
embedding-centric and is physically owned by the semantic subsystem. Important
limitations:

1. **Embedding-only execution path**: the connector service resolves endpoint,
   model, capability, credential, and secret material for embeddings. Graph
   automation chat/completion calls bypass this path.
2. **No stable inference profile abstraction**: semantic indexes bind directly
   to endpoint/model/capability IDs, and automations bind to provider/model
   strings. Neither model gives users a stable space-scoped runtime profile such
   as `summarizer` or `private-local-chat`.
3. **Credential owner terminology is not aligned with principals**: current
   owner types include `user` and `organization`; the target identity model uses
   principals, spaces, and system actors.
4. **Policy decisions are modeled but not consistently produced**: policy
   evaluation should persist allow/deny decisions and connect them to usage
   events.
5. **Usage accounting is neutral enough to reuse**: the usage ledger has the
   right shape for endpoint/model/credential/policy/principal metadata and token
   counts. It should remain neutral and not add pricing or credits.
6. **Admin capability is too coarse**: Admin Inference currently uses semantic
   search capability for broad management operations. A standalone inference
   subsystem needs clearer manage/use/audit capabilities.

### Graph automation subsystem

The current graph automation subsystem uses a simpler model:

- automation definitions contain provider/model-like configuration
- the daemon reads automation provider settings from environment variables
- the provider path is separate from semantic inference credentials, grants,
  policies, and usage telemetry
- run records include token usage and a cost estimate

Important limitations:

1. **Credentials are global daemon config**: automations effectively depend on
   one configured provider/API key path instead of scoped credentials and grants.
2. **Provider/model strings are not resolved resources**: validation only checks
   syntax/ranges, not endpoint/model/capability availability or policy.
3. **Policy is separate and incomplete**: automation safety policy is not the
   same as inference privacy policy, and some ceilings are enforced only after a
   provider call.
4. **Cost estimates conflict with mycel neutrality**: token counts are useful;
   hard-coded product/provider pricing belongs outside mycel.
5. **Run actor context is under-modeled**: automations should run as a service
   actor on behalf of a principal/application context and with explicit graph
   and inference authorization.

## Proposed conceptual model

The redesigned model has four layers.

### 1. System-level connector and catalog layer

System-level definitions describe what this mycel deployment can execute.
They are managed by system administrators and are not tied to one application
or space.

System-level resources:

| Resource | Scope | Purpose |
|---|---|---|
| Connector adapter | binary/system | Built-in connector implementation such as `openai-compatible`, `ollama`, `local-http`, or `fake` for tests. |
| Inference endpoint | system | A configured provider endpoint URL plus connector type, network class, privacy class, supported auth modes, and enabled state. |
| Inference model | system | A logical model definition: operation, provider model name, modality, token/context limits, embedding dimensions, and supported connector types. |
| Endpoint capability | system | A binding proving that an endpoint can run a model for an operation with known features. |
| Vector store | system | Storage backend for embedding/vector search. This remains relevant to semantic indexes. |
| Secret backend configuration | system | What secret reference schemes this daemon accepts, such as encrypted-inline and `env://`. |

Low-level connector adapters stay system-level because they are code capabilities
of the mycel binary. Endpoint and model catalog entries may be installed from
packages, but packages should only define neutral execution resources. They
should not include application pricing.

### 2. Space-level runtime selection layer

Space-level resources define how a space uses available system-level inference
resources.

The key new resource is an **InferenceProfile**. A profile is a stable,
space-scoped runtime reference such as:

```text
summarize-page
classify-note
private-local-chat
default-embedding
```

A profile is not a credential. It describes desired operation/capabilities,
selection constraints, default runtime parameters, and privacy requirements.
Automations and semantic indexes can reference a profile without knowing which
API key, secret version, or concrete endpoint will be used.

Space-level resources:

| Resource | Scope | Purpose |
|---|---|---|
| Inference profile | space, optionally domain-limited | Stable runtime ref used by semantic indexes and automations. |
| Credential grant | space/domain/node scope | Allows a credential to be used for an operation/profile/capability in a scope. |
| Inference policy | space/domain/node scope | Allows, denies, or restricts inference based on privacy/network/locality/operation. |
| Policy decision | space/domain/run/request scope | Durable audit record for allow/deny/restrict decisions. |
| Usage event | append-only ledger | Neutral telemetry for completed, failed, and denied inference attempts. |

Profiles should normally be space-scoped. Domain scoping can be represented as
profile constraints or as policies/grants that restrict where the profile can be
used. This avoids a global `default model` that silently applies to every space.

### 3. Credential and grant layer

Credentials represent authority to authenticate to an endpoint. Grants represent
where and on whose behalf those credentials may be used.

Credential ownership should support:

| Owner type | Meaning |
|---|---|
| `principal` | A human/service principal owns the credential. Use is limited by grants and usually by on-behalf-of principal context. |
| `space` | A space-owned credential managed by space administrators. |
| `system` | A daemon/system credential managed by system administrators and grantable to spaces/domains. |

Credentials should be stable across secret rotation. Automations reference
profiles; grants reference credentials; credentials reference secret material.
Rotating a secret updates the credential secret reference or credential version
without changing automations or profiles.

### 4. Workload layer

Workloads are mycel-owned consumers of inference:

- semantic embedding generation
- semantic search query embedding
- graph automation LLM steps
- future maintenance tasks that explicitly use inference

Workloads declare the operation and profile/model capability they need. They do
not directly resolve credentials or secrets.

## Proposed data structures

The following structures are conceptual. Exact protobuf and Go names can evolve,
but the boundaries should remain.

### ConnectorAdapter

Connector adapters are code-level registrations, not user data records.

```go
type ConnectorAdapter struct {
    Type                string // openai-compatible, ollama, local-http, fake
    SupportedOperations []InferenceOperation
    SupportedAuthTypes  []CredentialAuthType
    SupportsStreaming   bool
    SupportsToolCalls   bool
    SupportsJSONMode    bool
}
```

### InferenceEndpoint

```go
type InferenceEndpoint struct {
    ID             string
    Key            string
    DisplayName    string
    ConnectorType  string
    BaseURL        string
    NetworkClass   NetworkClass  // local | private_network | public_internet
    PrivacyClass   PrivacyClass  // local_only | private | third_party
    SupportedAuth  []CredentialAuthType
    Operations     []InferenceOperation
    Enabled        bool
    CreatedAt      time.Time
    UpdatedAt      time.Time
    Metadata       map[string]string
}
```

`PrivacyClass` is about where data goes when this endpoint is used. A public
third-party endpoint should require explicit policy allowance for private graph
content.

### InferenceModel

```go
type InferenceModel struct {
    ID                string
    Key               string
    DisplayName       string
    Operation         InferenceOperation // chat | embeddings | rerank | summarize | classify
    ProviderModelName string
    ConnectorTypes    []string
    InputModalities   []string
    OutputModalities  []string
    ContextTokens     int32
    MaxOutputTokens   int32
    EmbeddingDims     int32
    VectorSpace       string
    Enabled           bool
    Metadata          map[string]string
}
```

### EndpointCapability

```go
type EndpointCapability struct {
    ID                     string
    EndpointID             string
    ModelID                string
    Operation              InferenceOperation
    CapabilityKey          string
    ProviderModelOverride  string
    SupportsJSONMode       bool
    SupportsToolCalls      bool
    SupportsEmbeddings     bool
    MaxInputTokens         int32
    MaxOutputTokens        int32
    DefaultParameters      map[string]string
    Enabled                bool
    CreatedAt              time.Time
    UpdatedAt              time.Time
}
```

A capability is the concrete resource the runtime can invoke once policy and
credential resolution succeed.

### InferenceProfile

```go
type InferenceProfile struct {
    ID                string
    SpaceID           string
    Key               string
    DisplayName       string
    Description       string
    Operation         InferenceOperation
    Purpose           string // semantic_index | automation | general_internal
    DomainAllowlist   []string
    CapabilityRefs    []string // optional explicit candidate capabilities
    EndpointRefs      []string // optional endpoint constraints
    ModelRefs         []string // optional model constraints
    RequiredFeatures  []string // json_mode, tool_calls, embeddings, local_only
    PrivacyRequired   PrivacyRequirement
    DefaultParameters InferenceParameters
    Enabled           bool
    CreatedBy         string
    CreatedAt         time.Time
    UpdatedAt         time.Time
    Metadata          map[string]string
}
```

Profiles are stable references used by workloads. A profile can either pin a
specific capability or describe a selector that the resolver matches against the
system catalog.

### InferenceCredential

```go
type InferenceCredential struct {
    ID            string
    Key           string
    DisplayName   string
    EndpointID    string
    OwnerType     CredentialOwnerType // principal | space | system
    OwnerID       string
    AuthType      CredentialAuthType   // api_key | bearer | basic | none
    SecretRef     string               // encrypted-inline://... | env://... | external://...
    SecretVersion string
    Status        CredentialStatus     // active | disabled | revoked
    CreatedBy     string
    CreatedAt     time.Time
    UpdatedAt     time.Time
    RotatedAt     time.Time
    Metadata      map[string]string
}
```

Secret material must not appear in list/get responses, automation definitions,
run records, policy decisions, or usage events.

### CredentialGrant

```go
type CredentialGrant struct {
    ID                  string
    SpaceID             string
    CredentialID        string
    Scope               InferenceScope
    Operations          []InferenceOperation
    ProfileRefs         []string
    CapabilityRefs      []string
    EndpointRefs        []string
    ModelRefs           []string
    UsageModes          []UsageMode // interactive | automation | background | semantic
    GranteePrincipals   []string    // optional; empty means scoped-space policy decides
    AllowOnBehalfOf     []string    // optional principal IDs or space-principal wildcard
    Priority            int32
    State               GrantState // active | expired | revoked
    CreatedBy           string
    CreatedAt           time.Time
    ExpiresAt           time.Time
    RevokedBy           string
    RevokedAt           time.Time
    Reason              string
}
```

A credential grant gives inference-use authority only. It does not grant graph
read/write access and does not make a principal a space administrator.

### InferencePolicy

```go
type InferencePolicy struct {
    ID                    string
    SpaceID               string
    Scope                 InferenceScope
    Operations            []InferenceOperation
    ProfileRefs           []string
    Action                PolicyAction // allow | restrict | deny
    NoInference           bool
    AllowedPrivacyClasses []PrivacyClass
    RequireLocalEndpoint  bool
    DisallowThirdParty    bool
    MaxInputTokens        int32
    MaxOutputTokens       int32
    MaxRequestsPerRun     int32
    DataClasses           []string
    Priority              int32
    State                 PolicyState // active | expired | revoked
    CreatedBy             string
    CreatedAt             time.Time
    ExpiresAt             time.Time
    Reason                string
}
```

Token/request ceilings are operational safety limits, not pricing or credit
limits.

### PolicyDecision

```go
type PolicyDecision struct {
    ID               string
    SpaceID          string
    DomainID         string
    NodeID           string
    Operation        InferenceOperation
    UsageMode        UsageMode
    ProfileID        string
    CapabilityID     string
    EndpointID       string
    ModelID          string
    CredentialID     string
    CredentialGrantID string
    ActorPrincipalID string
    OnBehalfOfPrincipalID string
    Action           PolicyDecisionAction // allowed | denied
    MatchedPolicyIDs []string
    Reason           string
    DecidedAt        time.Time
    Metadata         map[string]string
}
```

Policy decisions should be written for allowed and denied requests. A denied
request should still produce a usage event with status `denied` and zero provider
tokens if enough context exists to identify the attempted operation.

### InferenceUsageEvent

```go
type InferenceUsageEvent struct {
    ID                    string
    RequestID             string
    Operation             InferenceOperation
    UsageMode             UsageMode
    Status                UsageStatus // succeeded | failed | denied | canceled
    SpaceID               string
    DomainID              string
    NodeID                string
    AutomationID          string
    AutomationRunID       string
    SemanticIndexID       string
    ActorPrincipalID      string
    OnBehalfOfPrincipalID string
    ProfileID             string
    EndpointID            string
    ModelID               string
    CapabilityID          string
    CredentialID          string
    CredentialGrantID     string
    PolicyDecisionID      string
    ProviderRequestID     string
    InputTokens           int64
    OutputTokens          int64
    TotalTokens           int64
    LatencyMillis         int64
    ErrorCode             string
    ErrorMessage          string
    StartedAt             time.Time
    CompletedAt           time.Time
    Metadata              map[string]string
}
```

This event intentionally has no `cost`, `price`, `credit`, `margin`,
`customer`, or subscription fields.

### Automation definition inference reference

Replace the current automation `model` provider/model object with an inference
reference.

Single-step example:

```json
{
  "id": "summarize_page",
  "name": "Summarize page",
  "status": "disabled",
  "on": {"events": ["node.created", "node.updated"], "labels": ["Page"]},
  "condition": {"gql": "MATCH (changed:Page) RETURN changed"},
  "input": {"target": "changed", "fields": ["properties.title", "payload.text"]},
  "inference": {
    "operation": "chat",
    "profile": "summarize-page",
    "parameters": {
      "temperature": 0.2,
      "maxOutputTokens": 512,
      "responseFormat": "text"
    }
  },
  "prompt": "Summarize this page in concise markdown.",
  "output": {
    "mode": "text",
    "actions": [
      {"update_node": {"target": "changed", "set": {"payload.summaryMarkdown": "$result.text"}}}
    ]
  }
}
```

Workflow step example:

```json
{
  "id": "extract_tasks",
  "kind": "llm",
  "inference": {
    "operation": "chat",
    "profile": "structured-task-extractor",
    "parameters": {"responseFormat": "json", "maxOutputTokens": 1024}
  }
}
```

Automation validation should verify that:

- an inference reference is present for LLM-backed steps;
- the requested operation is compatible with the step kind;
- generation parameters are syntactically valid;
- credentials are not embedded in the definition;
- raw endpoint URLs, API keys, secret refs, and bearer tokens are rejected;
- a profile key is syntactically valid, but runtime resolution remains
  fail-closed because profiles/grants/policies can change after definition
  registration.

## Proposed resolution flow

### Inputs

Runtime resolution receives:

- workload type: `automation`, `semantic_index`, or `semantic_search`
- operation: `chat`, `embeddings`, etc.
- space ID
- domain ID
- optional node/subtree context
- actor principal ID
- on-behalf-of principal ID
- requested profile key or explicit model/capability ref
- usage mode: `automation`, `background`, `semantic`, or `interactive`
- requested runtime parameters
- content privacy/data-class metadata where available

### Actor and on-behalf-of semantics

Graph automation execution should distinguish:

| Field | Meaning |
|---|---|
| Actor principal | The mycel service principal executing the automation worker. |
| On-behalf-of principal | The principal whose context caused or authorized the work. |
| Automation owner | The principal that created or last updated the automation definition. |

For committed graph-change triggers, the on-behalf-of principal should be the
principal associated with the triggering graph write when that metadata is
available. For scheduled/scanned automations, the on-behalf-of principal should
be the automation owner or an explicitly configured service principal. This must
be recorded in invocation/run metadata.

Graph access and inference access are separate:

- graph read/write authorization controls whether the automation can read input
  and apply graph actions;
- inference grants and policies control whether the automation can call a model
  with the selected content/context.

### Resolution steps

1. **Load workload and scope**
   - Load the automation definition or semantic index.
   - Resolve space and domain.
   - Determine actor and on-behalf-of principals.
   - Determine operation and usage mode.

2. **Resolve profile**
   - Resolve the profile key/ID under the space.
   - Check enabled state and optional domain allowlist.
   - Merge profile default parameters with workload parameters.
   - Reject if the workload requests a parameter outside profile limits.

3. **Select candidate capabilities**
   - If the profile pins capabilities, use them as candidates.
   - Otherwise match operation, required features, endpoint/model constraints,
     network class, privacy requirements, and enabled state.
   - Reject if no compatible capability exists.

4. **Resolve credential grants**
   - For each candidate capability, find active credential grants in the space
     whose scope matches the domain/node/subtree, operation, profile, endpoint,
     model, capability, usage mode, actor principal, and on-behalf-of principal.
   - Resolve active credentials for the candidate endpoint.
   - Reject candidates without a usable credential/grant.

5. **Evaluate inference policies**
   - Apply deny/no-inference policies first.
   - Apply restrict policies next.
   - Require at least one active allow/restrict policy that permits the
     operation/profile/scope/privacy class.
   - Enforce third-party/local requirements and token/request safety ceilings.
   - Persist a `PolicyDecision` for allow or deny.

6. **Resolve secret material**
   - Fetch/decrypt secret material only after policy allows the request.
   - Secret material is kept in memory only for connector invocation.
   - Secret values are never returned by APIs or written to logs/events.

7. **Invoke connector**
   - Build a connector-neutral request with prompt/messages/input,
     parameters, model override, endpoint URL, auth material, and tracing
     metadata.
   - Call the connector with timeout/retry rules appropriate for the operation.

8. **Record usage telemetry**
   - Append an `InferenceUsageEvent` for success, failure, denial, or
     cancellation.
   - Attach policy decision, credential grant, profile, capability, provider
     request ID, token counts, latency, and automation/semantic refs.

9. **Return workload result**
   - Return generated text/JSON/embeddings to the caller subsystem.
   - Do not expose credential or secret details to the automation definition,
     graph result, or client API response.

## Credential and secret rotation

Credential rotation should not require changes to automations, semantic indexes,
or profiles.

Recommended model:

- `InferenceCredential.ID` is stable.
- Grants reference `CredentialID`.
- Profiles reference capabilities/selectors, not credentials.
- Automation definitions reference profiles.
- Rotation updates `Credential.SecretRef` and increments `SecretVersion`, or
  creates a replacement credential and atomically updates relevant grants.

CLI/API should support both patterns:

```sh
mycel inference credential rotate openai-prod \
  --external-ref env://OPENAI_API_KEY_V2

mycel inference grant expire '<grant-id>' --space-id '<space-id>'
mycel inference grant openai-prod-v2 --space-id '<space-id>' --domain notes --operation chat
```

Secret representation:

| Secret ref | Meaning |
|---|---|
| `encrypted-inline://...` | Daemon-encrypted secret material in mycel storage. Requires daemon encryption key. |
| `env://NAME` | Read from daemon process environment. Good for local/dev and simple deployments. |
| `external://provider/path` | Future external secret manager reference. Must be explicit and connector-backed. |

If external secret manager support is not implemented, docs and validation must
not claim that `vault://` or similar schemes work.

## Credential ownership and grants

### Principal-owned credentials

Principal-owned credentials are useful when a human/service principal brings its
own provider key.

Rules:

- The owner principal can create/revoke/rotate the credential subject to
  capability checks.
- A principal-owned credential can be granted to a space/domain/profile for
  automation use.
- The grant should normally restrict use to `on_behalf_of == owner`, unless the
  owner explicitly grants broader use.
- If the owner is disabled or the credential is revoked, resolution fails closed.

### Space-owned credentials

Space-owned credentials are managed by principals with appropriate space-scoped
inference management authority.

Rules:

- Grants can permit use by automations in that space/domain.
- Optional grantee principal constraints can limit which automation owners or
  service principals may use the credential.
- Space ownership does not imply system-wide use.

### System-owned credentials

System-owned credentials are managed by system administrators and can be granted
to spaces/domains.

Rules:

- System-owned credentials require explicit grants before any space workload can
  use them.
- A system credential should never act as a global implicit default.
- Grants should identify usage modes and scopes to prevent accidental broad
  third-party inference.

## Privacy and inference policy enforcement

Policy enforcement should be centralized in the inference subsystem and reused by
semantic indexing, semantic search, and graph automations.

Principles:

1. **Fail closed**: missing profile, capability, credential grant, active
   credential, secret material, or allow/restrict policy denies the request.
2. **Deny wins**: explicit deny/no-inference policies override allow policies.
3. **Scope specificity matters**: node/subtree policy should be more specific
   than domain policy, which is more specific than space policy.
4. **Third-party use is explicit**: sending private graph content to a
   third-party endpoint requires a policy that allows the endpoint privacy class.
5. **Local-only means local-only**: if a policy or profile requires local
   inference, public/third-party endpoint candidates are rejected.
6. **Policy decisions are durable**: decisions are written before connector
   invocation and referenced by usage events.
7. **Inference policy is not billing**: request/token ceilings are operational
   safety controls only.

Policy inputs should include:

- space/domain/node scope
- operation and usage mode
- actor principal and on-behalf-of principal
- profile/capability/endpoint/model
- endpoint network and privacy class
- credential owner/grant metadata
- content privacy class/data classes when known
- automation ID/run ID or semantic index/search context

## Usage telemetry

mycel should record neutral inference telemetry for observability, audit, and
operator troubleshooting.

Record:

- status: allowed/succeeded/failed/denied/canceled
- operation and usage mode
- space/domain/node/semantic-index/automation-run refs
- actor/on-behalf-of principal refs
- profile/endpoint/model/capability refs
- credential and credential grant refs, but not secret material
- policy decision ref
- provider request ID when available
- input/output/total tokens when available
- latency and retry/error metadata

Do not record:

- price
- credit usage
- customer billing IDs
- subscription IDs
- provider margins
- app-specific account balances

Automation run records may denormalize token counts and provider request IDs for
run debugging, but the inference usage ledger should be authoritative for
cross-subsystem telemetry.

## How existing structures should change

### Semantic/inference structures

Recommended direction: extract shared inference resources from the semantic
subsystem into a standalone inference subsystem.

Current concept | Target concept | Direction
---|---|---
`ModelEndpoint` | `InferenceEndpoint` | Reuse concept; make operation-generic.
`InferenceModel` | `InferenceModel` | Reuse concept; ensure chat/JSON/tool features are modeled.
`ModelEndpointCapability` | `EndpointCapability` | Reuse concept; extend beyond embeddings.
`InferenceCredential` | `InferenceCredential` | Reuse concept; replace `user` owner with `principal`; support stable rotation semantics.
`CredentialGrant` | `CredentialGrant` | Reuse concept; add profiles, usage modes, actor/on-behalf constraints.
`InferencePolicy` | `InferencePolicy` | Reuse concept; centralize enforcement and fix scope specificity.
`PolicyDecision` | `PolicyDecision` | Start writing decisions consistently.
`InferenceUsageEvent` | `InferenceUsageEvent` | Reuse ledger; keep it pricing-free.
`SemanticIndex` endpoint/model refs | `SemanticIndex` profile/capability refs | Prefer profile refs for runtime stability.

The semantic subsystem should become a consumer of inference for embeddings. It
should still own semantic indexes, backfill, vector records, vector stores where
appropriate, search planning, and semantic maintenance. It should not be the
owner of all inference credentials/policies if graph automations also use them.

### Graph automation structures

Current concept | Target concept | Direction
---|---|---
Automation `Model{provider,model}` | Automation `InferenceRef{operation,profile,parameters}` | Replace.
Daemon env automation provider/API key | Inference profiles + credentials + grants | Replace.
Automation cost estimate | Neutral token/provider telemetry | Remove pricing.
`MaxCostPerRun` | Request/token/provider-call safety ceilings | Replace.
Run provider/model fields | Profile/endpoint/model/capability refs | Replace or denormalize from inference usage.
Automation policy budget fields | Automation safety + inference policy | Split responsibilities.

Automation validation should reject embedded credentials and raw endpoint URLs.
Runtime execution should call an inference runtime API instead of an
automation-specific provider interface.

## Proposed CLI command structure

CLI design should be systematic, singular-noun-first, and scoped consistently.
Plural aliases can be added where useful, but docs should use singular nouns.

### Global/system inference catalog

```sh
mycel inference package apply <file>
mycel inference package list

mycel inference endpoint list
mycel inference endpoint get <endpoint-ref>
mycel inference endpoint enable <endpoint-ref>
mycel inference endpoint disable <endpoint-ref>
mycel inference endpoint delete <endpoint-ref>

mycel inference model list
mycel inference model get <model-ref>
mycel inference model enable <model-ref>
mycel inference model disable <model-ref>
mycel inference model delete <model-ref>

mycel inference capability list --endpoint <endpoint-ref> --operation chat
mycel inference capability get <capability-ref>
mycel inference capability enable <capability-ref>
mycel inference capability disable <capability-ref>
mycel inference capability delete <capability-ref>
```

### Space-level profiles

```sh
mycel inference profile create summarize-page \
  --space-id <space-id> \
  --operation chat \
  --capability openai-gpt-4o-mini-chat \
  --privacy-class third_party \
  --max-output-tokens 512

mycel inference profile list --space-id <space-id>
mycel inference profile get summarize-page --space-id <space-id>
mycel inference profile enable summarize-page --space-id <space-id>
mycel inference profile disable summarize-page --space-id <space-id>
mycel inference profile delete summarize-page --space-id <space-id>
```

Profiles accept key or UUID refs. Domain constraints should use the same flag
shape as other scoped commands:

```sh
--space-id <space-id>
--domain <domain-key-or-id>
--node <node-id>
--include-descendants
```

### Credentials and rotation

```sh
mycel inference credential create openai-prod \
  --model-endpoint openai \
  --owner-type system \
  --owner-id daemon \
  --external-ref env://OPENAI_API_KEY

mycel inference credential list --owner-type system
mycel inference credential disable openai-prod
mycel inference credential revoke openai-prod
mycel inference credential rotate openai-prod --external-ref env://OPENAI_API_KEY_V2
mycel inference credential delete openai-prod --delete-secret
```

CLI output must never print secret material.

### Grants

Grants should be top-level under `inference grant`, with `credential grant` as an
alias if desired.

```sh
mycel inference grant openai-prod \
  --space-id <space-id> \
  --domain notes \
  --operation chat \
  --model openai/gpt-4o-mini \
  --grantee-principal-id automation \
  --allow-on-behalf-of-principal-id <owner-principal-id>

mycel inference grant list --space-id <space-id>
mycel inference grant expire <grant-id> --space-id <space-id>
mycel inference grant delete <grant-id> --space-id <space-id>
```

### Policies

```sh
mycel inference policy allow \
  --space-id <space-id> \
  --domain notes \
  --profile summarize-page \
  --operation chat \
  --privacy-class third_party

mycel inference policy restrict \
  --space-id <space-id> \
  --domain private \
  --operation chat \
  --require-local-endpoint

mycel inference policy deny \
  --space-id <space-id> \
  --domain legal \
  --operation chat \
  --reason "no external inference"

mycel inference policy list --space-id <space-id> --domain notes
mycel inference policy expire <policy-id> --space-id <space-id>
mycel inference policy delete <policy-id> --space-id <space-id>
```

### Usage telemetry

```sh
mycel inference usage list \
  --space-id <space-id> \
  --domain notes \
  --operation chat \
  --automation-id summarize-page

mycel inference usage summarize \
  --space-id <space-id> \
  --since 2026-01-01T00:00:00Z \
  --group-by profile_id \
  --group-by operation \
  --group-by status

mycel inference decision get <decision-id> --space-id <space-id>
```

### Automation CLI

Automation commands should use consistent scope flags and should not expose
provider credentials.

```sh
mycel automation validate summarize-page.json
mycel automation create summarize-page.json --space-id <space-id> --domain notes
mycel automation list --space-id <space-id> --domain notes
mycel automation get summarize-page --space-id <space-id> --domain notes
mycel automation enable summarize-page --space-id <space-id> --domain notes
mycel automation disable summarize-page --space-id <space-id> --domain notes
mycel automation delete summarize-page --space-id <space-id> --domain notes
mycel automation runs --space-id <space-id> --domain notes --automation summarize-page
mycel automation run get <run-id> --space-id <space-id> --domain notes
```

Automation docs should show profile references, not provider/model/API key
configuration.

## Proposed API and service boundaries

### Protobuf source of truth

All public API changes must be made in `mycel-api` protobufs and regenerated in
mycel. Generated protobuf files in mycel must not be hand-edited.

### Admin APIs

Admin inference should be exposed as a family of themed services rather than one
monolithic gRPC service:

| Service | Owns |
|---|---|
| `AdminInferenceCatalogService` | packages, endpoints, models, endpoint capabilities, and vector stores |
| `AdminInferenceProfileService` | space-scoped inference profiles |
| `AdminInferenceCredentialService` | credentials, secret refs, credential status, and rotation |
| `AdminInferenceGrantService` | credential grants |
| `AdminInferencePolicyService` | inference policies and policy decisions |
| `AdminInferenceUsageService` | neutral usage events and usage summaries |

Authorization should be capability-based and scoped. Suggested capabilities:

| Capability | Purpose |
|---|---|
| `inference.catalog.read` | List endpoints/models/capabilities/packages. |
| `inference.catalog.manage` | Apply packages and enable/disable/delete catalog resources. |
| `inference.credential.read` | List credential metadata without secret values. |
| `inference.credential.manage` | Create/rotate/revoke/delete credentials. |
| `inference.profile.read` | Read space inference profiles. |
| `inference.profile.manage` | Manage space inference profiles. |
| `inference.grant.manage` | Manage credential grants. |
| `inference.policy.manage` | Manage inference policies. |
| `inference.usage.read` | Read decisions and usage telemetry. |

These can be bundled into roles such as `semantic.admin`, `automation.admin`, or
`inference.admin`, but enforcement should be by capability.

### Client APIs

Graph automation client APIs can remain the user-facing way to define and manage
automations in visible domains. They should validate references but not expose
credential details.

A generic `ClientInferenceService.Invoke` is not required for this design. If it
is added later, it should use the same profile/grant/policy/usage path and must
not become an unscoped provider proxy.

### Internal services

Introduce or extract an `internal/inference` subsystem with:

- catalog manager
- credential/secret manager
- profile manager
- grant manager
- policy engine
- resolver
- connector runtime
- usage ledger

Semantic and automation subsystems consume it:

```text
semantic subsystem -> inference runtime for embeddings
automation subsystem -> inference runtime for chat/structured output
```

The automation subsystem should not own provider API keys or connector-specific
HTTP clients except through the inference runtime.

### Storage boundaries

Suggested storage ownership:

| Data | Storage owner |
|---|---|
| connector registrations | code/runtime only |
| endpoints/models/capabilities/vector stores | inference global manager under `meta/inference/` |
| secrets/credentials | inference global credential manager under `meta/secrets/` and `meta/credentials/` |
| profiles/grants/policies/decisions | inference space manager under per-space inference dirs |
| usage events | inference accounting ledger under `meta/accounting/` or per-space append logs |
| semantic indexes/vector records | semantic subsystem |
| automation definitions/runs/invocations | automation subsystem |

Clustered deployments should make the authoritative resources WAL/Raft-backed
according to existing subsystem conventions. Space-scoped inference resources
should reload consistently with space/domain state. Usage telemetry can be
append-only and may have different replication/retention semantics, but its
contract should be explicit.

## Migration/refactor strategy

No backward compatibility is required. The safest strategy is a staged breaking
refactor where each stage leaves the daemon buildable and testable.

### Phase 1: finalize API/resource design

- Update mycel-api protobufs for inference profiles, revised credentials,
  grants, policies, decisions, and usage telemetry.
- Remove or replace automation provider/model fields in automation definition
  protos/types.
- Add capability enums/names for inference management.
- Regenerate generated stubs. Do not hand-edit generated files.

### Phase 2: extract standalone inference subsystem

- Move shared semantic inference model/storage/accounting concepts into an
  inference subsystem.
- Keep semantic-specific index/vector logic in semantic.
- Add chat/generation connector interfaces alongside embeddings.
- Implement fake connector support for deterministic tests.

### Phase 3: implement profiles and centralized resolution

- Implement profile CRUD and storage.
- Implement resolver: profile -> capability -> credential grant -> policy ->
  secret -> connector.
- Persist policy decisions for allowed and denied requests.
- Keep policy evaluation fail-closed.

### Phase 4: convert semantic subsystem to consume inference

- Change semantic indexes to reference embedding profiles or capabilities.
- Route backfill/search query embedding through the inference runtime.
- Record usage events through the shared ledger.
- Remove duplicate grant/policy evaluators from semantic once centralized logic
  is in place.

### Phase 5: convert automation subsystem to consume inference

- Replace automation `model.provider/model` with `inference.profile` references.
- Remove daemon automation provider API key environment variables.
- Remove hard-coded cost estimation and max-cost policy fields.
- Route LLM steps through the inference runtime.
- Store profile/capability/request IDs on run records for debugging.

### Phase 6: CLI/docs/examples

- Rewrite inference CLI around endpoint/model/capability/profile/credential/grant
  /policy/usage nouns.
- Rewrite automation examples to use inference profiles.
- Update operations docs and admin UI assumptions.

### Phase 7: cleanup

- Delete obsolete automation provider code and pricing tables.
- Delete stale semantic-only inference helpers that duplicate centralized
  resolver/policy logic.
- Remove docs that imply application chat catalogs or product billing belong in
  mycel.

## Testing strategy

### Unit tests

- Profile validation:
  - required fields
  - operation compatibility
  - parameter bounds
  - domain allowlist behavior
- Credential validation:
  - owner type `principal|space|system`
  - secret refs are accepted/rejected by supported schemes
  - secret material is redacted from API/CLI output
- Grant matching:
  - space/domain/node/subtree specificity
  - usage mode matching
  - actor and on-behalf-of constraints
  - profile/capability/model/endpoint constraints
- Policy engine:
  - missing policy denies
  - deny/no-inference wins
  - restrict policies narrow allowed endpoints
  - local-only and third-party restrictions
  - token/request ceilings
- Resolver:
  - profile selector chooses enabled compatible capability
  - disabled endpoint/model/capability/credential fails closed
  - credential rotation keeps profile/automation stable

### Connector tests

- Fake chat connector returns deterministic text/JSON and token counts.
- Fake embedding connector returns deterministic vectors.
- OpenAI-compatible request formation is tested without real network calls.
- Provider errors map to stable internal status and usage events.

### Integration tests

- End-to-end automation run:
  - create space/domain/principal
  - install endpoint/model/capability
  - create credential
  - create profile
  - grant credential
  - create allow policy
  - register automation using profile
  - trigger graph change
  - verify graph action and usage event
- Policy denial automation run:
  - same setup but deny/no-inference policy
  - verify no provider call, denied decision, denied usage event, run failure
    reason
- Credential rotation:
  - run automation with credential v1
  - rotate credential
  - run automation again without changing automation definition
  - verify new secret version used and usage events distinguish versions if
    appropriate
- Principal-owned credential:
  - grant only on behalf of owner
  - verify another principal cannot trigger use unless grant explicitly allows
    it
- Semantic embedding path:
  - create embedding profile
  - create semantic index referencing profile
  - run backfill/search
  - verify shared usage ledger entries

### CLI tests

- Golden/help tests for systematic command tree.
- JSON output tests for list/get commands.
- Secret redaction tests.
- Scope flag consistency tests: `--space-id`, `--domain`, `--node`,
  `--include-descendants`.
- Automation definition validation rejects API keys and raw endpoint URLs.

### Authorization tests

- System admin can manage catalog and system credentials.
- Space admin can manage profiles/policies in their space but not system
  endpoints.
- A principal with only graph write cannot manage inference credentials.
- Usage telemetry read requires explicit read/audit capability.

### Durability/reload tests

- File store round trip for profiles/grants/policies/decisions.
- WAL/Raft apply tests for authoritative resource mutations where applicable.
- Snapshot/reload tests for inference resources and automation runs.
- Reference-safe delete tests for endpoint/model/capability/credential/profile
  references.

## Fit with existing mycel architecture

### Admin APIs

The Admin Inference API remains the management surface, but expands from
semantic embeddings to standalone inference resources. It should use unified
principal authorization and scoped capabilities.

### Daemon services

Inference should become a daemon subsystem with runtime init/start/stop behavior
consistent with identity, space, schema, graph, semantic, backup, and automation
subsystems. Connector adapters are runtime components; endpoint/profile/credential
configuration is daemon-owned data.

### Storage managers

The existing storage-manager pattern still applies:

```text
internal/inference/model
internal/inference/service
internal/inference/storage
internal/inference/accounting
internal/inference/connectors
```

Semantic and automation should stop duplicating inference storage/policy logic.

### Space/domain model

Profiles, grants, policies, and decisions are space-scoped and may restrict to
one or more domains. Domain refs should accept key or UUID at API/CLI boundaries
where possible.

### Principal/session model

All inference management and execution should use the unified principal model:

- administrators are principals with capabilities;
- automation workers are service/system principals;
- on-behalf-of principals are recorded for audit and grant resolution;
- credentials can be owned by principals, spaces, or the system.

### Semantic subsystem

Semantic indexing/search remains a mycel subsystem. It should use the inference
subsystem for embedding calls, credentials, policies, and usage events.

### Automation subsystem

Graph automation remains domain-scoped and graph-change-driven. It should use
inference profiles for LLM steps and should not know provider secrets. Run
records should point to inference usage events rather than carrying a separate
pricing/accounting model.

## Open questions and risks

1. **Profile scope**: should profiles be strictly space-scoped, or should domain
   profiles be first-class? This design prefers space-scoped profiles with
   domain constraints.
2. **Principal-owned credential consent**: what UX/API should a human principal
   use to grant their credential to a space automation? CLI may be enough first,
   but admin UI will need a safe consent model later.
3. **External secret managers**: which schemes should mycel support initially?
   If only `env://` and encrypted-inline are implemented, docs must say so.
4. **Rate limiting**: existing config hints at throttling, but real enforcement
   needs a concrete design for endpoint/profile/credential concurrency and rate
   limits.
5. **Local model support**: local endpoints are important for privacy, but each
   local connector may have different capabilities and health semantics.
6. **Usage ledger retention**: telemetry can grow quickly. Retention/export
   policy should be designed without becoming billing.
7. **Cluster ownership**: endpoint/credential/profile/policy resources need
   clear raft/WAL authority in clustered deployments.
8. **Automation graph authorization**: inference policy is not a substitute for
   graph read/write authorization. Automation service principals need a clear,
   testable graph authorization model.
9. **Structured output safety**: JSON-mode and tool-call capabilities should be
   explicitly modeled before workflow automations rely on them.
10. **Reference-safe deletes**: deleting profiles/capabilities/credentials must
    account for semantic indexes, automation definitions, pending invocations,
    runs, policy decisions, and usage events.
