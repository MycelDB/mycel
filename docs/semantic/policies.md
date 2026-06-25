# Inference Policies

Inference policies control whether graph content may be processed by model endpoints.

They are separate from credentials:

```text
CredentialGrant = authorization to use a secret
InferencePolicy = authorization/restriction for content to be processed
```

A credential grant may allow an OpenAI key in a space, while a node-level policy may still forbid sending a private subtree to any third-party model endpoint. The content policy must win.

## Policy Scopes

Inference policies are owned by the space whose content they govern. They should be provisioned with the space and stored with the space's semantic metadata.

Policies can be attached to:

```text
space
domain
semantic index
node
subtree rooted at a node
```

Scope fields:

```text
space_id              # implied by owning space storage; retained for validation if present
domain_id
semantic_index_id
node_id
include_descendants
```

## Common Policies

### No inference

```text
no_inference
```

Content must not be processed by embeddings, chat, rerank, summarization, or classification.

### Local only

```text
local_only
```

Content may only be processed by local model endpoints.

### Enterprise private only

```text
enterprise_private_only
```

Content may only be processed by local or enterprise-private model endpoints.

### Deny operation

```text
deny embeddings
deny chat
```

Content may not be processed for a specific operation.

### Allow operation

```text
allow embeddings
```

Content may be processed for a specific operation or endpoint class, subject to more specific denies.

## Conceptual Fields

```text
id
scope
effect                  # allow, deny, restrict
operations
no_inference
allowed_privacy_classes
disallow_third_party
require_local_endpoint
reason
created_by
created_at
expires_at
```

## Policy Resolution

Policy resolution happens before credential resolution and before any model endpoint call.

Recommended order:

1. resolve the graph content scope being processed
2. collect policies inherited from space, domain, semantic index, node ancestors, and explicit subtree policies
3. apply the most restrictive effective policy
4. remove semantic indexes/model endpoints/models that violate the policy
5. resolve credential grants only for remaining allowed model endpoint calls
6. execute and record policy/credential provenance

Rules:

- deny beats allow
- `no_inference` excludes content from all inference operations
- `local_only` excludes third-party and enterprise-private remote model endpoints unless they are classified as local
- node/subtree policies override broader domain/space allowances
- semantic index source selection must skip nodes disallowed by effective policy
- a semantic query over a broad scope may partially search allowed indexes and return warnings for skipped indexes/content

## Example

Given:

```text
space grant allows OpenAI
space policy allows third-party embeddings
node policy marks a subtree local_only
```

Result:

```text
OpenAI semantic indexes must skip that subtree.
A local semantic index may include it if its endpoint/model/vector store satisfy policy.
```

## Policy Decisions

Policy evaluation should produce an inspectable decision when content is embedded or durably skipped.

Conceptual fields:

```text
id
scope
operation
model_endpoint_id
model_id
allowed
matched_policy_ids
reason
created_at
```

Policy decision records are useful for:

- debugging missing embeddings
- explaining skipped content
- proving private content was not sent to a third-party model endpoint
- auditing embedding provenance

Low-level storage for policies and policy decisions is documented in [../storage/semantic.md](../storage/semantic.md).
