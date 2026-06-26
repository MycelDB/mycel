# Inference Policies

Inference policies control whether graph content may be processed by model endpoints.

They are separate from credentials:

```text
CredentialGrant = authorization to use a secret
InferencePolicy = authorization/restriction for content to be processed
```

A credential grant may allow an OpenAI key in a space, while a node-level policy may still forbid sending a private subtree to any third-party model endpoint. The content policy must win.

Default behavior is deny:

```text
no applicable inference policy => no inference is allowed
```

Applications/operators must provision an explicit space-owned baseline policy when they create/configure a space. A semantic index plus credential grant is not sufficient; policy must explicitly allow processing.

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
2. collect policies inherited from space, domain, semantic index, containment ancestors, and explicit subtree policies
3. apply the most restrictive effective policy
4. remove semantic indexes/model endpoints/models that violate the policy
5. resolve credential grants only for remaining allowed model endpoint calls
6. execute and record policy/credential provenance

Rules:

- no applicable inference policy means deny/no inference
- deny beats allow, including inherited deny policies and more-specific allow policies
- at least one applicable allow/restrict policy must match the operation for processing to proceed
- multiple restrict policies combine by intersection / most restrictive result
- `allowed_privacy_classes` sets are intersected across applicable allow/restrict policies
- restrictive booleans such as `disallow_third_party` and `require_local_endpoint` are combined with OR semantics; if any applicable policy requires them, the effective policy requires them
- `no_inference` excludes content from all inference operations
- `local_only` excludes third-party and enterprise-private remote model endpoints unless they are classified as local
- policies inherit only through containment edges
- non-containment edges such as references, backlinks, tags, mentions, and embeds do not imply policy inheritance
- node/subtree policies override broader domain/space allowances by narrowing the effective policy; they cannot loosen inherited restrictions
- semantic index extraction must stop at nodes/subtrees disallowed by effective policy for the index endpoint/model
- a semantic query over a broad scope may partially search allowed indexes and return warnings for skipped indexes/content

## Containment Moves

When a node moves into or out of a restricted subtree, the graph write should not synchronously decide embedding cleanup or regeneration. The graph transaction records the move and appends a raw dirty event with old/new containment context.

The semantic analyzer/maintainer later evaluates policy and source roots for the old and new locations. It may dirty the old containing source root, dirty the new containing source root, refresh moved content, skip newly restricted content, tombstone records, or delete derived vectors according to the effective policy and cleanup rules.

## Example

Given:

```text
space grant allows OpenAI
space policy allows embeddings for local_only + enterprise_private + third_party
domain policy restricts embeddings to local_only + enterprise_private
node policy marks a subtree local_only
```

Effective result for that subtree:

```text
allowed_privacy_classes = local_only
require_local_endpoint = true, if set by node policy
third_party endpoints are disallowed
OpenAI semantic indexes must stop traversal at that subtree and exclude its contents from extraction.
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
