# Credentials and Credential Grants

Credentials authorize Mycel to call provisioned inference runtimes. Credential grants authorize where a credential may be used.

## Inference Credential

An inference credential stores authorization material for one runtime.

Examples:

- Martin's personal OpenAI key
- Martin's work OpenAI key
- an organization Azure OpenAI key
- a system credential for an enterprise-private gateway
- a local runtime token

Possible owner principal types:

```text
user
space
organization/tenant
system/deployment
```

A user can own many credentials, including multiple credentials for the same runtime.

Conceptual fields:

```text
id
key
name
runtime_id
owner_type
owner_id
auth_type              # api_key, bearer_token, none, service_account
secret_ref             # reference into encrypted secret storage
status                 # active, revoked, expired, disabled
is_default
created_at / updated_at
last_used_at
```

Secret values should be stored separately in an encrypted secret store or external secret manager. Runtime definitions and semantic indexes must never contain raw secret values.

## Credential Grant

Credential ownership alone is not enough. Mycel also needs to know where a credential is authorized to be used.

A credential grant is an atomic statement:

```text
This one credential may be used for these operation(s) in this processing scope.
```

The cardinality should be:

```text
one grant -> one credential -> one scope
```

Many grants may target the same scope, and one credential may have many grants.

Conceptual fields:

```text
id
credential_id
scope                  # space/domain/semantic-index/node/subtree
operations             # embeddings, chat, rerank, summarize
runtime_id             # optional but recommended constraint
model_id               # optional constraint
priority
is_default
granted_by
created_at
expires_at
```

Scope fields:

```text
space_id
domain_id
semantic_index_id
node_id
include_descendants
```

## Credential Resolution Is Not Index Selection

A semantic query is planned over semantic indexes and vector spaces, not over credential grants.

Credential resolution happens after Mycel determines that it needs to call a runtime, for example to:

- generate content embeddings during backfill/refresh
- generate query embeddings for a selected vector space
- call a chat/rerank/summarization runtime

The planning direction is:

```text
query scope -> semantic indexes -> vector spaces -> runtime calls -> credential resolution
```

not:

```text
query scope -> credential grants -> indexes
```

## Grant Resolution

When a runtime call requires a credential, Mycel resolves applicable grants using:

```text
processing scope
operation
runtime
model
credential status
principal access
```

Recommended specificity order:

1. node/subtree grant
2. semantic index grant
3. domain grant
4. space grant
5. owner default credential for the runtime
6. organization/system default credential for the runtime

Rules:

- the grant operation must match the requested operation
- runtime/model constraints must match when present
- expired, revoked, disabled, or inaccessible credentials are ignored
- the most specific compatible grant wins
- same-specificity conflicts should error unless exactly one grant is default or has highest priority
- credential grants never override inference/content policy restrictions

Resolution should return both:

```text
selected credential
selected grant
```

so embedding/query records can be audited.

## Examples

Domain-level grant:

```yaml
credential: martin-openai
scope:
  space: Personal PKM
  domain: personal-pkm
operations:
  - embeddings
runtime: openai-public
model: openai/text-embedding-3-small
```

Node/subtree grant:

```yaml
credential: martin-local-ollama
scope:
  node: private-journal-root
  includeDescendants: true
operations:
  - embeddings
  - chat
runtime: local-ollama
```

## Audit Requirements

Embedding records and durable query/maintenance logs should retain provenance:

```text
credential_id
credential_grant_id
runtime_id
model_id
semantic_index_id
policy_decision_id, when applicable
```

This lets operators answer:

- which credential generated this embedding?
- which grant authorized it?
- which content scopes use this credential?
- what must be refreshed or disabled if a credential is revoked?
