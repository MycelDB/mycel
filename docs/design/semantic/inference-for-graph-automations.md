# Intelligence Access for Graph Automations and Semantic Rules

## Status

Implemented direction for shared AI/embedding access control. The shared
profile, credential, grant, policy, decision, and usage concept is named
**Intelligence Access**.

## Problem

mycel-owned workloads need provider access without embedding credentials or model
selection directly into every workload definition. The main daemon-owned
workloads are:

- semantic generation rules and semantic search;
- graph automations;
- future internal AI maintenance jobs.

Operators need one place to configure provider endpoints, models, vector stores,
credentials, grants, policies, and usage attribution.

## Goals

- Let semantic rules and automations reference stable Intelligence Access
  profiles instead of raw endpoint/model/credential IDs.
- Support background use through explicit credential grants and policies.
- Attribute usage to the workload, actor/on-behalf-of principal, space, domain,
  semantic rule/binding or automation run, and resolved provider resources.
- Keep secrets out of graph content, semantic rule definitions, automation
  definitions, usage responses, and logs.
- Make credential rotation possible without editing semantic rules or
  automations.

## Core concepts

| Concept | Scope | Purpose |
| --- | --- | --- |
| Model endpoint | system | Provider connector endpoint such as `openai-compatible` or `ollama`. |
| Model | system | Provider model/capability metadata. |
| Vector store | system | Storage backend for embedding/vector search. |
| Intelligence profile | space, optionally domain-limited | Stable runtime reference used by semantic rules and automations. |
| Credential | system/user/space | Encrypted provider credential material. |
| Credential grant | space/domain/workload scope | Allows a workload to use a credential, including background use when explicit. |
| Policy | system/space/domain/workload scope | Allows or denies operations such as embeddings or chat. |
| Policy decision | per request | Auditable authorization result. |
| Usage event | per request | Token/latency/error attribution. |

## Workload attribution

Usage and policy decisions include workload-specific references when present:

- workload type: `semantic_rule`, `semantic_search`, `automation`, or future
  internal workload;
- semantic rule ID and embedding binding key;
- automation ID and automation run ID;
- space ID, domain ID, and target node ID when applicable;
- actor principal and on-behalf-of principal;
- resolved profile, endpoint, model, credential grant, and policy decision IDs.

Older index-oriented workload terminology is not part of the current public
model. Use semantic rule and binding attribution for semantic generation and
search.

## Semantic rule flow

1. Dirty analysis identifies a target for a semantic generation rule and
   embedding binding.
2. The worker assembles source text from the rule source policy.
3. Intelligence Access resolves the binding's profile and vector store.
4. Credential grants and policies are evaluated for background embedding use.
5. If allowed, the provider call is made and usage is recorded with
   `semantic_rule_id` and `embedding_binding_key`.
6. The semantic subsystem writes the vector record and updates the derived
   physical search index.

Semantic search follows the same profile/policy path for query embeddings and
records usage with rule/binding provenance.

## Automation flow

1. The automation scheduler executes a graph automation as the service actor on
   behalf of the automation owner or triggering user, depending on policy.
2. The automation action references an Intelligence Access profile.
3. Grants and policies are evaluated for the requested operation.
4. Usage is recorded with automation and run IDs.

## Authorization and safety

- Background use requires explicit grants that allow background execution.
- Policy denial happens before provider calls.
- Usage responses and diagnostics must not expose API keys, decrypted secrets,
  raw provider requests, raw provider responses, raw embedding vectors, or raw
  semantic source text.
- User backups must not export plaintext passwords, active sessions/tokens, or
  decrypted Intelligence Access credentials.

## Ownership boundaries

| Area | Owning subsystem |
| --- | --- |
| semantic generation rules, vector records, physical search indexes | semantic subsystem |
| graph automation definitions, scheduling, runs | automation subsystem |
| profiles, credentials, grants, policies, decisions, usage | inference / Intelligence Access subsystem |
| graph visibility and committed node reads | graph/domain/session subsystems |

Semantic-specific vector logic remains in the semantic subsystem. Shared provider
catalog, credential, grant, policy, and usage mechanics remain in Intelligence
Access so semantic rules and automations behave consistently.
