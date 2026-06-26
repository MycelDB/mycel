# Semantic Provisioning

Provisioning is split across Mycel, operators, applications, and users.

CLI syntax is documented in the CLI command reference under [`../cli/commands/`](../cli/commands/). This document describes responsibilities and flow, not command syntax.

## Responsibility Split

### Mycel Library

Mycel owns:

- resource schemas
- stores
- validation rules
- connector interfaces
- generation/search execution contracts
- stale record semantics
- policy checks
- query planning contracts
- accounting event contracts

Mycel should not silently create provider-specific endpoint/model definitions for every installation.

### Mycel CLI / Operator

The CLI/operator provisions:

- inference packages
- model endpoint definitions
- model definitions
- model endpoint capabilities
- vector store backends
- credentials
- space-owned credential grants
- space-owned inference/content policies
- semantic indexes
- backfill/maintenance jobs
- accounting index/rollup maintenance

### Application Using Mycel

The application provisions semantic intent:

- graph templates
- spaces/domains with application meaning
- semantic index definitions
- default source policies
- index purposes
- refresh behavior
- baseline space inference policies

For Knot PKM, this includes indexes over templates such as:

```text
logseq.journal
logseq.page
app.task
```

### User / Organization / Deployment

The user or operator provisions authority and policy:

- API keys
- local/private model endpoints
- privacy constraints
- allowed providers/model endpoints
- credential grants
- local-only or no-inference subtrees

Credential grants and inference/content policies belong to the owning space. For a system like Knot PKM, provisioning a `Personal PKM` space should also provision that space's semantic indexes, grants, and policies.

Because Mycel's fallback is no inference when no applicable policy exists, application space provisioning should include an explicit baseline inference policy for the intended privacy posture.

## Example Provisioning Flow

### 1. Initialize Mycel

Initialize the data directory and initial administrator.

CLI: [init](../cli/commands/init.md)

`mycel init` should create and enable the built-in `mycel-file` vector store instance, so external vector stores only need provisioning when an installation wants Qdrant, pgvector, Pinecone, or another backend.

### 2. Apply inference definitions

Apply inference packages for providers/endpoints/models/vector stores.

CLI: [inference package apply](../cli/commands/inference-package-apply.md)

Packages create model endpoint, model, model endpoint capability, and vector-store definitions. They must not contain secrets.

### 3. Create user, space, domain, and templates

Create principals, content boundaries, and graph template definitions.

CLI:

- [user add](../cli/commands/user-add.md)
- [space add](../cli/commands/space-add.md)
- [template import](../cli/commands/template-import.md)

### 4. Verify endpoint/model capability

Before an index can use a model endpoint and model, an enabled global capability must exist.

CLI: [inference capability add](../cli/commands/inference-capability-add.md)

Mycel trusts provisioned capabilities; it should not probe endpoints automatically. If a capability is missing or disabled, a semantic index binding using that endpoint/model/operation is invalid or inactive.

### 5. Add user credential

Create credential metadata and secret material for a model endpoint.

CLI: [inference credential add](../cli/commands/inference-credential-add.md)

### 6. Create semantic index

Create the semantic source policy and endpoint/model/vector-store binding.

CLI: [semantic index add](../cli/commands/semantic-index-add.md)

The semantic index binding describes endpoint/model/vector-store infrastructure. It does not contain a credential/API key.

### 7. Grant credential use

Create an explicit space-owned grant authorizing a credential for a processing scope.

CLI: [inference credential grant](../cli/commands/inference-credential-grant.md)

The explicit grant is required for every endpoint call. Background semantic maintenance requires a grant that allows background use.

### 8. Add baseline content policy

No inference is allowed unless an applicable policy explicitly allows it. Space provisioning should create a baseline policy matching the intended privacy posture.

CLI:

- [inference policy allow](../cli/commands/inference-policy-allow.md)
- [inference policy deny](../cli/commands/inference-policy-deny.md)
- [inference policy restrict](../cli/commands/inference-policy-restrict.md)

### 9. Backfill index

Backfill evaluates inference policy before each embedding and resolves a compatible credential grant before each model endpoint call.

CLI: [semantic index backfill](../cli/commands/semantic-index-backfill.md)

Every model endpoint call during backfill appends an inference usage accounting event.

### 10. Search

Plan and execute a semantic search over one or more semantic indexes.

CLI: [semantic search](../cli/commands/semantic-search.md)

Query embedding endpoint calls append inference usage accounting events.

### 11. Report usage

Summarize or inspect token usage from the append-only inference accounting ledger.

CLI:

- [accounting usage summarize](../cli/commands/accounting-usage-summarize.md)
- [accounting usage events](../cli/commands/accounting-usage-events.md)
- [accounting usage export](../cli/commands/accounting-usage-export.md)

## Idempotency

Provisioning commands should be safe to repeat where possible.

Recommended behavior:

- packages apply by name/version/checksum
- model endpoints/models/vector stores upsert by key
- model endpoint capabilities upsert by `model_endpoint + model + operation`
- semantic indexes upsert by `space_id + domain_id + key`
- credentials may require explicit update to replace secrets
- grants upsert by `credential + scope + operations + endpoint/model constraints`
- policies upsert by explicit policy ID or deterministic scope/effect key

## Current MVP Equivalent

The current CLI uses lower-level embedding commands. See [current-mvp.md](current-mvp.md) and the current MVP embedding command docs under [`../cli/commands/`](../cli/commands/).

The advanced commands above are target design, not all implemented today.
