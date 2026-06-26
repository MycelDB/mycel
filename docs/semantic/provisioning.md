# Semantic Provisioning

Provisioning is split across Mycel, operators, applications, and users.

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

### Application Using Mycel

The application provisions semantic intent:

- graph templates
- spaces/domains with application meaning
- semantic index definitions
- default source policies
- index purposes
- refresh behavior

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

```sh
mycel -d /data/mycel init
```

This initializes storage and system stores only.

### 2. Apply inference definitions

```sh
mycel inference package apply standard-openai.yaml
mycel inference package apply local-ollama.yaml
```

These packages create model endpoint, model, model endpoint capability, and vector-store definitions. They must not contain secrets.

`mycel init` should create and enable the built-in `mycel-file` vector store instance, so external vector stores only need provisioning when an installation wants Qdrant, pgvector, Pinecone, or another backend.

### 3. Create user, space, domain, and templates

```sh
mycel user add martin --password pass
mycel space add "Personal PKM" --owner martin --default-domain personal-pkm
mycel template import logseq-journal.json
```

### 4. Verify endpoint/model capability

Before an index can use a model endpoint and model, an enabled global capability must exist:

```sh
mycel inference capability add \
  --model-endpoint openai-public \
  --model openai/text-embedding-3-small \
  --operation embeddings
```

Packages may create this automatically for standard model endpoints. Mycel trusts provisioned capabilities; it should not probe endpoints automatically. If a capability is missing or disabled, a semantic index binding using that endpoint/model/operation is invalid or inactive.

### 5. Add user credential

```sh
OPENAI_API_KEY=sk-... mycel inference credential add martin-openai \
  --model-endpoint openai-public \
  --owner-user martin \
  --auth api-key \
  --api-key-env OPENAI_API_KEY
```

### 6. Create semantic index

```sh
mycel semantic index add notes-search \
  --space-id <space-id> \
  --domain personal-pkm \
  --purpose semantic_search \
  --template-key logseq.journal \
  --template-key logseq.page \
  --source subtree \
  --model-endpoint openai-public \
  --model openai/text-embedding-3-small \
  --vector-store mycel-file
```

The semantic index binding describes endpoint/model/vector-store infrastructure. It does not contain a credential/API key.

### 7. Grant credential use

```sh
mycel inference credential grant martin-openai \
  --space-id <space-id> \
  --domain personal-pkm \
  --semantic-index notes-search \
  --operation embeddings \
  --allow-background-use
```

The explicit grant is required for every endpoint call. `--allow-background-use` permits offline semantic maintenance, such as backfill and dirty refresh, to use this credential within the grant scope without requiring a live user session.

### 8. Add baseline content policy

No inference is allowed unless an applicable policy explicitly allows it. Space provisioning should create a baseline policy matching the intended privacy posture.

Example broad personal-space allow policy:

```sh
mycel inference policy allow \
  --space-id <space-id> \
  --domain personal-pkm \
  --operation embeddings \
  --privacy-class local_only \
  --privacy-class enterprise_private \
  --privacy-class third_party
```

Block a private subtree from all inference processing:

```sh
mycel inference policy deny \
  --space-id <space-id> \
  --domain personal-pkm \
  --node <private-node-id> \
  --include-descendants \
  --operation embeddings \
  --operation chat
```

Require local processing for a subtree:

```sh
mycel inference policy restrict \
  --space-id <space-id> \
  --domain personal-pkm \
  --node <private-node-id> \
  --include-descendants \
  --local-only
```

### 9. Backfill index

```sh
mycel semantic index backfill notes-search \
  --space-id <space-id> \
  --domain personal-pkm
```

Backfill evaluates inference policy before each embedding and resolves a compatible credential grant before each model endpoint call.

### 10. Search

```sh
mycel semantic search \
  --space-id <space-id> \
  --domain personal-pkm \
  --index notes-search \
  --text "sleep, exercise, and focus"
```

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

The current CLI uses lower-level embedding commands. See [current-mvp.md](current-mvp.md).

The advanced commands above are target design, not all implemented today.
