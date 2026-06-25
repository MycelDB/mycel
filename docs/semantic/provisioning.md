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

Mycel should not silently create provider-specific runtime/model definitions for every installation.

### Mycel CLI / Operator

The CLI/operator provisions:

- inference packages
- runtime definitions
- model definitions
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
- local/private runtime endpoints
- privacy constraints
- allowed providers/runtimes
- credential grants
- local-only or no-inference subtrees

Credential grants and inference/content policies belong to the owning space. For a system like Knot PKM, provisioning a `Personal PKM` space should also provision that space's semantic indexes, grants, and policies.

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

These packages create runtime/model/vector-store definitions. They must not contain secrets.

### 3. Create user, space, domain, and templates

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

The semantic index binding describes runtime/model/vector-store infrastructure. It does not contain a credential/API key.

### 6. Grant credential use

```sh
mycel inference credential grant martin-openai \
  --space-id <space-id> \
  --domain personal-pkm \
  --semantic-index notes-search \
  --operation embeddings
```

### 7. Optionally add content policy

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

### 8. Backfill index

```sh
mycel semantic index backfill notes-search \
  --space-id <space-id> \
  --domain personal-pkm
```

Backfill evaluates inference policy before each embedding and resolves a compatible credential grant before each runtime call.

### 9. Search

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
- runtimes/models/vector stores upsert by key
- semantic indexes upsert by `space_id + domain_id + key`
- credentials may require explicit update to replace secrets
- grants upsert by `credential + scope + operations + runtime/model constraints`
- policies upsert by explicit policy ID or deterministic scope/effect key

## Current MVP Equivalent

The current CLI uses lower-level embedding commands. See [current-mvp.md](current-mvp.md).

The advanced commands above are target design, not all implemented today.
