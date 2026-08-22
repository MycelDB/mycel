# Admin Semantic API

## Status

Implemented daemon-oriented Admin Semantic API for semantic generation rules.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/admin/v1/semantic.proto
```

## Purpose

`AdminSemanticService` manages semantic generation rules through the operator-facing Admin API.
A semantic generation rule is the durable configuration that selects graph nodes,
assembles source text, resolves Intelligence Access profiles, maintains embedding
records, and exposes physical per-rule/per-binding search-index health.

The public Admin API does not expose the former semantic-index-as-resource model.
Use "search index" only for the derived physical per-binding vector search index
that is rebuilt from durable rule vector records.

## Scope

Implemented rule lifecycle RPCs:

- `ListSemanticRules`
- `GetSemanticRule`
- `ValidateSemanticRule`
- `CreateSemanticRule`
- `UpdateSemanticRule`
- `SetSemanticRuleEnabled`
- `DeleteSemanticRule`

Rule responses include rule identity, display metadata, enabled/state fields,
embedding binding summaries, maintenance status, and physical search-index status
where available. Validation returns diagnostics suitable for CLI and Console
presentation before a rule is persisted.

`DeleteSemanticRule` is explicit and reference-safe. `purge_vectors` removes
rule vector records and derived physical search-index state for the deleted rule.
Credential/secret management, profile discovery, grants, policies, and usage
summaries are implemented separately in [Admin Inference API](inference.md) and
the Intelligence Access API.

Semantic backfill and maintenance analyze/process are implemented separately in
[Admin Semantic Maintenance API](semantic-maintenance.md).

## Authorization

Admin Semantic API methods require an operator bearer token and semantic
management/maintenance capability. The built-in `semantic_admin` operator role
and bootstrap/system admins grant the required capabilities in the current daemon
implementation.

## Rule shape

Operators author rules as structured JSON/YAML. A common node-type rule looks
like:

```yaml
space_id: <space-id>
domain_id: <domain-id>
key: notes-search
display_name: Notes Search
enabled: true
trigger:
  events: [changed]
  labels: [Note]
selector:
  mode: node_type
  labels: [Note]
source:
  mode: self
embeddings:
  - key: search
    purpose: search
    intelligence_profile: embedding-default
    vector_store: mycel-file
    enabled: true
storage:
  searchable: true
  physical_index: exact
```

Embedding bindings reference Intelligence Access profiles and vector stores by
stable key or ID. User-authored rules do not directly own provider endpoint,
model, capability, or credential IDs; those are resolved and attributed at
execution time.

## CLI

Daemon-backed rule management is available when `--daemon-addr` is supplied:

```sh
./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule validate --file examples/semantic/notes-rule.json

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule create --file examples/semantic/notes-rule.json

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule list --space-id '<space-id>' --domain default

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule update notes-search --space-id '<space-id>' --file examples/semantic/notes-rule.json

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule disable notes-search --space-id '<space-id>'

./bin/mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  semantic rule delete notes-search --space-id '<space-id>' --purge-vectors
```

With AdminDomainService in place, daemon-mode semantic commands can resolve
`--domain` by UUID, stable key such as `default`, or the empty/default domain
reference.
