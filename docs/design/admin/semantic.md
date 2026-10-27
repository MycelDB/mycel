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

A rule UUID is its authoritative identity. A non-empty `display_name` is an
operator-facing label and must be unique within the rule's `space_id` and
`domain_id` after trimming surrounding whitespace and case-folding for
comparison. The authored casing is preserved. The same display name may be used
in another domain or space, and blank display names remain available for callers
that use the rule key as their visible fallback. Display names are never used as
durable foreign keys.

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

The Admin gRPC API uses the enum-only `trigger.events` field. Omitting the set
defaults to `CHANGED`; explicit `UNSPECIFIED` and unknown numeric values are
rejected. Legacy string events and the transitional `trigger.event_types` field
are not accepted or projected. Persisted daemon state and CLI rule files remain
transport-neutral and use canonical event names rather than generated protobuf
values. Persisted catalogs,
Raft snapshots, and restored backups containing duplicate normalized non-empty
rule display names in one space/domain are rejected rather than repaired or
silently renamed.

Embedding bindings reference Intelligence Access profiles and vector stores by
ID whenever available. A vector-store UUID is the authoritative durable
reference; its unique display name is only an operator-facing label and may be
renamed without changing rules. Legacy key-only references remain readable, but
ambiguous key resolution fails closed and new operator tooling should select from
the catalog and persist `vector_store_id`. User-authored rules do not directly
own provider endpoint, model, capability, or credential IDs; those are resolved
and attributed at execution time.

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
