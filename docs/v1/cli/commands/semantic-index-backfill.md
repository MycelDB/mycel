# `mycel semantic index backfill`

Runs the initial semantic-index backfill path for a provisioned semantic index.

Phase 5 runs backfill synchronously from the CLI. Later phases will add dirty queues and background workers.

## Usage

```sh
mycel semantic index backfill INDEX \
  --space-id <space_id> \
  --domain <domain-key-or-id> \
  [--node <node_id>] \
  [--limit 100] \
  [--force] \
  [--continue-on-error]
```

`INDEX` may be a semantic index key or ID.

## Example

```sh
mycel semantic index backfill notes-search \
  --space-id <space_id> \
  --domain personal-pkm
```

## Behavior

Backfill:

- selects roots from explicit `--node` values or the semantic index `source_policy.template_keys`
- removes nested selected roots so one semantic index does not embed both an ancestor and descendant root
- supports `self` and `subtree` source extraction
- hashes source text and skips records whose current source hash already exists unless `--force` is set
- requires an applicable inference policy; no policy means no inference
- requires an explicit credential grant with `allow_background_use = true`
- emits inference accounting events for model endpoint calls
- writes advanced records through the built-in `mycel-file` vector backend

Only `mycel-file` vector stores are supported by this initial command.

## Notes

The old MVP `mycel embeddings ...` profile flow has been removed. Semantic index backfill and maintenance are the supported embedding-generation paths.
