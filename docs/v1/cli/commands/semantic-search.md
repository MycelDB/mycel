# `mycel semantic search`

Searches one or more provisioned semantic indexes using the Phase 6 semantic query planner.

## Usage

```sh
mycel semantic search \
  --space-id <space_id> \
  --domain <domain-key-or-id> \
  --text "sleep, exercise, and focus" \
  [--index notes-search] \
  [--limit 10] \
  [--min-score 0.2]
```

`--index` may be repeated and accepts a semantic index key or ID. Index keys are resolved inside the requested domain.

## Example

```sh
mycel semantic search \
  --space-id <space_id> \
  --domain personal-pkm \
  --index notes-search \
  --text "sleep, exercise, and focus"
```

## Behavior

The planner:

- selects enabled semantic indexes in the requested space/domain
- optionally narrows to `--index` values
- validates endpoint/model/capability bindings
- applies default-deny inference policy filtering
- resolves an explicit compatible credential grant for query embedding calls
- groups compatible indexes by vector space plus endpoint/model/grant binding
- generates one query embedding per compatible group
- searches the built-in `mycel-file` vector backend
- returns result provenance and warnings for skipped indexes/groups
- avoids blind global score truncation across incompatible groups

Query embedding calls append inference accounting events with reason `semantic_query`.

## Notes

Semantic search uses daemon semantic indexes. The older MVP `mycel embeddings search` command has been removed; migrate legacy profile/key metadata with `semantic migrate legacy-embeddings` where needed.
