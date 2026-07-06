# `mycel semantic maintenance process`

Processes pending semantic dirty work explicitly from the CLI.

## Usage

```sh
mycel semantic maintenance process \
  --space-id <space_id> \
  [--limit 1]
```

## Behavior

The worker reads pending items from `graphs/<space_id>/semantic/dirty_queue.json`.

- `refresh` and `backfill` items run through the semantic index backfill path.
- `delete` and `cleanup` items append local tombstone/delete records through the configured vector backend.
- item status, attempts, and errors are persisted back to the dirty queue.

No background daemon is started by default in Phase 7.
