# `mycel semantic maintenance analyze`

Analyzes raw graph dirty events into coalesced semantic dirty work.

## Usage

```sh
mycel semantic maintenance analyze \
  --space-id <space_id> \
  [--index <semantic-index-key-or-id>] \
  [--limit 100]
```

## Behavior

The analyzer:

- reads `graphs/<space_id>/semantic/events/graph-dirty-000001.ksem`
- processes events after each semantic index checkpoint
- enqueues dirty work in `graphs/<space_id>/semantic/dirty_queue.json`
- coalesces by `semantic_index_id + target_node_id`
- updates `graphs/<space_id>/semantic/index_state.json`

This command does not call model endpoints.
