# `mycel embeddings generate`

Current MVP command that generates embeddings for one node or a selected batch.

## Examples

```sh
mycel embeddings generate --space-id <space_id> --node <node_id> --profile <profile_id> -u USER -p PASSWORD
mycel embeddings generate --space-id <space_id> --profile <profile_id> --template-key logseq.page --limit 500 -u USER -p PASSWORD
```

## Notes

Batch generation requires `--node`, `--template-key`, or `--contains`. It skips current embeddings by source hash unless `--force` is set.
