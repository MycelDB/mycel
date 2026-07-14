# `mycel embeddings generate`

Removed legacy MVP command.

Embedding generation is now daemon-owned semantic maintenance over semantic indexes. Use:

```sh
mycel semantic maintenance analyze --space-id <space_id>
mycel semantic maintenance process --space-id <space_id>
mycel semantic index backfill <semantic_index_id_or_key> --space-id <space_id>
```

Existing legacy embedding profile/key metadata can be converted with `mycel semantic migrate legacy-embeddings` while that migration path remains available.
