# `mycel embeddings search`

Removed legacy MVP command.

Profile-based embedding search has been replaced by daemon semantic-index search:

```sh
mycel semantic search --space-id <space_id> --domain default --text "sleep and focus"
```

Provision semantic indexes and inference credentials/grants with the `semantic` and `inference` command groups. Existing legacy embedding profile/key metadata can be converted with `mycel semantic migrate legacy-embeddings` while that migration path remains available.
