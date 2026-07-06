# `mycel embeddings profiles add`

Removed legacy MVP command.

Embedding profiles have been replaced by daemon semantic indexes plus inference credentials/grants:

```sh
mycel semantic index add ...
mycel inference credential add ...
mycel inference credential grant ...
```

Existing legacy provider-key/profile metadata can be converted with `mycel semantic migrate legacy-embeddings` while that migration path remains available.
