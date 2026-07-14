# `mycel embeddings catalog`

Removed legacy MVP command.

Use daemon inference and semantic resources instead:

```sh
mycel inference package apply <package.yaml>
mycel inference capability add ...
mycel semantic index add ...
```

The built-in catalog remains an internal migration/provisioning helper, but the `mycel embeddings ...` command tree is no longer registered.
