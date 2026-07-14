# `mycel inference package apply`

Command that applies inference package definitions.

## Example

```sh
mycel inference package apply examples/inference/standard-openai-embeddings.json
```

Example semantic/embedding packages are available under `examples/inference/`:

- `standard-openai-embeddings.json`

Application chat catalogs/packages belong in applications such as Knot PKM, not in MycelDB inference packages.

## Creates/updates

- model endpoint definitions
- model definitions
- model endpoint capabilities
- vector store definitions

Packages must not contain secrets.

Requires system access management privileges because package application mutates global inference metadata.
