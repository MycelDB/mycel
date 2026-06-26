# `mycel inference package apply`

Command that applies inference package definitions.

## Example

```sh
mycel inference package apply standard-openai.yaml
```

## Creates/updates

- model endpoint definitions
- model definitions
- model endpoint capabilities
- vector store definitions

Packages must not contain secrets.

Requires system access management privileges because package application mutates global inference metadata.
