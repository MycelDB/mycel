# `mycel inference capability add`

Command that provisions a model endpoint capability.

## Example

```sh
mycel inference capability add --model-endpoint openai-public --model openai/text-embedding-3-small --operation embeddings
```

## Notes

Capabilities are trusted as provisioned. Mycel does not probe endpoints automatically during planning.

Requires system access management privileges because capabilities are global inference metadata.
