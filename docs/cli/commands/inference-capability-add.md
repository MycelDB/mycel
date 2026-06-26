# `mycel inference capability add`

Target advanced command that provisions a model endpoint capability.

## Example

```sh
mycel inference capability add --model-endpoint openai-public --model openai/text-embedding-3-small --operation embeddings
```

## Notes

Capabilities are trusted as provisioned. Mycel does not probe endpoints automatically during planning.
