# `mycel inference credential add`

Target advanced command that stores credential metadata and secret material for a model endpoint.

## Example

```sh
OPENAI_API_KEY=sk-... mycel inference credential add martin-openai --model-endpoint openai-public --owner-user martin --auth api-key --api-key-env OPENAI_API_KEY
```

## Notes

A credential alone does not authorize content processing. A space-owned credential grant is required for every endpoint call.
