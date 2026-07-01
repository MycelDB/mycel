# `mycel inference credential add`

Command that stores credential metadata and secret material for a model endpoint.

## Example

```sh
OPENAI_API_KEY=sk-... mycel inference credential add martin-openai --model-endpoint openai-public --owner-user martin --auth api_key --api-key-env OPENAI_API_KEY
```

## Notes

A credential alone does not authorize content processing. A space-owned credential grant is required for every endpoint call.

Requires system access management privileges because credentials are global/principal-level metadata. Inline secrets require `--user-store-encryption-key-b64` or `MYCELDB_USER_STORE_ENCRYPTION_KEY_B64`; otherwise use `--external-ref`.
