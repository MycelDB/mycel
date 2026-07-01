# `mycel inference credential grant`

Command that creates a space-owned grant authorizing one credential for a processing scope.

## Example

```sh
mycel inference credential grant martin-openai --space-id <space_id> --domain personal-pkm --semantic-index notes-search --operation embeddings --allow-background-use
```

## Notes

- Every endpoint call requires an explicit grant.
- `--allow-background-use` permits offline semantic maintenance within the grant scope.
- Requires admin access to the target space because grants are space-owned processing authorization.
