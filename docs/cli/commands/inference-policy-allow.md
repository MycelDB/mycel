# `mycel inference policy allow`

Command that creates an inference policy allowing processing within explicit constraints.

## Example

```sh
mycel inference policy allow --space-id <space_id> --domain personal-pkm --operation embeddings --privacy-class local_only --privacy-class enterprise_private --privacy-class third_party
```

## Notes

No inference is allowed unless an applicable policy explicitly allows it. Requires admin access to the target space because policies are space-owned processing rules.
