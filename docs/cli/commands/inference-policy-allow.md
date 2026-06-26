# `mycel inference policy allow`

Target advanced command that creates an inference policy allowing processing within explicit constraints.

## Example

```sh
mycel inference policy allow --space-id <space_id> --domain personal-pkm --operation embeddings --privacy-class local_only --privacy-class enterprise_private --privacy-class third_party
```

## Notes

No inference is allowed unless an applicable policy explicitly allows it.
