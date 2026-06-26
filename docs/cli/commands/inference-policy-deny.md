# `mycel inference policy deny`

Target advanced command that creates an inference policy denying processing.

## Example

```sh
mycel inference policy deny --space-id <space_id> --domain personal-pkm --node <private-node-id> --include-descendants --operation embeddings --operation chat
```

## Notes

Deny always wins over allow.
