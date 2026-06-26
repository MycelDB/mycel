# `mycel semantic index backfill`

Target advanced command that enqueues or runs backfill for a semantic index.

## Example

```sh
mycel semantic index backfill notes-search --space-id <space_id> --domain personal-pkm
```

## Notes

Backfill evaluates inference policy and resolves a compatible credential grant before each model endpoint call.
