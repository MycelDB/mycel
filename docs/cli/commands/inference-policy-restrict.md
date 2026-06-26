# `mycel inference policy restrict`

Target advanced command that creates an inference policy narrowing allowed processing.

## Example

```sh
mycel inference policy restrict --space-id <space_id> --domain personal-pkm --node <private-node-id> --include-descendants --local-only
```

## Notes

Multiple restrict policies combine by intersection / most restrictive result.
