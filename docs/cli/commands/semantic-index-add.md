# `mycel semantic index add`

Command that creates a semantic index definition.

## Example

```sh
mycel semantic index add notes-search --space-id <space_id> --domain personal-pkm --purpose semantic_search --template-key logseq.journal --template-key logseq.page --source subtree --model-endpoint openai-public --model openai/text-embedding-3-small --vector-store mycel-file
```

## Notes

The semantic index binding does not contain credentials. Endpoint calls require credential grants and inference policy approval.

Requires admin access to the target space because semantic indexes are space-owned semantic metadata.
