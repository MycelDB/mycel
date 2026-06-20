# Custom metadata

Mycel stores application-level block metadata in node `Props` using two canonical keys:

```json
{
  "tags": ["project", "urgent"],
  "properties": {
    "priority": "high",
    "rating": 5,
    "flagged": true
  }
}
```

## Tags

Tags are node-level labels. Canonical tag identities are:

- trimmed
- lower-cased
- whitespace-collapsed
- stripped of one leading `#` when present
- deduplicated per node

## Custom properties

Custom name-value properties live under `props.properties`. Canonical property names are:

- trimmed
- lower-cased
- whitespace-collapsed

Supported indexed values are scalar JSON-compatible values:

- string
- number
- boolean

## Index compatibility

The metadata index is rebuilt from committed graph nodes and does not require a destructive migration. Existing JSON-shaped data such as `[]any` tag arrays and `map[string]any` property maps is normalized while indexing. Malformed legacy entries are ignored at the individual tag/property level so one bad value does not prevent other valid metadata on the same node from being queryable.

Metadata query APIs are committed-state only: transaction changes become visible after commit and remain invisible after rollback.
