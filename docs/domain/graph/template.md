# Node Templates

Node templates are per-space, immutable, semver-versioned contracts for node properties and direct child-node rules.

Nodes reference templates by `TemplateID`. Import JSON identifies template versions by `key` + `version`; the system assigns `TemplateID` during import.

## Import document

```json
{
  "schema_version": 1,
  "templates": [
    {
      "key": "note",
      "version": "1.0.0",
      "display_name": "Note",
      "description": "Basic note node",
      "properties": {
        "allow_extra": false,
        "allowed": [
          { "name": "title", "type": "string", "required": true },
          { "name": "tags", "type": "array", "default": [] }
        ],
        "forbidden": ["secret"]
      },
      "children": {
        "allowed": true,
        "allowed_templates": [
          { "key": "task", "version": "1.0.0" }
        ]
      }
    }
  ]
}
```

## Versioning

- Versions must be semver, e.g. `1.0.0`.
- Template versions are immutable.
- Import rejects an existing `space_id + key + version`.
- Child template references require an exact `key` and `version`.

## Property policy

- `allowed` lists properties recognized by the template.
- `forbidden` lists properties that must never appear.
- A property cannot appear in both `allowed` and `forbidden`; imports with overlaps are rejected.
- `allow_extra: false` rejects any property not listed in `allowed`.
- Missing props are treated as `{}` and defaults are applied before required-property checks.

Supported property types:

- `string`
- `number`
- `bool`
- `object`
- `array`
- `date`

## Child policy

Child policies apply to direct children only.

- `allowed: false` means nodes with this template cannot have direct children.
- `allowed: true` with no `allowed_templates` allows any direct child.
- `allowed: true` with `allowed_templates` requires direct children to use one of the listed exact template versions.
