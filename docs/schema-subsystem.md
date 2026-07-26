# Schema subsystem

Mycel schemas are domain-scoped graph contracts. They replace the removed Mycel template subsystem and describe the labels, properties, payload fields, metadata fields, edge labels, endpoint constraints, and hierarchy rules that apply to a domain.

## Scope

A schema is attached to a domain, not globally to a daemon. A space may contain multiple domains with different schemas. Graph mutations and query compilation use the schema for the transaction domain when one is present.

The canonical API representation is JSON. Human-authored GWL files can be compiled to canonical JSON with the CLI.

## Modes

Schemas support three validation modes:

| Mode | Behavior |
| --- | --- |
| `permissive` | Unknown labels/properties are accepted. Schema may still be used for hints/planning. |
| `warn` | Unknown labels/properties are accepted. Warning diagnostics are intended for callers that expose them. |
| `strict` | Unknown labels/properties, invalid field types, invalid payload fields, and invalid relationship endpoints fail validation. |

## Core model

A domain schema contains:

- `DomainID`
- `Name`
- `Version`
- `Mode`
- `NodeTypes`
- `EdgeTypes`

Node and edge types define labels plus field specs for properties/payload/meta. Edge types may also define endpoint constraints and hierarchy policy.

Hierarchy policy replaces the old hardcoded template child-policy behavior. A schema edge type can mark a label such as `contains` as a hierarchy edge and enforce acyclic, single-parent, and same-domain constraints.

## CLI

```sh
mycel schema get --domain <domain-id>
mycel schema put --domain <domain-id> schema.json
mycel schema put --domain <domain-id> schema.gwl
mycel schema validate schema.json
mycel schema validate schema.gwl
mycel schema compile schema.gwl
```

Input format defaults to auto-detection by extension. JSON is canonical; GWL is a source format compiled to JSON.

## API and SDKs

Client and admin schema services support domain schema get/put operations. SDKs expose the generated schema clients directly.

## Knot PKM

Knot PKM provisions schemas for registration, content, and settings domains. Knot PKM records are classified with schema-era properties such as `record_type` and `settings_key`, plus edge labels such as `contains` and `references`.
