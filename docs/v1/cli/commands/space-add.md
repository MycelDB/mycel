# `mycel space add`

Creates a space.

## Examples

```sh
mycel -d ./data -u admin -p change-me space add demo
mycel -d ./data -u admin -p change-me space add "Personal PKM" --owner-ref bob
mycel -d ./data -u admin -p change-me space add "Personal PKM" --owner-user-id <user_id>
```

## Advanced semantic note

Applications should provision a baseline inference policy when creating/configuring spaces that use semantic features.
