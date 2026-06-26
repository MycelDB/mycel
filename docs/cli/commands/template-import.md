# `mycel template import`

Imports graph template definitions for a space.

## Examples

```sh
mycel -d ./data -u admin -p change-me template import --space-id <space_id> --file templates.json
cat templates.json | mycel -d ./data -u admin -p change-me template import --space-id <space_id> --file -
```
