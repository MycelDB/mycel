# `mycel node delete`

Deletes a node.

## Example

```sh
mycel -d ./data -u admin -p change-me node delete --space-id <space_id> <node_id>
```

## Notes

Deleting a node removes the node and incident edges. If the node has descendants, pass `--recursive`.
