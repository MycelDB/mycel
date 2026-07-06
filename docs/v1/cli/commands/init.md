# `mycel init`

Initializes a Mycel data directory and bootstraps the initial superuser.

## Example

```sh
mycel -d ./data -u admin -p change-me init
```

## Notes

- Run once before normal commands.
- The supplied `-u/-p` credentials become the initial administrative user.
- Future advanced initialization should create the built-in `mycel-file` vector store instance.
