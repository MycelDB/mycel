# `mycel repl`

Starts the interactive Mycel CLI session.

## Example

```sh
mycel -d ./data repl
```

Inside the REPL:

```text
mycel> login admin change-me
mycel> space add demo
mycel> logout
mycel> exit
```

## Notes

- `login` and `logout` are REPL-only commands.
- `space set` and `space unset` update the current in-memory CLI session.
