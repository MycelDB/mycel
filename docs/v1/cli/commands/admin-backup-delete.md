# mycel admin backup delete

Delete a completed backup archive and its sidecar manifest by backup id.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup delete '<backup-id>'
```

Aliases:

```text
admin backup del
admin backup rm
```

The daemon validates delete paths are contained in the configured backup directory. Delete is a mutating Admin operation and is not quiesce-exempt; retry if it returns a transient unavailable error during an active backup.

Use `admin backup list` to discover backup ids.
