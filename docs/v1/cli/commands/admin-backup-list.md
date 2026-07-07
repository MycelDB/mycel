# mycel admin backup list

List completed daemon backups.

```sh
mycel --daemon-addr 127.0.0.1:9091 -u admin -p '<operator-password>' \
  admin backup list --page-size 50
```

Options:

| Flag | Description |
|---|---|
| `--page-size` | Maximum number of backups to return. |
| `--page-token` | Pagination token returned by a previous JSON response. |

Text output prints backup id, completion time, and archive size. JSON output returns the full `ListBackupsResponse`, including `next_page_token`:

```sh
mycel ... --output json admin backup list --page-size 50
```

Incomplete temporary `.tmp` files are ignored by list and retention logic.
