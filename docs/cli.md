# KnotDB CLI

`knotdb` is a Cobra-based command-line client for the embedded KnotDB engine.

It supports:

- one-shot CLI commands
- interactive REPL mode

Build from the module root:

```sh
cd knot_db/knot_db
make build
```

Or run without installing:

```sh
go run ./cmd/knotdb --help
```

Set the shared KnotDB data directory, or pass `-d/--data-dir` on each command:

```sh
export KNOTDB_DATA_DIR=~/knot_data
```

## Configuration

KnotDB CLI configuration precedence is: built-in defaults, optional YAML file, environment variables, then command-line flags.

Use `--config` or `KNOTDB_CONFIG` to load a YAML file:

```yaml
data_dir: ~/knot_data
output: text
security:
  user_store_encryption_key_b64: ""
auth:
  access_token_ttl: 1h
storage:
  blobs:
    stale_tmp_age: 1h
    max_size_bytes: -1
    max_image_bytes: -1
    max_pdf_bytes: -1
    max_audio_bytes: -1
    max_video_bytes: -1
    max_other_bytes: -1
    mime_type_limits:
      application/zip: 0
```

Blob upload limits use `-1` for unlimited. Exact MIME overrides can use `0` to disallow that MIME type. The existing `KNOTDB_DATA_DIR` environment variable remains supported, and additional environment aliases include `KNOTDB_AUTH_ACCESS_TOKEN_TTL`, `KNOTDB_USER_STORE_ENCRYPTION_KEY_B64`, and `KNOTDB_STORAGE_BLOBS_MAX_*_BYTES`.

Initialize a data directory once before running normal commands:

```sh
knotdb -d ./data -u admin -p change-me init
```

`init` bootstraps the store using the supplied `-u/-p` credentials as the initial superuser.

All other non-REPL commands require an initialized data directory and credentials:

```sh
knotdb -d ./data -u admin -p change-me space add demo
```

## REPL

```sh
knotdb -d ./data repl
```

Inside the REPL, use `login` and `logout`:

```text
knotdb> login admin change-me
knotdb> space add demo
knotdb> logout
knotdb> exit
```

Set a default working space for space-specific commands:

```text
knotdb> space set <space_id>
knotdb> node add --content "hello"
knotdb> template list
knotdb> space unset
```

`login` and `logout` are REPL-only commands. `space set` and `space unset` are useful in the REPL because they update the current in-memory CLI session.

## Commands

### Users

```sh
knotdb -d ./data -u admin -p change-me user add --ref bob --new-password secret
knotdb -d ./data -u admin -p change-me user list
knotdb -d ./data -u admin -p change-me user delete <user_id>
```

Deleting a user is a hard delete and also deletes spaces owned by that user and all associated constructs.

### ACLs

System ACL:

```sh
knotdb -d ./data -u admin -p change-me acl grant system --user-id <user_id> --role user_admin
knotdb -d ./data -u admin -p change-me acl revoke system --user-id <user_id>
knotdb -d ./data -u admin -p change-me acl list system
```

Space ACL:

```sh
knotdb -d ./data -u admin -p change-me acl grant space --space-id <space_id> --user-id <user_id> --permission read
knotdb -d ./data -u admin -p change-me acl revoke space --space-id <space_id> --user-id <user_id>
knotdb -d ./data -u admin -p change-me acl list space --space-id <space_id>
```

Roles: `superuser`, `user_admin`, `operator`.

Permissions: `read`, `write`, `admin`.

### Spaces

```sh
knotdb -d ./data -u admin -p change-me space add demo
knotdb -d ./data -u admin -p change-me space list
knotdb -d ./data -u admin -p change-me space delete <space_id>
```

Deleting a space is a hard delete and removes metadata, ACLs, templates, and graph files associated with the space.

### Templates

Import templates from a JSON file:

```sh
knotdb -d ./data -u admin -p change-me template import --space-id <space_id> --file templates.json
```

Import templates from stdin:

```sh
cat templates.json | knotdb -d ./data -u admin -p change-me template import --space-id <space_id> --file -
```

List templates for a space:

```sh
knotdb -d ./data -u admin -p change-me template list --space-id <space_id>
```

### Nodes

```sh
knotdb -d ./data -u admin -p change-me node add --space-id <space_id> --content "hello" --props-json '{"tag":"demo"}'
knotdb -d ./data -u admin -p change-me node list --space-id <space_id> --contains hello --limit 20
knotdb -d ./data -u admin -p change-me node get --space-id <space_id> <node_id>
knotdb -d ./data -u admin -p change-me node delete --space-id <space_id> <node_id>
```

Deleting a node removes the node and incident edges. If the node has descendants, pass `--recursive`.

## JSON output

Add `--output json` to any command to emit JSON.
