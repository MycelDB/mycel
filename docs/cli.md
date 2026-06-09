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

Initialize a data directory once before running normal commands:

```sh
knotdb -d ./data -u admin -p change-me init
```

`init` bootstraps the store using the supplied `-u/-p` credentials as the initial superuser.

All other non-REPL commands require an initialized data directory and credentials:

```sh
knotdb -d ./data -u admin -p change-me add space demo
```

## REPL

```sh
knotdb -d ./data repl
```

Inside the REPL, use `login` and `logout`:

```text
knotdb> login admin change-me
knotdb> add space demo
knotdb> logout
knotdb> exit
```

Set a default working space for space-specific commands:

```text
knotdb> set_space <space_id>
knotdb> add node --content "hello"
knotdb> list templates
knotdb> unset_space
```

`login`, `logout`, `set_space`, and `unset_space` are REPL-only commands.

## Commands

### Users

```sh
knotdb -d ./data -u admin -p change-me add user --ref bob --new-password secret
knotdb -d ./data -u admin -p change-me list users
knotdb -d ./data -u admin -p change-me delete user <user_id>
```

Deleting a user is a hard delete and also deletes spaces owned by that user and all associated constructs.

### ACLs

System ACL:

```sh
knotdb -d ./data -u admin -p change-me add acl --user-id <user_id> --role user_admin
knotdb -d ./data -u admin -p change-me delete acl --user-id <user_id> --system
knotdb -d ./data -u admin -p change-me list acl --system
```

Space ACL:

```sh
knotdb -d ./data -u admin -p change-me add acl --space-id <space_id> --user-id <user_id> --permission read
knotdb -d ./data -u admin -p change-me delete acl --space-id <space_id> --user-id <user_id>
knotdb -d ./data -u admin -p change-me list acl --space-id <space_id>
```

Explicit ACL command aliases are also available:

```sh
knotdb -d ./data -u admin -p change-me acl grant system --user-id <user_id> --role user_admin
knotdb -d ./data -u admin -p change-me acl revoke system --user-id <user_id>
knotdb -d ./data -u admin -p change-me acl list system

knotdb -d ./data -u admin -p change-me acl grant space --space-id <space_id> --user-id <user_id> --permission read
knotdb -d ./data -u admin -p change-me acl revoke space --space-id <space_id> --user-id <user_id>
knotdb -d ./data -u admin -p change-me acl list space --space-id <space_id>
```

Roles: `superuser`, `user_admin`, `operator`.

Permissions: `read`, `write`, `admin`.

### Spaces

```sh
knotdb -d ./data -u admin -p change-me add space demo
knotdb -d ./data -u admin -p change-me list spaces
knotdb -d ./data -u admin -p change-me delete space <space_id>
```

Deleting a space is a hard delete and removes metadata, ACLs, templates, and graph files associated with the space.

### Templates

Import templates from a JSON file:

```sh
knotdb -d ./data -u admin -p change-me add template --space-id <space_id> --file templates.json
```

Import templates from stdin:

```sh
cat templates.json | knotdb -d ./data -u admin -p change-me add template --space-id <space_id> --file -
```

List templates for a space:

```sh
knotdb -d ./data -u admin -p change-me list templates --space-id <space_id>
```

### Nodes

```sh
knotdb -d ./data -u admin -p change-me add node --space-id <space_id> --content "hello" --props-json '{"tag":"demo"}'
knotdb -d ./data -u admin -p change-me delete node --space-id <space_id> <node_id>
```

Deleting a node removes the node and incident edges. If the node has descendants, pass `--recursive`.

## JSON output

Add `--output json` to any command to emit JSON.
