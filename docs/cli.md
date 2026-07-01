# MycelDB CLI

`mycel` is a Cobra-based command-line client for the embedded MycelDB engine.

It supports:

- one-shot CLI commands
- interactive REPL mode

Build from the module root:

```sh
cd mycel
make build
```

Or run without installing:

```sh
go run ./cmd/mycel --help
```

Set the shared MycelDB data directory, or pass `-d/--data-dir` on each command:

```sh
export MYCELDB_DATA_DIR=~/mycel_data
```

## Configuration

MycelDB CLI configuration precedence is: built-in defaults, optional YAML file, environment variables, then command-line flags.

Use `--config` or `MYCELDB_CONFIG` to load a YAML file:

```yaml
data_dir: ~/mycel_data
output: text
security:
  user_store_encryption_key_b64: ""
auth:
  access_token_ttl: 1h
  refresh_idle_ttl: 720h
  refresh_absolute_ttl: 2160h
  refresh_audit_retention_ttl: 720h
  refresh_token_bytes: 32
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

Blob upload limits use `-1` for unlimited. Exact MIME overrides can use `0` to disallow that MIME type.

Auth TTL environment aliases include:

- `MYCELDB_AUTH_ACCESS_TOKEN_TTL`
- `MYCELDB_AUTH_REFRESH_IDLE_TTL`
- `MYCELDB_AUTH_REFRESH_ABSOLUTE_TTL`
- `MYCELDB_AUTH_REFRESH_AUDIT_RETENTION_TTL`
- `MYCELDB_AUTH_REFRESH_TOKEN_BYTES`

Other common environment aliases include `MYCELDB_DATA_DIR`, `MYCELDB_USER_STORE_ENCRYPTION_KEY_B64`, and `MYCELDB_STORAGE_BLOBS_MAX_*_BYTES`.

Initialize a data directory once before running normal commands:

```sh
mycel -d ./data -u admin -p change-me init
```

`init` bootstraps the store using the supplied `-u/-p` credentials as the initial superuser.

All other non-REPL commands require an initialized data directory and credentials:

```sh
mycel -d ./data -u admin -p change-me space add demo
```

## Auth sessions

Durable refresh-session commands operate on Mycel-owned refresh sessions created by `engine.Engine.LoginSession`. They never print refresh tokens or refresh-token hashes.

List sessions for the authenticated user:

```sh
mycel -d ./data -u admin -p change-me auth session list
```

Revoke one session owned by the authenticated user:

```sh
mycel -d ./data -u admin -p change-me auth session revoke <session_id> --reason "lost device"
```

Revoke all other sessions while keeping the specified current session:

```sh
mycel -d ./data -u admin -p change-me auth session revoke-other --current-session-id <session_id>
```

Run cleanup/redaction for expired or old revoked refresh sessions. The authenticated user must have system operation permission:

```sh
mycel -d ./data -u admin -p change-me auth session cleanup
```

Use `--output json` on these commands for machine-readable output.

## REPL

```sh
mycel -d ./data repl
```

Inside the REPL, use `login` and `logout`:

```text
mycel> login admin change-me
mycel> space add demo
mycel> logout
mycel> exit
```

Set a default working space for space-specific commands:

```text
mycel> space set <space_id>
mycel> node add --content "hello"
mycel> template list
mycel> space unset
```

`login` and `logout` are REPL-only commands. `space set` and `space unset` are useful in the REPL because they update the current in-memory CLI session.

## Commands

### Users

```sh
mycel -d ./data -u admin -p change-me user add --ref bob --new-password secret
mycel -d ./data -u admin -p change-me user list
mycel -d ./data -u admin -p change-me user delete <user_id>
```

Deleting a user is a hard delete and also deletes spaces owned by that user and all associated constructs.

### ACLs

System ACL:

```sh
mycel -d ./data -u admin -p change-me acl grant system --user-id <user_id> --role user_admin
mycel -d ./data -u admin -p change-me acl revoke system --user-id <user_id>
mycel -d ./data -u admin -p change-me acl list system
```

Space ACL:

```sh
mycel -d ./data -u admin -p change-me acl grant space --space-id <space_id> --user-id <user_id> --permission read
mycel -d ./data -u admin -p change-me acl revoke space --space-id <space_id> --user-id <user_id>
mycel -d ./data -u admin -p change-me acl list space --space-id <space_id>
```

Roles: `superuser`, `user_admin`, `operator`.

Permissions: `read`, `write`, `admin`.

### Spaces

```sh
mycel -d ./data -u admin -p change-me space add demo
mycel -d ./data -u admin -p change-me space add "Personal PKM" --owner-ref bob
mycel -d ./data -u admin -p change-me space add "Personal PKM" --owner-user-id <user_id>
mycel -d ./data -u admin -p change-me space list
mycel -d ./data -u admin -p change-me space delete <space_id>
```

Deleting a space is a hard delete and removes metadata, ACLs, templates, and graph files associated with the space.

### Templates

Import templates from a JSON file:

```sh
mycel -d ./data -u admin -p change-me template import --space-id <space_id> --file templates.json
```

Import templates from stdin:

```sh
cat templates.json | mycel -d ./data -u admin -p change-me template import --space-id <space_id> --file -
```

List templates for a space:

```sh
mycel -d ./data -u admin -p change-me template list --space-id <space_id>
```

### Nodes

```sh
mycel -d ./data -u admin -p change-me node add --space-id <space_id> --content "hello" --props-json '{"tag":"demo"}'
mycel -d ./data -u admin -p change-me node list --space-id <space_id> --contains hello --limit 20
mycel -d ./data -u admin -p change-me node get --space-id <space_id> <node_id>
mycel -d ./data -u admin -p change-me node delete --space-id <space_id> <node_id>
```

Deleting a node removes the node and incident edges. If the node has descendants, pass `--recursive`.

### Embeddings

Create a profile, generate one node embedding, or backfill a selected set of nodes:

```sh
mycel -d ./data -u admin -p change-me embeddings profiles add \
  --name pages \
  --provider openai \
  --model openai/text-embedding-3-small \
  --source subtree

mycel -d ./data -u admin -p change-me embeddings generate \
  --space-id <space_id> \
  --node <node_id> \
  --profile <profile_id>

mycel -d ./data -u admin -p change-me embeddings generate \
  --space-id <space_id> \
  --profile <profile_id> \
  --template-key logseq.page \
  --limit 500
```

Batch generation requires `--node`, `--template-key`, or `--contains`. It skips current embeddings by source hash unless `--force` is set.

## JSON output

Add `--output json` to any command to emit JSON.
