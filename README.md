# KnotDB

Go library and CLI for KnotDB graph spaces.

Module path: `martinbeauvais.com/mbgit/knotbase/knotdb`.

## Layout
- `cmd/knotdb/`: CLI entrypoint binary
- `internal/cli/`: CLI command implementation (not importable by library consumers)
- `engine/`: public engine API, constructor, lifecycle, auth, and system/space management
- `engine/internal/`: default engine implementation
- `core/`: injectable managers for users, spaces, templates, and access control
- `domain/identity`: users, user IDs, and ownership
- `domain/space`: spaces, space IDs, and space-level configuration types
- `domain/access`: system roles, space permissions, and ACL rule types
- `domain/graph`: pure graph domain types for nodes, edges, templates, and template policies
- `session`: scoped graph-space operation API for nodes, edges, templates, queries, and future transactions
- `query`: programmatic GQL-style query builder
- `internal/`: private implementation packages
- `docs/`: API/reference documentation

## API docs
- Edge structures: `docs/domain/graph/edge.md`
- Session API: `docs/session/session.md`
- Programmatic queries: `docs/query/gql-mapping.md`
- Node operations: `docs/domain/graph/node.md`
- Node templates: `docs/domain/graph/template.md`
- User structures: `docs/domain/identity/user.md`
- Space structures: `docs/domain/space/space.md`
- Access control: `docs/domain/access/access.md`
- Storage layout: `docs/storage/layout.md`
- CLI usage: `docs/cli.md`

## CLI

Build and run the operator CLI from the module root:

```sh
make build
bin/knotdb --help
```

See `docs/cli.md` for command reference.

## Data directory

KnotDB tools and services use `KNOTDB_DATA_DIR` as the shared default data directory when no explicit data directory is supplied.

```sh
export KNOTDB_DATA_DIR=~/knot_data
```

Explicit `engine.EngineConfig.DataDir` values still take precedence.

## Testing
- Run once: `make test`
- Verbose + coverage: `make test-verbose`
- Watch mode (verbose + coverage): `make test-watch` (requires `watchexec`)
