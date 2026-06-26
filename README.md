# MycelDB

Go library and CLI for MycelDB graph spaces.

Module path: `github.com/myceldb/mycel`.

## Layout

See [`docs/architecture.md`](docs/architecture.md) for a navigation guide, import conventions, and layer diagram.

**Public API (safe for embedders):**
- `engine/`: engine API, constructor, lifecycle, auth, and system/space management
- `session/`: scoped graph-space operations (nodes, edges, templates, queries)
- `domain/identity`: users, user IDs, and ownership
- `domain/space`: spaces, space IDs, and space-level configuration types
- `domain/access`: system roles, space permissions, and ACL rule types
- `domain/graph`: pure graph domain types for nodes, edges, templates, and template policies
- `domain/embedding`: pure embedding metadata, record, and semantic-search result types
- `query/`: programmatic GQL-style query builder
- `store/`: injectable persistence interfaces (for tests or custom backends)

**Internal (do not import from applications):**
- `engine/internal/`: default engine implementation
- `internal/session/filesession/`: file-backed session implementation
- `internal/graphstorage/`, `internal/filestore/`: low-level persistence
- `internal/cli/`: CLI command implementation
- `cmd/mycel/`: CLI entrypoint binary

**Documentation:**
- `docs/`: API/reference documentation

## API docs
- Architecture and navigation: `docs/architecture.md`
- Edge structures: `docs/domain/graph/edge.md`
- Session API: `docs/session/session.md`
- Programmatic queries: `docs/query/gql-mapping.md`
- Node operations: `docs/domain/graph/node.md`
- Node templates: `docs/domain/graph/template.md`
- User structures: `docs/domain/identity/user.md`
- Space structures: `docs/domain/space/space.md`
- Access control: `docs/domain/access/access.md`
- Storage layout: `docs/storage/layout.md`
- CLI usage: `docs/cli/README.md`
- Semantic indexing and embeddings: `docs/semantic/README.md`
- Embeddings MVP: `docs/semantic/current-mvp.md`
- Custom metadata indexing: `docs/metadata.md`

## CLI

Build and run the operator CLI from the module root:

```sh
make build
bin/mycel --help
```

See `docs/cli/README.md` for command reference.

## Data directory

MycelDB tools and services use `MYCELDB_DATA_DIR` as the shared default data directory when no explicit data directory is supplied.

```sh
export MYCELDB_DATA_DIR=~/mycel_data
```

Explicit `engine.EngineConfig.DataDir` values still take precedence.

## Testing
- Run once: `make test`
- Verbose + coverage: `make test-verbose`
- Watch mode (verbose + coverage): `make test-watch` (requires `watchexec`)
