# KnotDB

Go library for KnotDB graph spaces.

Module path: `martinbeauvais.com/mbgit/knotbase/knotdb`.

## Layout
- `core/`: injectable managers for users, spaces, templates, and access control
- `domain/identity`: users, spaces, tenancy IDs, ownership, and settings
- `domain/access`: system roles, space permissions, and ACL rule types
- `domain/graph`: nodes, edges, templates, graph inputs, and session API
- `internal/`: private implementation packages
- `docs/`: API/reference documentation

## API docs
- Edge structures: `docs/domain/graph/edge.md`
- Node operations: `docs/domain/graph/node.md`
- Node templates: `docs/domain/graph/template.md`
- User structures: `docs/domain/identity/user.md`
- Access control: `docs/domain/access/access.md`
- Storage layout: `docs/storage/layout.md`

## Testing
- Run once: `make test`
- Verbose + coverage: `make test-verbose`
- Watch mode (verbose + coverage): `make test-watch` (requires `watchexec`)
