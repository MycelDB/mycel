# knotdb

Go library for Knotbase graph spaces.

Module path: `martinbeauvais.com/mbgit/knotbase/knotdb`.

## Layout
- `core/`: injectable managers for users, spaces, templates, and access control
- `graph/`: public graph types and interfaces
- `model/`: shared domain/storage model types
- `internal/`: private implementation packages
- `docs/`: API/reference documentation

## API docs
- Edge structures: `docs/graph/edge.md`
- Node templates: `docs/graph/template.md`
- User structures: `docs/model/user.md`
- Access control: `docs/model/access.md`
- Storage layout: `docs/storage/layout.md`

## Testing
- Run once: `make test`
- Verbose + coverage: `make test-verbose`
- Watch mode (verbose + coverage): `make test-watch` (requires `watchexec`)
