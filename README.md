# knotdb

Go library for Knotbase graph spaces.

Module path: `martinbeauvais.com/mbgit/knotbase/knotdb`.

## Layout
- `core/`: injectable managers for users and spaces
- `graph/`: public graph types and interfaces
- `model/`: shared domain/storage model types
- `internal/`: private implementation packages
- `docs/`: API/reference documentation

## API docs
- Edge structures: `docs/graph/edge.md`
- User structures: `docs/model/user.md`

## Testing
- Run once: `make test`
- Verbose + coverage: `make test-verbose`
- Watch mode (verbose + coverage): `make test-watch` (requires `watchexec`)
