# knot_db

Go library for Knotbase graph spaces.

## Layout
- `core/`: foundational services (e.g. user management)
- `api/`: public graph API types and interfaces
- `model/`: shared domain/storage model types
- `internal/`: private implementation packages
- `docs/`: API/reference documentation

## API docs
- Edge structures: `docs/api/edge.md`
- User structures: `docs/core/user.md`

## Testing
- Run once: `make test`
- Verbose + coverage: `make test-verbose`
- Watch mode (verbose + coverage): `make test-watch` (requires `watchexec`)
