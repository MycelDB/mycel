# WAL Phase 6 Status

## Status

Phase 6 implementation is functionally complete for daemon-owned metadata and graph commit paths covered by the implementation plan. Remaining direct mutations are documented exceptions or lower-level storage operations reached through WAL-backed module APIs.

## WAL-backed mutation slices completed

- Space metadata:
  - create/delete space
  - grant space user
  - create/update/delete domain
- Space templates:
  - create/update/archive/delete/import templates
- Identity user metadata:
  - create user
  - enable/disable/delete user
  - set user password
- Admin/operator metadata:
  - create/update/enable/disable/delete operator
  - set operator password
  - grant/revoke roles
  - grant/revoke capabilities
- Blob metadata:
  - upload metadata put/update
  - delete metadata
- Graph commits:
  - logical graph transaction commit
- Semantic/inference metadata:
  - global package/endpoint/model/capability/vector-store/secret/credential metadata
  - space semantic index/credential grant/inference policy metadata
- Semantic maintenance/accounting:
  - usage accounting append
  - dirty event append
  - maintenance checkpoint save
  - dirty work upsert/complete/fail
- Embedding provider store:
  - provider key add/update/delete via WAL manager wrapper
- Backup/daemon metadata:
  - backup policy update
  - backup delete

## Known exceptions / follow-up candidates

- Auth refresh-session lifecycle still uses the existing session store directly from user/admin modules. This includes session create/refresh/revoke and audit events. These records are security-sensitive token lifecycle state and should receive a dedicated WAL record family rather than being folded into user/admin metadata records.
- Semantic vector record file backend mutations are not daemon-WAL-backed yet. These are generated index/vector artifacts and should be coordinated with semantic index rebuild/backfill semantics.
- Low-level graph file-session mutations remain local staging operations; durable graph commit is WAL-backed at `graph.commit.v1`.
- Test fixtures and package-local storage tests still call stores directly by design.

## Validation

The full internal test suite passed after the implemented Phase 6 slices:

```sh
go test ./internal/...
```
