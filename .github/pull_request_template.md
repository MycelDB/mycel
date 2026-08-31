## Summary

<!-- What changed and why? -->

## Scope and compatibility

- [ ] This change is backward compatible for daemon behavior, CLI usage, configuration, and storage formats.
- [ ] This change may be incompatible and has maintainer approval.
- [ ] README/docs/examples are updated if public usage, operations, or APIs changed.
- [ ] Migration, downgrade, rollback, or operator notes are included when needed.

## API and generated code

- [ ] Source protobuf/API contract changes are not added here; they belong in `mycel-api` first.
- [ ] Daemon protobuf stubs were regenerated with `make generate-proto` if the API contract changed.
- [ ] Generated protobuf or ANTLR output was not hand-edited.
- [ ] Public SDK/API generated code was not added unless explicitly approved.

## Safety-sensitive areas

- [ ] Raft/clustering ownership, consistency, recovery, and fail-closed behavior are unaffected or documented.
- [ ] Backup/restore, import/export, and divergence workflows remain explicit and safe by default.
- [ ] Authentication, authorization, TLS/mTLS, tokens, and secret handling are unaffected or documented.
- [ ] No hidden destructive behavior, automatic repair, overwrite, rebalance, merge, or force action was added.

## Repository boundaries

- [ ] Daemon API adapters remain under `internal/daemon/api`.
- [ ] Service implementations remain under their subsystem packages.
- [ ] Applications are not encouraged to import internal implementation packages from this module.
- [ ] Secrets, tokens, passwords, TLS key material, private data, and confidential infrastructure details are not logged or exposed.

## Validation

- [ ] `make test` passes, or narrower validation is justified below.
- [ ] `make docs-check` passes for documentation changes.
- [ ] Raft/cluster/backup destructive gates were run only if explicitly appropriate.

## Notes

<!-- Commands run, migration notes, downstream impact, matching mycel-api/SDK versions, or follow-up work. -->
