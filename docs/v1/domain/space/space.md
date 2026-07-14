# Spaces (v1 historical)

This page described the former public embedded Go `domain/space` package. The
implementation package is now internal.

Applications should use daemon Admin/Client Space and Domain APIs (`mycel-api`
protobufs or `mycel-go-sdk`) instead of importing space structs from the `mycel`
module. Daemon-internal space records live under `internal/space/model` and are
not application contracts.

See [API space design](../../../v2/design/api/space.md) and [API domain design](../../../v2/design/api/domain.md).
