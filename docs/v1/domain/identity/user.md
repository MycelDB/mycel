# User Identity (v1 historical)

This page described the former public embedded Go `domain/identity` package. The
implementation package is now internal.

Applications should use daemon Admin/Client user/auth APIs (`mycel-api`
protobufs or `mycel-go-sdk`) instead of importing identity structs from the
`mycel` module. Daemon-internal identity records live under
`internal/identity/model` and are not application contracts.

See [Admin user design](../../../v2/design/admin/user.md) and [Client auth design](../../../v2/design/grpc-client-auth.md).
