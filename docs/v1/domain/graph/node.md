# Node Operations (v1 historical)

This page described the former public embedded Go `domain/graph` and `session`
packages. Those implementation packages are now internal.

Applications should use the daemon Client Graph API (`mycel-api` protobufs or
`mycel-go-sdk`) for node operations. Daemon-internal graph records live under
`internal/graph/model` and are not application contracts.

See [API graph design](../../../v2/design/api/graph.md).
