# Spaces

Space domain types are exposed by `github.com/myceldb/mycel/domain/space`.

`SpaceID` is the immutable UUID identifier for a space.

`Space` is the owner-scoped logical container for graph data. It stores the space ID, owner user ID, name, status, and space settings.

`SpaceSettings` defines tunable limits and behavior at space scope, such as maximum space size, chunk targets, asset/PDF limits, and compaction behavior.
