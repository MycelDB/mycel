package identity

import (
	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

// SpaceID uniquely identifies a space.
//
// SpaceID is an immutable UUID used as the stable space key.
type SpaceID = uuid.UUID

// Space is an owner-scoped logical container for graph data.
type Space struct {
	SpaceID  SpaceID
	OwnerID  UserID
	Name     string
	Status   string
	Settings space.SpaceSettings
}
