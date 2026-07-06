package space

import (
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/identity"
)

// SpaceID uniquely identifies a space.
//
// SpaceID is an immutable UUID used as the stable space key.
type SpaceID = uuid.UUID

// Space is an owner-scoped logical container for graph data.
type Space struct {
	SpaceID   SpaceID
	OwnerID   identity.UserID
	Name      string
	Status    string
	Settings  SpaceSettings
	CreatedAt time.Time
	UpdatedAt time.Time
}
