package identity

import "github.com/google/uuid"

// UserID uniquely identifies a user internally.
//
// UserID is an immutable UUID used as the stable system key.
type UserID = uuid.UUID

// SpaceID uniquely identifies a space.
//
// SpaceID is an immutable UUID used as the stable space key.
type SpaceID = uuid.UUID
