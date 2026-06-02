package model

import "github.com/google/uuid"

// UserID uniquely identifies a user internally.
//
// UserID is an immutable UUID used as the stable system key.
type UserID = uuid.UUID

// SpaceID uniquely identifies a space.
//
// SpaceID is an immutable UUID used as the stable space key.
type SpaceID = uuid.UUID

// UserRef uniquely identifies a user externally.
//
// UserRef is an immutable external identifier such as an email, username, or
// identity-provider subject.
type UserRef string

// UserStatus defines lifecycle state for a user.
type UserStatus string

const (
	UserStatusPending UserStatus = "pending"
	UserStatusActive  UserStatus = "active"
	UserStatusPaused  UserStatus = "paused"
	UserStatusRevoked UserStatus = "revoked"
)

// User is the core identity model.
//
// UserID is the immutable internal key.
// UserRef is the immutable external unique key.
type User struct {
	ID       UserID
	Ref      UserRef
	Email    *string
	Username *string
	Status   UserStatus
}

// UserInput is the create/upsert payload for user records.
//
// ID is optional so callers can provide one or let the implementation assign it.
// Ref is required and must be unique.
type UserInput struct {
	ID       *UserID
	Ref      UserRef
	Email    *string
	Username *string
	Status   UserStatus
}
