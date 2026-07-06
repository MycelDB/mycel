package identity

import "github.com/google/uuid"

// UserID uniquely identifies a user internally.
//
// UserID is an immutable UUID used as the stable system key.
type UserID = uuid.UUID

// UserRef uniquely identifies a user externally.
//
// UserRef is an immutable external username/login identifier.
type UserRef string

// UserStatus defines lifecycle state for a user.
type UserStatus string

const (
	UserStatusPending  UserStatus = "pending"
	UserStatusActive   UserStatus = "active"
	UserStatusDisabled UserStatus = "disabled"
)

// User is the core identity model.
//
// UserID is the immutable internal key.
// Username is the immutable external unique key.
type User struct {
	ID       UserID
	Username UserRef
	Status   UserStatus
}

// UserInput is the create/upsert payload for user records.
//
// ID is optional so callers can provide one or let the implementation assign it.
// Username is required and must be unique.
type UserInput struct {
	ID       *UserID
	Username UserRef
	Status   UserStatus
}
