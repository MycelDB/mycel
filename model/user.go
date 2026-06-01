package model

// User stores identity metadata used to map external users to storage owners.
//
// UserRef must be unique and immutable.
type User struct {
	UserID   string
	UserRef  string
	Email    *string
	Username *string
	Status   string
}
