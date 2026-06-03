package knotdb

import "martinbeauvais.com/mbgit/knotbase/knotdb/model"

// CreateUserInput defines a user creation request payload.
type CreateUserInput struct {
	AccessToken AccessToken
	User        model.UserInput
	Password    string
}

// ListUsersInput defines a user list request payload.
type ListUsersInput struct {
	AccessToken AccessToken
}

// DeleteUserInput defines a hard-delete user request payload.
// Deleting a user also deletes all spaces owned by that user and all constructs
// associated with those spaces.
type DeleteUserInput struct {
	AccessToken AccessToken
	UserID      model.UserID
}
