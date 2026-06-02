package knotdb

import (
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// GrantSpaceAccessInput defines a request to grant or update user access for a space.
type GrantSpaceAccessInput struct {
	AccessToken AccessToken
	SpaceID     model.SpaceID
	UserID      model.UserID
	Permissions []model.SpacePermission
}

// RevokeSpaceAccessInput defines a request to revoke user access for a space.
type RevokeSpaceAccessInput struct {
	AccessToken AccessToken
	SpaceID     model.SpaceID
	UserID      model.UserID
}

// ListSpaceAccessInput defines a request to list access rules for a space.
type ListSpaceAccessInput struct {
	AccessToken AccessToken
	SpaceID     model.SpaceID
}
