package internal

import (
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// CreateSpaceInput defines space creation request payload.
type CreateSpaceInput struct {
	AccessToken AccessToken
	Name        string
}

// ListSpacesInput defines a space list request payload.
type ListSpacesInput struct {
	AccessToken AccessToken
}

// DeleteSpaceInput defines a hard-delete space request payload.
type DeleteSpaceInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}

// SpaceInfo is returned after creating or resolving a space.
type SpaceInfo struct {
	OwnerID identity.UserID     `json:"owner_id"`
	SpaceID domainspace.SpaceID `json:"space_id"`
	Name    string              `json:"name"`
}
