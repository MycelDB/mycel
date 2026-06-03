package knotdb

import "martinbeauvais.com/mbgit/knotbase/knotdb/model"

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
	SpaceID     model.SpaceID
}

// SpaceInfo contains resulting space identifiers.
type SpaceInfo struct {
	OwnerID model.UserID  `json:"owner_id"`
	SpaceID model.SpaceID `json:"space_id"`
	Name    string        `json:"name"`
}
