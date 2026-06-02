package client

import "knot_db/model"

// CreateSpaceInput defines space creation request payload.
type CreateSpaceInput struct {
	AccessToken AccessToken
	Name        string
}

// SpaceInfo contains resulting space identifiers.
type SpaceInfo struct {
	OwnerID model.UserID  `json:"owner_id"`
	SpaceID model.SpaceID `json:"space_id"`
	Name    string        `json:"name"`
}
