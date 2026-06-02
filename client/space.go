package client

// CreateSpaceInput defines space creation request payload.
type CreateSpaceInput struct {
	AccessToken AccessToken
	Name        string
}

// SpaceInfo contains resulting space identifiers.
type SpaceInfo struct {
	OwnerID string `json:"owner_id"`
	SpaceID string `json:"space_id"`
	Name    string `json:"name"`
}
