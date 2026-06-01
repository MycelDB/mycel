package client

// CreateDatabaseInput defines database creation request payload.
type CreateDatabaseInput struct {
	Auth AuthToken
	Name string
}

// DatabaseInfo contains resulting database identifiers.
type DatabaseInfo struct {
	OwnerID string `json:"owner_id"`
	SpaceID string `json:"space_id"`
	Name    string `json:"name"`
}
