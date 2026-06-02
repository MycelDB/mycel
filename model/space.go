package model

// Space is an owner-scoped logical container for graph data.
type Space struct {
	SpaceID  SpaceID
	OwnerID  UserID
	Name     string
	Status   string
	Settings SpaceSettings
}
