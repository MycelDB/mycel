package model

// Space is an owner-scoped logical container for graph data.
type Space struct {
	SpaceID  string
	OwnerID  string
	Name     string
	Status   string
	Settings SpaceSettings
}
