package model

type Space struct {
	SpaceID  string
	OwnerID  string
	Name     string
	Status   string
	Settings SpaceSettings
}
