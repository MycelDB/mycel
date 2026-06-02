package client

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	AccessToken AccessToken
	SpaceID     string
}
