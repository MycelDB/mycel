package client

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	Auth    AuthToken
	SpaceID string
}
