package client

import "knot_db/model"

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	AccessToken AccessToken
	SpaceID     model.SpaceID
}
