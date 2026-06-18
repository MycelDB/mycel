package internal

import domainspace "github.com/myceldb/mycel/domain/space"

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}
