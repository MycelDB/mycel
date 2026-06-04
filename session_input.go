package knotdb

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	AccessToken AccessToken
	SpaceID     identity.SpaceID
}
