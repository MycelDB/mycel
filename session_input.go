package knotdb

import "martinbeauvais.com/mbgit/knotbase/knotdb/model"

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	AccessToken AccessToken
	SpaceID     model.SpaceID
}
