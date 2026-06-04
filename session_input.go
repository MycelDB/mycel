package knotdb

import domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"

// OpenSessionInput defines session-open request payload.
type OpenSessionInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}
