package knotdb

import (
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
)

// ImportTemplatesInput defines template import request payload.
type ImportTemplatesInput struct {
	AccessToken AccessToken
	SpaceID     identity.SpaceID
	Document    coretemplate.ImportDocument
}

// ListTemplatesInput defines a request to list templates for a space.
type ListTemplatesInput struct {
	AccessToken AccessToken
	SpaceID     identity.SpaceID
}
