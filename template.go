package knotdb

import (
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

// ImportTemplatesInput defines template import request payload.
type ImportTemplatesInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
	Document    coretemplate.ImportDocument
}

// ListTemplatesInput defines a request to list templates for a space.
type ListTemplatesInput struct {
	AccessToken AccessToken
	SpaceID     domainspace.SpaceID
}
