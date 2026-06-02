package knotdb

import (
	coretemplate "martinbeauvais.com/mbgit/knotbase/knotdb/core/template"
	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// ImportTemplatesInput defines template import request payload.
type ImportTemplatesInput struct {
	AccessToken AccessToken
	SpaceID     model.SpaceID
	Document    coretemplate.ImportDocument
}
