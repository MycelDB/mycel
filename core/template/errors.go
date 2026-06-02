package template

import "errors"

var (
	ErrInvalidInput             = errors.New("invalid input")
	ErrTemplateNotFound         = errors.New("template not found")
	ErrDuplicateTemplateVersion = errors.New("duplicate template version")
)
