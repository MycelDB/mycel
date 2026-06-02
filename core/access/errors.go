package access

import "errors"

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrRuleNotFound = errors.New("access rule not found")
	ErrLastAdmin    = errors.New("space must retain at least one admin")
)
