package principal

import "errors"

var (
	ErrInvalidCredentials = errors.New("invalid principal credentials")
	ErrDuplicatePrincipal = errors.New("principal already exists")
	ErrPrincipalNotFound  = errors.New("principal not found")
	ErrGrantNotFound      = errors.New("grant not found")
	ErrLastSystemAdmin    = errors.New("cannot remove the last active system admin")
	ErrInvalidInput       = errors.New("invalid principal input")
)
