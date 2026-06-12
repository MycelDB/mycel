package internal

import "martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"

// AuthInput is the authentication request payload.
type AuthInput struct {
	UserRef  identity.UserRef
	Password string
}

// AccessToken is the opaque bearer token returned on successful auth.
type AccessToken string

// AuthResult is the authentication result returned to external callers.
type AuthResult struct {
	AccessToken AccessToken `json:"access_token"`
}

// CurrentUserInput identifies the bearer token whose authenticated user should be returned.
type CurrentUserInput struct {
	AccessToken AccessToken
}
