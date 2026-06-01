package client

import "knot_db/core/identity"

// AuthInput is the authentication request payload.
type AuthInput struct {
	UserRef  identity.UserRef
	Password string
}

// AuthToken is the authentication result payload.
type AuthToken struct {
	AccessToken string
}
