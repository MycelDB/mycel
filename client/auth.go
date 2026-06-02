package client

import "knot_db/core/model"

// AuthInput is the authentication request payload.
type AuthInput struct {
	UserRef  model.UserRef
	Password string
}

// AccessToken is the opaque bearer token returned on successful auth.
type AccessToken string

// AuthResult is the authentication result returned to external callers.
type AuthResult struct {
	AccessToken AccessToken `json:"access_token"`
}
