package client

import "knot_db/core/identity"

// AuthInput is the authentication request payload.
type AuthInput struct {
	UserRef  identity.UserRef
	Password string
}

// AuthToken is the JWT-like access token payload returned on successful auth.
type AuthToken struct {
	Iss      string           `json:"iss"`
	Sub      string           `json:"sub"`
	Aud      string           `json:"aud"`
	JTI      string           `json:"jti"`
	IAT      int64            `json:"iat"`
	EXP      int64            `json:"exp"`
	UserID   identity.UserID  `json:"user_id"`
	UserRef  identity.UserRef `json:"user_ref"`
	Roles    []string         `json:"roles"`
	OwnerIDs []string         `json:"owner_ids"`
	SpaceIDs []string         `json:"space_ids"`
	Scopes   []string         `json:"scopes"`
}
