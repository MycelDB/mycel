package usermgmt

import (
	"context"

	"knot_db/core/identity"
)

// CreateUserInput is the create payload managed by UserManager.
type CreateUserInput struct {
	User     identity.UserInput
	Password string
}

// UserManager manages users and their credentials.
type UserManager interface {
	Init(ctx context.Context, location string, encryptionKeyB64 string) error
	ExistsByRef(ctx context.Context, ref identity.UserRef) (bool, error)
	GetByRef(ctx context.Context, ref identity.UserRef) (identity.User, error)
	Create(ctx context.Context, in CreateUserInput) (identity.User, error)
}
