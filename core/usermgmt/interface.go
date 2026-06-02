package usermgmt

import (
	"context"

	"knot_db/model"
)

// CreateUserInput is the create payload managed by UserManager.
type CreateUserInput struct {
	User     model.UserInput
	Password string
}

// UserManager manages users and their credentials.
type UserManager interface {
	Init(ctx context.Context, location string, encryptionKeyB64 string) error
	ExistsByRef(ctx context.Context, ref model.UserRef) (bool, error)
	GetByRef(ctx context.Context, ref model.UserRef) (model.User, error)
	Create(ctx context.Context, in CreateUserInput) (model.User, error)
	Authenticate(ctx context.Context, ref model.UserRef, password string) (model.User, error)
}
