package user

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/model"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	User     model.UserInput
	Password string
}

// Manager manages users and their credentials.
type Manager interface {
	Init(ctx context.Context, location string, encryptionKeyB64 string) error
	ExistsByRef(ctx context.Context, ref model.UserRef) (bool, error)
	GetByRef(ctx context.Context, ref model.UserRef) (model.User, error)
	Create(ctx context.Context, in CreateInput) (model.User, error)
	Authenticate(ctx context.Context, ref model.UserRef, password string) (model.User, error)
}
