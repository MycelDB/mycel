package user

import (
	"context"

	"github.com/myceldb/mycel/domain/identity"
)

// CreateInput is the create payload managed by Manager.
type CreateInput struct {
	User     identity.UserInput
	Password string
}

// Manager manages users and their credentials.
type Manager interface {
	Init(ctx context.Context, location string, encryptionKeyB64 string) error
	ExistsByRef(ctx context.Context, ref identity.UserRef) (bool, error)
	GetByRef(ctx context.Context, ref identity.UserRef) (identity.User, error)
	GetByID(ctx context.Context, id identity.UserID) (identity.User, error)
	List(ctx context.Context) ([]identity.User, error)
	Create(ctx context.Context, in CreateInput) (identity.User, error)
	DeleteByID(ctx context.Context, id identity.UserID) error
	Authenticate(ctx context.Context, ref identity.UserRef, password string) (identity.User, error)
}
