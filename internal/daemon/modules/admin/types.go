package admin

import (
	"context"
	"time"
)

type Admin struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	CreatedAt    time.Time `json:"created_at"`
}

type AdminSummary struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
}

func (a Admin) toSummary() AdminSummary {
	return AdminSummary{ID: a.ID, Username: a.Username, CreatedAt: a.CreatedAt}
}

type AdminLister interface {
	ListAdmins(ctx context.Context) ([]AdminSummary, error)
}

type OperatorAuthenticator interface {
	AuthenticateOperator(ctx context.Context, username string, password string) (AdminSummary, error)
}

type OperatorPasswordManager interface {
	SetOperatorPassword(ctx context.Context, operatorID string, password string) (AdminSummary, error)
}
