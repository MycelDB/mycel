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

type AdminLister interface {
	ListAdmins(ctx context.Context) ([]AdminSummary, error)
}
