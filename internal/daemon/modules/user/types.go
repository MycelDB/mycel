package user

import (
	"context"
	"time"

	domainauth "github.com/myceldb/mycel/domain/auth"
)

const (
	ModuleName = "user"

	UserStateActive   = "active"
	UserStateDisabled = "disabled"
	UserStateDeleted  = "deleted"
)

type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	State        string    `json:"state"`
	PasswordHash string    `json:"password_hash,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type UserSummary struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (u User) normalized() User {
	if u.State == "" {
		u.State = UserStateActive
	}
	if u.UpdatedAt.IsZero() {
		u.UpdatedAt = u.CreatedAt
	}
	return u
}

func (u User) toSummary() UserSummary {
	u = u.normalized()
	return UserSummary{ID: u.ID, Username: u.Username, State: u.State, CreatedAt: u.CreatedAt, UpdatedAt: u.UpdatedAt}
}

type Manager interface {
	ListUsers(ctx context.Context) ([]UserSummary, error)
	GetUser(ctx context.Context, userID string) (UserSummary, error)
	FindUser(ctx context.Context, username string) (UserSummary, error)
	CreateUser(ctx context.Context, input CreateUserInput) (UserSummary, error)
	DisableUser(ctx context.Context, userID string) (UserSummary, error)
	EnableUser(ctx context.Context, userID string) (UserSummary, error)
	DeleteUser(ctx context.Context, userID string) (UserSummary, error)
	SetUserPassword(ctx context.Context, userID string, password string) (UserSummary, error)
	AuthenticateUser(ctx context.Context, username string, password string) (UserSummary, error)
	CreateAuthSession(ctx context.Context, user UserSummary, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration, absoluteTTL time.Duration) (domainauth.RefreshToken, domainauth.RefreshSession, error)
	RefreshAuthSession(ctx context.Context, refreshToken domainauth.RefreshToken, metadata domainauth.RefreshSessionMetadata, tokenBytes int, idleTTL time.Duration) (UserSummary, domainauth.RefreshToken, domainauth.RefreshSession, error)
	ListUserSessions(ctx context.Context, userID string) ([]domainauth.RefreshSession, error)
	RevokeUserSession(ctx context.Context, userID string, sessionID string) error
	RevokeUserSessions(ctx context.Context, userID string) (int, error)
}

type CreateUserInput struct {
	Username string
	Password string
	Disabled bool
}
