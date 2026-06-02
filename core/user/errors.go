package user

import "errors"

var (
	ErrInvalidInput     = errors.New("invalid input")
	ErrUserNotFound     = errors.New("user not found")
	ErrDuplicateUserRef = errors.New("duplicate user_ref")
	ErrInvalidKey       = errors.New("invalid encryption key")
	ErrDecryptFailed    = errors.New("failed to decrypt user store")
)
