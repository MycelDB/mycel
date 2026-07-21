package service

import (
	"testing"

	runtime "github.com/myceldb/mycel/internal/runtime"
)

func TestIdentityManagersImplementRuntimeService(t *testing.T) {
	var _ runtime.Service = NewUserManager()
	var _ runtime.Service = NewAdminManager()
}
