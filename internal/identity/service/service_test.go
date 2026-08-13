package service

import (
	"testing"

	runtime "github.com/myceldb/mycel/internal/runtime"
)

func TestIdentityManagerImplementsRuntimeService(t *testing.T) {
	var _ runtime.Service = NewPrincipalManager()
}
