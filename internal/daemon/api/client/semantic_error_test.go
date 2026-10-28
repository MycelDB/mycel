package client

import (
	"errors"
	"testing"

	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapSemanticErrorMapsInferenceDenialToPermissionDenied(t *testing.T) {
	err := mapSemanticError(errors.Join(errors.New("semantic search failed"), inferenceruntime.ErrDenied), "semantic search")
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("mapSemanticError() code=%s error=%v, want PermissionDenied", status.Code(err), err)
	}
}
