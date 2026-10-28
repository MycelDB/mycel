package service

import (
	"errors"
	"testing"

	inferenceruntime "github.com/myceldb/mycel/internal/inference/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRaftSemanticSearchErrorMapsInferenceDenial(t *testing.T) {
	err := raftSemanticSearchError(errors.Join(errors.New("search failed"), inferenceruntime.ErrDenied))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("raftSemanticSearchError() code=%s error=%v, want PermissionDenied", status.Code(err), err)
	}
}

func TestRaftSemanticSearchErrorPreservesOtherErrors(t *testing.T) {
	want := errors.New("storage failed")
	if got := raftSemanticSearchError(want); !errors.Is(got, want) {
		t.Fatalf("raftSemanticSearchError()=%v, want original error", got)
	}
}
