package admin

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminSemanticErrorPreservesGRPCUnavailable(t *testing.T) {
	err := status.Error(codes.Unavailable, "service is quiesced")
	mapped := mapAdminSemanticError(err, "begin semantic mutation")
	if status.Code(mapped) != codes.Unavailable {
		t.Fatalf("mapAdminSemanticError() code = %v, want %v (err=%v)", status.Code(mapped), codes.Unavailable, mapped)
	}
}

func TestAdminInferenceErrorPreservesGRPCUnavailable(t *testing.T) {
	err := status.Error(codes.Unavailable, "service is quiesced")
	mapped := mapAdminInferenceError(err, "begin semantic mutation")
	if status.Code(mapped) != codes.Unavailable {
		t.Fatalf("mapAdminInferenceError() code = %v, want %v (err=%v)", status.Code(mapped), codes.Unavailable, mapped)
	}
}
