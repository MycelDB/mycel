package client

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientMappersPreserveGRPCUnavailable(t *testing.T) {
	err := status.Error(codes.Unavailable, "service is quiesced")
	checks := map[string]error{
		"graph":   mapGraphError(err, "commit graph"),
		"blob":    mapBlobError(err, "upload blob"),
		"session": mapSessionError(err, "open session"),
		"space":   mapSpaceError(err, "create space"),
		"auth":    mapAuthError(err, "create session"),
	}
	for name, mapped := range checks {
		if status.Code(mapped) != codes.Unavailable {
			t.Fatalf("%s mapper code = %v, want %v (err=%v)", name, status.Code(mapped), codes.Unavailable, mapped)
		}
	}
}
