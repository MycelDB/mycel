package routing

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRouteErrorCanonicalMapping(t *testing.T) {
	err := NewSessionHomeMismatch("session home mismatch", WithLocalNode(1), WithHomeNode(2), WithTargetNode(3), WithBackendAddr("127.0.0.1:9093"), WithCause(context.DeadlineExceeded))
	if !errors.Is(err, ErrSessionHomeMismatch) {
		t.Fatalf("errors.Is(..., ErrSessionHomeMismatch)=false for %v", err)
	}
	if errors.Is(err, ErrRouteUnavailable) {
		t.Fatalf("home mismatch must not match unavailable")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status.Code()=%v want FailedPrecondition", status.Code(err))
	}
	for _, want := range []string{"session home mismatch", "local_node=1", "home_node=2", "target_node=3", "backend_addr=\"127.0.0.1:9093\"", "context deadline exceeded"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("RouteError %q missing %q", err.Error(), want)
		}
	}
}

func TestRouteErrorGRPCCodes(t *testing.T) {
	cases := []struct {
		err  error
		want codes.Code
	}{
		{NewRouteUnavailable("home unavailable"), codes.Unavailable},
		{NewUnknownSessionHome("unknown session"), codes.NotFound},
		{NewSessionHomeMismatch("mismatch"), codes.FailedPrecondition},
		{NewForwardingLoop("loop"), codes.FailedPrecondition},
	}
	for _, tc := range cases {
		if got := status.Code(tc.err); got != tc.want {
			t.Fatalf("status.Code(%v)=%v want %v", tc.err, got, tc.want)
		}
	}
}

func TestForwardingGuardAddsDepthAndRejectsSecondHop(t *testing.T) {
	guard := ForwardingGuard{LocalNode: consensus.NodeID(2), MaxDepth: 1}
	out, err := guard.OutgoingContext(context.Background())
	if err != nil {
		t.Fatalf("OutgoingContext() error = %v", err)
	}
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get(RouteDepthMetadataKey); len(got) != 1 || got[0] != "1" {
		t.Fatalf("route depth metadata=%v want [1]", got)
	}
	if got := md.Get(ForwardedFromMetadataKey); len(got) != 1 || got[0] != "2" {
		t.Fatalf("forwarded-from metadata=%v want [2]", got)
	}

	incoming := metadata.NewIncomingContext(context.Background(), md)
	if err := guard.Check(incoming); !errors.Is(err, ErrForwardingLoop) {
		t.Fatalf("Check(forwarded) error=%v want ErrForwardingLoop", err)
	}
	if _, err := guard.OutgoingContext(incoming); !errors.Is(err, ErrForwardingLoop) {
		t.Fatalf("OutgoingContext(forwarded) error=%v want ErrForwardingLoop", err)
	}
}

func TestForwardingGuardAllowsConfiguredDepth(t *testing.T) {
	guard := ForwardingGuard{LocalNode: 1, MaxDepth: 2}
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs(RouteDepthMetadataKey, "1"))
	out, err := guard.OutgoingContext(incoming)
	if err != nil {
		t.Fatalf("OutgoingContext(depth 1, max 2) error = %v", err)
	}
	md, _ := metadata.FromOutgoingContext(out)
	if got := md.Get(RouteDepthMetadataKey); len(got) != 1 || got[0] != "2" {
		t.Fatalf("route depth metadata=%v want [2]", got)
	}
}
