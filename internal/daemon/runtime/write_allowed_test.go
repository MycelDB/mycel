package runtime

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/daemon/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequireLocalWriteAllowedStandalone(t *testing.T) {
	rt := New(config.Config{Mode: config.DefaultMode}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); err != nil {
		t.Fatalf("RequireLocalWriteAllowed() error = %v", err)
	}
}

func TestRequireLocalWriteAllowedClusteredFailsClosed(t *testing.T) {
	rt := New(config.Config{Mode: "clustered"}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestRequireLocalWriteAllowedClusteredRejectsUnwiredExecutor(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	rt := New(config.Config{Mode: "clustered"}, nil, "", nil)
	rt.ClusterManager = mgr
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}
