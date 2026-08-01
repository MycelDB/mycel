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

func TestRequireLocalWriteAllowedStandaloneRaftConfiguredFailsClosed(t *testing.T) {
	rt := New(config.Config{Mode: config.DefaultMode, Cluster: config.ClusterConfig{RaftNodeCount: 1}}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestRequireLocalWriteAllowedMeshFailsClosed(t *testing.T) {
	rt := New(config.Config{Mode: "mesh"}, nil, "", nil)
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestRequireLocalWriteAllowedMeshRejectsUnwiredExecutor(t *testing.T) {
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093"}, nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	rt := New(config.Config{Mode: "mesh"}, nil, "", nil)
	rt.ClusterManager = mgr
	if err := rt.RequireLocalWriteAllowed(); status.Code(err) != codes.Unavailable {
		t.Fatalf("RequireLocalWriteAllowed() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}
