package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequireWriteAuthorityAllowsNoClusterManagerAndStandalone(t *testing.T) {
	if err := (&Runtime{}).RequireWriteAuthority(); err != nil {
		t.Fatalf("nil cluster manager should allow writes: %v", err)
	}
	mgr, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if mgr.State() != model.NodeStateStandalone {
		t.Fatalf("test expected standalone state, got %s", mgr.State())
	}
	if err := (&Runtime{ClusterManager: mgr}).RequireWriteAuthority(); err != nil {
		t.Fatalf("standalone should allow writes: %v", err)
	}
}

func TestRequireWriteAuthorityAllowsPrimary(t *testing.T) {
	primary, err := clustering.NewManager(context.Background(), clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9093", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Runtime{ClusterManager: primary}).RequireWriteAuthority(); err != nil {
		t.Fatalf("primary should allow writes: %v", err)
	}
}

func TestRequireWriteAuthorityRejectsUnadmittedAndFollower(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	follower, err := clustering.NewManager(ctx, clustering.Options{DataDir: dir, NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Runtime{ClusterManager: follower}).RequireWriteAuthority(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unadmitted should be permission denied, got %v", err)
	}
	admitted, err := clustering.AdmitLocalNode(ctx, dir, follower.Identity().ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	_ = admitted
	if err := follower.SetAuthority(ctx, clustering.Authority{Version: clustering.AuthorityVersion, ClusterID: follower.Identity().ClusterID, Primary: clustering.AuthorityPrimary{NodeID: "node-a"}, AuthorityEpoch: 1, Source: clustering.AuthoritySourceBootstrap, UpdatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	// Recreate manager to pick up admitted identity from disk.
	follower, err = clustering.NewManager(ctx, clustering.Options{DataDir: dir, NodeName: "node-b", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:9094"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = (&Runtime{ClusterManager: follower}).RequireWriteAuthority()
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("follower should be failed precondition, got %v", err)
	}
	st, _ := status.FromError(err)
	foundHint := false
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetReason() == NotPrimaryReason && info.GetMetadata()[PrimaryNodeIDKey] == "node-a" {
			foundHint = true
		}
	}
	if !foundHint {
		t.Fatalf("expected primary hint detail, got %#v", st.Details())
	}
}
