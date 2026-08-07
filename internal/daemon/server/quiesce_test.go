package server

import (
	"context"
	"log/slog"
	"testing"

	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	daemonchange "github.com/myceldb/mycel/internal/changestream/service"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQuiesceUnaryInterceptorAllowsNonQuiescedRequest(t *testing.T) {
	gate := quiesce.NewGate("api-ingress")
	called := false
	interceptor := quiesceUnaryInterceptor(gate, nil)
	_, err := interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, func(ctx context.Context, req any) (any, error) {
		called = true
		if gate.Status().Active != 1 {
			t.Fatalf("active count in handler = %d, want 1", gate.Status().Active)
		}
		return "response", nil
	})
	if err != nil {
		t.Fatalf("interceptor error = %v", err)
	}
	if !called {
		t.Fatal("expected handler to be called")
	}
	if gate.Status().Active != 0 {
		t.Fatalf("active count after handler = %d, want 0", gate.Status().Active)
	}
}

func TestQuiesceUnaryInterceptorRejectsQuiescedRequest(t *testing.T) {
	gate := quiesce.NewGate("api-ingress")
	lease, err := gate.Quiesce(context.Background(), quiesce.Request{Reason: "backup", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(context.Background())
	interceptor := quiesceUnaryInterceptor(gate, nil)
	called := false
	_, err = interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/svc/Method"}, func(ctx context.Context, req any) (any, error) {
		called = true
		return nil, nil
	})
	if called {
		t.Fatal("handler should not be called while quiesced")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error code = %s, want %s", status.Code(err), codes.Unavailable)
	}
}

func TestDefaultQuiesceExemptsAdminAuthRefresh(t *testing.T) {
	exempt := defaultQuiesceExemptMethods()
	for _, method := range []string{adminv1.AdminAuthService_LoginOperator_FullMethodName, adminv1.AdminAuthService_RefreshOperator_FullMethodName, adminv1.AdminAuthService_WhoAmI_FullMethodName} {
		if !exempt[method] {
			t.Fatalf("method %s is not quiesce-exempt", method)
		}
	}
}

func TestDefaultQuiesceExemptsClusterBackupArchiveRPC(t *testing.T) {
	exempt := defaultQuiesceExemptMethods()
	for _, method := range []string{clusterpb.ClusterBackendService_CheckLocalBackupReadiness_FullMethodName, clusterpb.ClusterBackendService_AcquireLocalBackupQuiesce_FullMethodName, clusterpb.ClusterBackendService_ReleaseLocalBackupQuiesce_FullMethodName, clusterpb.ClusterBackendService_AcquireLocalRaftBackupFreeze_FullMethodName, clusterpb.ClusterBackendService_ReleaseLocalRaftBackupFreeze_FullMethodName, clusterpb.ClusterBackendService_CreateLocalBackupArchive_FullMethodName} {
		if !exempt[method] {
			t.Fatalf("method %s is not quiesce-exempt", method)
		}
	}
}

func TestDefaultQuiesceExemptsAdminClusterBackupRPCs(t *testing.T) {
	exempt := defaultQuiesceExemptMethods()
	for _, method := range []string{adminv1.AdminBackupService_TriggerClusterBackup_FullMethodName, adminv1.AdminBackupService_GetClusterBackupStatus_FullMethodName, adminv1.AdminBackupService_ListClusterBackups_FullMethodName, adminv1.AdminBackupService_ValidateClusterBackupSet_FullMethodName} {
		if !exempt[method] {
			t.Fatalf("method %s is not quiesce-exempt", method)
		}
	}
}

func TestDefaultPublicMethodsAllowsClusterBackupArchiveRPC(t *testing.T) {
	public := defaultPublicMethods()
	for _, method := range []string{clusterpb.ClusterBackendService_CheckLocalBackupReadiness_FullMethodName, clusterpb.ClusterBackendService_AcquireLocalBackupQuiesce_FullMethodName, clusterpb.ClusterBackendService_ReleaseLocalBackupQuiesce_FullMethodName, clusterpb.ClusterBackendService_AcquireLocalRaftBackupFreeze_FullMethodName, clusterpb.ClusterBackendService_ReleaseLocalRaftBackupFreeze_FullMethodName, clusterpb.ClusterBackendService_CreateLocalBackupArchive_FullMethodName} {
		if !public[method] {
			t.Fatalf("method %s is not public for backend-token-only calls", method)
		}
	}
}

func TestQuiesceUnaryInterceptorExemptsMethods(t *testing.T) {
	gate := quiesce.NewGate("api-ingress")
	lease, err := gate.Quiesce(context.Background(), quiesce.Request{Reason: "backup", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(context.Background())
	interceptor := quiesceUnaryInterceptor(gate, map[string]bool{"/svc/Exempt": true})
	called := false
	_, err = interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: "/svc/Exempt"}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "response", nil
	})
	if err != nil {
		t.Fatalf("exempt interceptor error = %v", err)
	}
	if !called {
		t.Fatal("expected exempt handler to be called")
	}
}

type testServerStream struct{ grpc.ServerStream }

func (s testServerStream) Context() context.Context { return context.Background() }

func TestServerNewRegistersIngressGateWithCoordinator(t *testing.T) {
	ctx := context.Background()
	coordinator := quiesce.NewCoordinator()
	graphModule := daegraph.NewModule()
	rt := daemonruntime.New(config.Config{DataDir: t.TempDir()}, slog.Default(), "", nil)
	if result := graphModule.Init(ctx, rt); !result.OK {
		t.Fatalf("graph init failed: %v", result.Error)
	}
	blobModule := daemonblob.NewModule(graphModule)
	if result := blobModule.Init(ctx, rt); !result.OK {
		t.Fatalf("blob init failed: %v", result.Error)
	}
	semanticModule := daemonsemantic.NewModule()
	if result := semanticModule.Init(ctx, rt); !result.OK {
		t.Fatalf("semantic init failed: %v", result.Error)
	}
	graphNotificationModule := graphnotification.NewModule()
	if result := graphNotificationModule.Init(ctx, rt); !result.OK {
		t.Fatalf("graph change notification init failed: %v", result.Error)
	}
	changeModule := daemonchange.NewModule()
	if result := changeModule.Init(ctx, rt); !result.OK {
		t.Fatalf("change init failed: %v", result.Error)
	}

	srv, err := New(Config{Addr: "127.0.0.1:0", AdminLister: fakeOperatorManager{}, AdminAuthenticator: fakeOperatorManager{}, OperatorManager: fakeOperatorManager{}, UserManager: fakeUserManager{}, SpaceManager: fakeSpaceManager{}, SessionManager: daemonsession.NewModule(), GraphManager: graphModule, GraphChangeManager: graphNotificationModule, BlobManager: blobModule, SemanticManager: semanticModule, ChangeManager: changeModule, Quiesce: coordinator})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer srv.Stop()
	participants := coordinator.Participants()
	if len(participants) != 1 {
		t.Fatalf("len(participants) = %d, want 1", len(participants))
	}
	if participants[0].Name() != "api-ingress" {
		t.Fatalf("participant name = %q, want api-ingress", participants[0].Name())
	}
}

func TestQuiesceStreamInterceptorRejectsQuiescedRequest(t *testing.T) {
	gate := quiesce.NewGate("api-ingress")
	lease, err := gate.Quiesce(context.Background(), quiesce.Request{Reason: "backup", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(context.Background())
	interceptor := quiesceStreamInterceptor(gate, nil)
	called := false
	err = interceptor("srv", testServerStream{}, &grpc.StreamServerInfo{FullMethod: "/svc/Stream"}, func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	})
	if called {
		t.Fatal("handler should not be called while quiesced")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error code = %s, want %s", status.Code(err), codes.Unavailable)
	}
}
