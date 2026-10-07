package server

import (
	"context"
	"log/slog"
	"testing"

	daemonblob "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
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
	interceptor := quiesceUnaryInterceptor(gate, nil, nil)
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
	interceptor, release := quiescedUnaryInterceptor(t, false)
	defer release()
	called, _, err := callUnaryInterceptor(interceptor, "/svc/Method")
	if called {
		t.Fatal("handler should not be called while quiesced")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error code = %s, want %s", status.Code(err), codes.Unavailable)
	}
}

func TestQuiesceUnaryInterceptorAllowsReadOnlyDuringBackupWhenPolicyAllows(t *testing.T) {
	interceptor, release := quiescedUnaryInterceptor(t, true)
	defer release()
	called, _, err := callUnaryInterceptor(interceptor, "/svc/GetThing")
	if err != nil {
		t.Fatalf("read-only interceptor error = %v", err)
	}
	if !called {
		t.Fatal("expected read-only handler to be called")
	}
}

func TestQuiesceUnaryInterceptorRejectsWriteDuringReadAdmittingBackup(t *testing.T) {
	interceptor, release := quiescedUnaryInterceptor(t, true)
	defer release()
	called, _, err := callUnaryInterceptor(interceptor, "/svc/UpdateThing")
	if called {
		t.Fatal("write handler should not be called while quiesced")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("write error code = %s, want %s", status.Code(err), codes.Unavailable)
	}
}

func quiescedUnaryInterceptor(t *testing.T, allowReads bool) (grpc.UnaryServerInterceptor, func()) {
	t.Helper()
	gate := quiesce.NewGate("api-ingress")
	lease, err := gate.Quiesce(context.Background(), quiesce.Request{Reason: "backup", Mode: quiesce.ModeBackup, AllowReadsDuringBackup: allowReads})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	interceptor := quiesceUnaryInterceptor(gate, nil, map[string]bool{"/svc/GetThing": true})
	return interceptor, func() { _ = lease.Release(context.Background()) }
}

func callUnaryInterceptor(interceptor grpc.UnaryServerInterceptor, method string) (bool, any, error) {
	called := false
	res, err := interceptor(context.Background(), "request", &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, req any) (any, error) {
		called = true
		return "response", nil
	})
	return called, res, err
}

func TestDefaultQuiesceReadOnlyMethodsIncludesQueriesButNotMutations(t *testing.T) {
	readOnly := defaultQuiesceReadOnlyMethods()
	if !isQuiesceReadOnlyMethod(clientv1.QueryService_ExecuteQuery_FullMethodName, readOnly) {
		t.Fatalf("ExecuteQuery should be read-only during backup")
	}
	if !isQuiesceReadOnlyMethod(clientv1.GraphService_GetNode_FullMethodName, readOnly) {
		t.Fatalf("GetNode should be read-only by naming convention")
	}
	if isQuiesceReadOnlyMethod(clientv1.GraphService_UpdateNode_FullMethodName, readOnly) {
		t.Fatalf("UpdateNode should not be read-only during backup")
	}
	if isQuiesceReadOnlyMethod(clientv1.TransactionService_CommitTransaction_FullMethodName, readOnly) {
		t.Fatalf("CommitTransaction should not be read-only during backup")
	}
}

func TestDefaultQuiesceExemptsCommonAuth(t *testing.T) {
	exempt := defaultQuiesceExemptMethods()
	for _, method := range []string{commonv1.AuthService_Login_FullMethodName, commonv1.AuthService_Refresh_FullMethodName, commonv1.AuthService_WhoAmI_FullMethodName} {
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
	for _, method := range []string{adminv1.AdminBackupService_StartClusterBackup_FullMethodName, adminv1.AdminBackupService_GetClusterBackupStatus_FullMethodName, adminv1.AdminBackupService_CancelClusterBackup_FullMethodName, adminv1.AdminBackupService_ListClusterBackups_FullMethodName, adminv1.AdminBackupService_ValidateClusterBackupSet_FullMethodName} {
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
	interceptor := quiesceUnaryInterceptor(gate, map[string]bool{"/svc/Exempt": true}, nil)
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

	srv, err := New(Config{Addr: "127.0.0.1:0", PrincipalManager: fakePrincipalManager{}, SpaceManager: fakeSpaceManager{}, SessionManager: daemonsession.NewModule(), GraphManager: graphModule, GraphChangeManager: graphNotificationModule, BlobManager: blobModule, SemanticManager: semanticModule, Quiesce: coordinator})
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
	interceptor := quiesceStreamInterceptor(gate, nil, nil)
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
