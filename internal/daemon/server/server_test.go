package server

import (
	"context"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeAdminLister struct {
	admins []daemonadmin.AdminSummary
}

func (f fakeAdminLister) ListAdmins(context.Context) ([]daemonadmin.AdminSummary, error) {
	return f.admins, nil
}

type fakeAdminAuthenticator struct {
	admin daemonadmin.AdminSummary
}

func (f fakeAdminAuthenticator) AuthenticateOperator(context.Context, string, string) (daemonadmin.AdminSummary, error) {
	return f.admin, nil
}

func TestServerRegistersProtectedAdminOperatorService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	admin := daemonadmin.AdminSummary{ID: "admin-1", Username: "admin", CreatedAt: createdAt}
	tokens := daemonauth.NewTokenManager([]byte("01234567890123456789012345678901"), time.Minute)
	srv, errCh, err := Start(ctx, Config{Addr: "127.0.0.1:0", AdminLister: fakeAdminLister{admins: []daemonadmin.AdminSummary{admin}}, AdminAuthenticator: fakeAdminAuthenticator{admin: admin}, TokenManager: tokens})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.DialContext(ctx, srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial grpc server: %v", err)
	}
	defer conn.Close()
	operatorClient := adminv1.NewAdminOperatorServiceClient(conn)
	if _, err := operatorClient.ListOperators(ctx, &adminv1.ListOperatorsRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated list to fail, got %v", err)
	}
	authClient := adminv1.NewAdminAuthServiceClient(conn)
	login, err := authClient.LoginOperator(ctx, &adminv1.LoginOperatorRequest{Username: "admin", Password: "pass"})
	if err != nil {
		t.Fatalf("LoginOperator() error = %v", err)
	}
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+login.GetAccessToken())
	res, err := operatorClient.ListOperators(authCtx, &adminv1.ListOperatorsRequest{})
	if err != nil {
		t.Fatalf("ListOperators() error = %v", err)
	}
	if len(res.GetOperators()) != 1 || res.GetOperators()[0].GetUsername() != "admin" {
		t.Fatalf("unexpected operators response: %#v", res)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}
