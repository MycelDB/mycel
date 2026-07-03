package server

import (
	"context"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	daemonadmin "github.com/myceldb/mycel/internal/daemon/modules/admin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeAdminLister struct {
	admins []daemonadmin.AdminSummary
}

func (f fakeAdminLister) ListAdmins(context.Context) ([]daemonadmin.AdminSummary, error) {
	return f.admins, nil
}

func TestServerRegistersAdminOperatorService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	createdAt := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	srv, errCh, err := Start(ctx, Config{Addr: "127.0.0.1:0", AdminLister: fakeAdminLister{admins: []daemonadmin.AdminSummary{{ID: "admin-1", Username: "admin", CreatedAt: createdAt}}}})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.DialContext(ctx, srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial grpc server: %v", err)
	}
	defer conn.Close()
	client := adminv1.NewAdminOperatorServiceClient(conn)
	res, err := client.ListOperators(ctx, &adminv1.ListOperatorsRequest{})
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
