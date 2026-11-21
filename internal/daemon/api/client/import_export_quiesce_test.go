package client

import (
	"context"
	"testing"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestImportExportRegistersQuiesceParticipant(t *testing.T) {
	coord := quiesce.NewCoordinator()
	svc := NewImportExportService(nil, nil, nil, nil)
	if err := svc.RegisterQuiesceParticipant(coord); err != nil {
		t.Fatalf("RegisterQuiesceParticipant() error = %v", err)
	}
	participants := coord.Participants()
	if len(participants) != 1 || participants[0].Name() != importExportQuiesceParticipantName {
		t.Fatalf("participants = %#v, want import/export participant", participants)
	}
}

func TestImportExportQueuedSpaceExportRejectedDuringQuiesce(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: "user-1", Username: "alice"})
	coord := quiesce.NewCoordinator()
	svc := NewImportExportService(nil, nil, nil, nil).WithSpaceExportDir(t.TempDir())
	if err := svc.RegisterQuiesceParticipant(coord); err != nil {
		t.Fatalf("RegisterQuiesceParticipant() error = %v", err)
	}
	lease, err := coord.QuiesceAll(context.Background(), quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("QuiesceAll() error = %v", err)
	}
	defer lease.Release(context.Background())
	_, err = svc.CreateSpaceExport(ctx, &clientv1.CreateSpaceExportRequest{Options: &clientv1.SpaceExportOptions{SpaceId: "space-1"}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateSpaceExport() code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
	if reason, ok := quiesce.ErrorInfoReason(err); !ok || reason != quiesce.ReasonClusterQuiesced {
		t.Fatalf("CreateSpaceExport() quiesce reason = %q, %v; want %q, true", reason, ok, quiesce.ReasonClusterQuiesced)
	}
}

func TestImportExportActiveWriteDrainsBeforeQuiesce(t *testing.T) {
	coord := quiesce.NewCoordinator()
	svc := NewImportExportService(nil, nil, nil, nil)
	if err := svc.RegisterQuiesceParticipant(coord); err != nil {
		t.Fatalf("RegisterQuiesceParticipant() error = %v", err)
	}
	releaseWork, err := svc.enterImportExportWrite(context.Background())
	if err != nil {
		t.Fatalf("enterImportExportWrite() error = %v", err)
	}
	leaseCh := make(chan *quiesce.CompositeLease, 1)
	errCh := make(chan error, 1)
	go func() {
		lease, err := coord.QuiesceAll(context.Background(), quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
		if err != nil {
			errCh <- err
			return
		}
		leaseCh <- lease
	}()
	deadline := time.Now().Add(time.Second)
	var status quiesce.ParticipantStatus
	for time.Now().Before(deadline) {
		select {
		case lease := <-leaseCh:
			t.Fatalf("QuiesceAll returned before import/export write drained: %#v", lease)
		case err := <-errCh:
			t.Fatalf("QuiesceAll() error = %v", err)
		default:
		}
		status = coord.Status().Participants[0]
		if status.Quiesced && status.ActiveWrites == 1 && !status.AcceptingWrites {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !status.Quiesced || status.ActiveWrites != 1 || status.AcceptingWrites {
		t.Fatalf("unexpected import/export participant status while draining: %#v", status)
	}
	releaseWork()
	select {
	case lease := <-leaseCh:
		if err := lease.Release(context.Background()); err != nil {
			t.Fatalf("Release() error = %v", err)
		}
	case err := <-errCh:
		t.Fatalf("QuiesceAll() error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("QuiesceAll did not return after import/export write drained")
	}
}
