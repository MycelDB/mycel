package quiesce

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGateEnterReleaseUpdatesActiveStatus(t *testing.T) {
	g := NewGate("graph")
	release, err := g.Enter(context.Background())
	if err != nil {
		t.Fatalf("Enter() error = %v", err)
	}
	if status := g.Status(); status.Name != "graph" || status.Active != 1 || status.Quiesced {
		t.Fatalf("unexpected active status: %#v", status)
	}
	release()
	release()
	if status := g.Status(); status.Active != 0 || status.Quiesced {
		t.Fatalf("unexpected released status: %#v", status)
	}
}

func TestGateQuiesceWaitsForActiveWorkToDrain(t *testing.T) {
	g := NewGate("graph")
	releaseWork, err := g.Enter(context.Background())
	if err != nil {
		t.Fatalf("Enter() error = %v", err)
	}
	leaseCh := make(chan Lease, 1)
	errCh := make(chan error, 1)
	go func() {
		lease, err := g.Quiesce(context.Background(), Request{Reason: "backup", Mode: ModeBackup, Source: "test"})
		if err != nil {
			errCh <- err
			return
		}
		leaseCh <- lease
	}()

	select {
	case lease := <-leaseCh:
		t.Fatalf("Quiesce returned before active work drained: %#v", lease)
	case err := <-errCh:
		t.Fatalf("Quiesce error = %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	status := g.Status()
	if !status.Quiesced || status.Active != 1 || status.Reason != "backup" || status.Mode != ModeBackup || status.Source != "test" || status.Since.IsZero() {
		t.Fatalf("unexpected quiescing status: %#v", status)
	}
	releaseWork()

	var lease Lease
	select {
	case lease = <-leaseCh:
	case err := <-errCh:
		t.Fatalf("Quiesce error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("Quiesce did not return after active work drained")
	}
	if status := g.Status(); !status.Quiesced || status.Active != 0 {
		t.Fatalf("unexpected drained status: %#v", status)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if status := g.Status(); status.Quiesced || status.Reason != "" || !status.Since.IsZero() {
		t.Fatalf("unexpected reopened status: %#v", status)
	}
}

func TestGateEnterFailsWhileQuiescedAndReopensOnRelease(t *testing.T) {
	g := NewGate("blob")
	lease, err := g.Quiesce(context.Background(), Request{Reason: "backup", Mode: ModeBackup})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if _, err := g.Enter(context.Background()); !errors.Is(err, ErrQuiesced) {
		t.Fatalf("Enter() error = %v, want ErrQuiesced", err)
	}
	if _, err := g.Quiesce(context.Background(), Request{Reason: "nested"}); !errors.Is(err, ErrAlreadyQuiesced) {
		t.Fatalf("nested Quiesce() error = %v, want ErrAlreadyQuiesced", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if release, err := g.Enter(context.Background()); err != nil {
		t.Fatalf("Enter() after release error = %v", err)
	} else {
		release()
	}
}

func TestGateQuiesceContextCancellationReopensGate(t *testing.T) {
	g := NewGate("semantic")
	releaseWork, err := g.Enter(context.Background())
	if err != nil {
		t.Fatalf("Enter() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := g.Quiesce(ctx, Request{Reason: "backup", Mode: ModeBackup})
		errCh <- err
	}()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if g.Status().Quiesced {
			break
		}
	}
	if !g.Status().Quiesced {
		t.Fatal("gate did not enter quiesced state")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Quiesce() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Quiesce did not return after cancellation")
	}
	status := g.Status()
	if status.Quiesced || status.Active != 1 || status.LastError == "" {
		t.Fatalf("unexpected canceled status: %#v", status)
	}
	if release, err := g.Enter(context.Background()); err != nil {
		t.Fatalf("Enter() after canceled quiesce error = %v", err)
	} else {
		release()
	}
	releaseWork()
}

func TestGateEnterHonorsCanceledContext(t *testing.T) {
	g := NewGate("graph")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.Enter(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Enter() error = %v, want context.Canceled", err)
	}
}

func TestGateReleaseIsIdempotent(t *testing.T) {
	g := NewGate("space")
	lease, err := g.Quiesce(context.Background(), Request{Reason: "backup", Mode: ModeBackup})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if g.Status().Quiesced {
		t.Fatal("gate should be reopened")
	}
}
