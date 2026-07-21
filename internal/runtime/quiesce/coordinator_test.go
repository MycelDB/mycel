package quiesce

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type testParticipant struct {
	name    string
	status  ParticipantStatus
	quiesce func(context.Context, Request) (Lease, error)
}

func (p testParticipant) Name() string { return p.name }
func (p testParticipant) Status() ParticipantStatus {
	if p.status.Name == "" {
		p.status.Name = p.name
	}
	return p.status
}
func (p testParticipant) Quiesce(ctx context.Context, req Request) (Lease, error) {
	if p.quiesce != nil {
		return p.quiesce(ctx, req)
	}
	return LeaseFunc(func(context.Context) error { return nil }), nil
}

func TestCoordinatorRegisterRejectsInvalidParticipants(t *testing.T) {
	c := NewCoordinator()
	if err := c.Register(nil); err == nil {
		t.Fatal("expected nil participant error")
	}
	if err := c.Register(testParticipant{name: "   "}); err == nil {
		t.Fatal("expected empty participant name error")
	}
	if err := c.Register(testParticipant{name: "graph"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := c.Register(testParticipant{name: "graph"}); err == nil {
		t.Fatal("expected duplicate participant error")
	}
}

func TestCoordinatorRegisterFirstPrependsParticipant(t *testing.T) {
	c := NewCoordinator()
	if err := c.Register(testParticipant{name: "graph"}); err != nil {
		t.Fatalf("Register(graph) error = %v", err)
	}
	if err := c.Register(testParticipant{name: "session"}); err != nil {
		t.Fatalf("Register(session) error = %v", err)
	}
	if err := c.RegisterFirst(testParticipant{name: "api-ingress"}); err != nil {
		t.Fatalf("RegisterFirst(api-ingress) error = %v", err)
	}
	participants := c.Participants()
	got := []string{participants[0].Name(), participants[1].Name(), participants[2].Name()}
	want := []string{"api-ingress", "graph", "session"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("participants = %#v, want %#v", got, want)
	}
}

func TestCoordinatorParticipantsReturnsCopy(t *testing.T) {
	c := NewCoordinator()
	if err := c.Register(testParticipant{name: "graph"}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	participants := c.Participants()
	participants[0] = testParticipant{name: "mutated"}
	if got := c.Participants()[0].Name(); got != "graph" {
		t.Fatalf("participant name = %q, want graph", got)
	}
}

func TestCoordinatorQuiesceAllOrderAndReverseRelease(t *testing.T) {
	c := NewCoordinator()
	var events []string
	for _, name := range []string{"api", "semantic", "graph"} {
		name := name
		if err := c.Register(testParticipant{name: name, quiesce: func(context.Context, Request) (Lease, error) {
			events = append(events, "quiesce "+name)
			return LeaseFunc(func(context.Context) error {
				events = append(events, "release "+name)
				return nil
			}), nil
		}}); err != nil {
			t.Fatalf("Register(%s) error = %v", name, err)
		}
	}

	lease, err := c.QuiesceAll(context.Background(), Request{Reason: "backup", Mode: ModeBackup})
	if err != nil {
		t.Fatalf("QuiesceAll() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	want := []string{"quiesce api", "quiesce semantic", "quiesce graph", "release graph", "release semantic", "release api"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestCoordinatorQuiesceAllFailureRollsBack(t *testing.T) {
	failure := errors.New("semantic failed")
	c := NewCoordinator()
	var events []string
	_ = c.Register(testParticipant{name: "api", quiesce: func(context.Context, Request) (Lease, error) {
		events = append(events, "quiesce api")
		return LeaseFunc(func(context.Context) error {
			events = append(events, "release api")
			return nil
		}), nil
	}})
	_ = c.Register(testParticipant{name: "semantic", quiesce: func(context.Context, Request) (Lease, error) {
		events = append(events, "quiesce semantic")
		return nil, failure
	}})
	_ = c.Register(testParticipant{name: "graph", quiesce: func(context.Context, Request) (Lease, error) {
		events = append(events, "quiesce graph")
		return LeaseFunc(func(context.Context) error { return nil }), nil
	}})

	lease, err := c.QuiesceAll(context.Background(), Request{Reason: "backup", Mode: ModeBackup})
	if lease != nil {
		t.Fatalf("lease = %#v, want nil", lease)
	}
	if !errors.Is(err, failure) {
		t.Fatalf("QuiesceAll() error = %v, want %v", err, failure)
	}
	want := []string{"quiesce api", "quiesce semantic", "release api"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
}

func TestCoordinatorStatus(t *testing.T) {
	c := NewCoordinator()
	_ = c.Register(testParticipant{name: "graph", status: ParticipantStatus{Name: "graph", Quiesced: true, Active: 2, Reason: "backup", Mode: ModeBackup}})
	status := c.Status()
	if len(status.Participants) != 1 {
		t.Fatalf("len(status.Participants) = %d, want 1", len(status.Participants))
	}
	got := status.Participants[0]
	if got.Name != "graph" || !got.Quiesced || got.Active != 2 || got.Mode != ModeBackup {
		t.Fatalf("unexpected status: %#v", got)
	}
}

func TestCompositeLeaseReleaseIsIdempotent(t *testing.T) {
	c := NewCoordinator()
	releases := 0
	_ = c.Register(testParticipant{name: "graph", quiesce: func(context.Context, Request) (Lease, error) {
		return LeaseFunc(func(context.Context) error {
			releases++
			return nil
		}), nil
	}})
	lease, err := c.QuiesceAll(context.Background(), Request{Reason: "backup", Mode: ModeBackup})
	if err != nil {
		t.Fatalf("QuiesceAll() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("first Release() error = %v", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if releases != 1 {
		t.Fatalf("releases = %d, want 1", releases)
	}
}
