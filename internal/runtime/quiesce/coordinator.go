package quiesce

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Mode identifies the reason-specific quiesce behavior requested by the daemon.
type Mode string

const (
	// ModeBackup is used when services are quiesced for data-directory backup.
	ModeBackup Mode = "backup"
)

// Request describes a daemon quiesce operation.
type Request struct {
	Reason string
	Mode   Mode
	Source string
}

// Lease reopens or resumes a participant that was quiesced.
type Lease interface {
	Release(context.Context) error
}

// LeaseFunc adapts a function into a Lease.
type LeaseFunc func(context.Context) error

func (f LeaseFunc) Release(ctx context.Context) error {
	if f == nil {
		return nil
	}
	return f(ctx)
}

// Participant is implemented by daemon services or service-owned gates that can
// temporarily stop accepting work and drain active operations.
type Participant interface {
	Name() string
	Quiesce(context.Context, Request) (Lease, error)
	Status() ParticipantStatus
}

// ParticipantStatus is a non-sensitive participant status snapshot.
type ParticipantStatus struct {
	Name      string
	Quiesced  bool
	Active    int
	Reason    string
	Mode      Mode
	Source    string
	Since     time.Time
	LastError string
}

// Status is a non-sensitive coordinator status snapshot.
type Status struct {
	Participants []ParticipantStatus
}

// Coordinator orchestrates quiesce participants in deterministic registration
// order and releases acquired leases in reverse order.
type Coordinator struct {
	mu             sync.RWMutex
	participants   []Participant
	participantsBy map[string]Participant
}

func NewCoordinator() *Coordinator {
	return &Coordinator{participantsBy: map[string]Participant{}}
}

func (c *Coordinator) Register(p Participant) error {
	return c.register(p, false)
}

func (c *Coordinator) RegisterFirst(p Participant) error {
	return c.register(p, true)
}

func (c *Coordinator) register(p Participant, first bool) error {
	if p == nil {
		return errors.New("quiesce participant must not be nil")
	}
	name := strings.TrimSpace(p.Name())
	if name == "" {
		return errors.New("quiesce participant name must not be empty")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.participantsBy == nil {
		c.participantsBy = map[string]Participant{}
	}
	if _, exists := c.participantsBy[name]; exists {
		return fmt.Errorf("quiesce participant %q is already registered", name)
	}
	c.participantsBy[name] = p
	if first {
		c.participants = append([]Participant{p}, c.participants...)
	} else {
		c.participants = append(c.participants, p)
	}
	return nil
}

func (c *Coordinator) Participants() []Participant {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]Participant, len(c.participants))
	copy(out, c.participants)
	return out
}

func (c *Coordinator) Status() Status {
	participants := c.Participants()
	status := Status{Participants: make([]ParticipantStatus, 0, len(participants))}
	for _, p := range participants {
		status.Participants = append(status.Participants, p.Status())
	}
	return status
}

func (c *Coordinator) QuiesceAll(ctx context.Context, req Request) (*CompositeLease, error) {
	participants := c.Participants()
	leases := make([]participantLease, 0, len(participants))
	for _, p := range participants {
		lease, err := p.Quiesce(ctx, req)
		if err != nil {
			rollbackErr := releaseParticipantLeases(context.Background(), leases)
			return nil, errors.Join(err, rollbackErr)
		}
		leases = append(leases, participantLease{name: p.Name(), lease: lease})
	}
	return &CompositeLease{leases: leases}, nil
}

type participantLease struct {
	name  string
	lease Lease
}

// CompositeLease releases participant leases in reverse acquisition order.
type CompositeLease struct {
	mu       sync.Mutex
	leases   []participantLease
	released bool
}

func (l *CompositeLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	leases := l.leases
	l.leases = nil
	l.released = true
	l.mu.Unlock()
	return releaseParticipantLeases(ctx, leases)
}

func releaseParticipantLeases(ctx context.Context, leases []participantLease) error {
	var errs []error
	for i := len(leases) - 1; i >= 0; i-- {
		if leases[i].lease == nil {
			continue
		}
		if err := leases[i].lease.Release(ctx); err != nil {
			errs = append(errs, fmt.Errorf("release quiesce participant %s: %w", leases[i].name, err))
		}
	}
	return errors.Join(errs...)
}
