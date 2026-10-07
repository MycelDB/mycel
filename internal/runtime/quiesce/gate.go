package quiesce

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrQuiesced indicates that a participant is not accepting new work because
	// it is quiesced for backup or maintenance.
	ErrQuiesced = errors.New("service is quiesced")

	// ErrAlreadyQuiesced indicates that a participant already has an active
	// quiesce lease.
	ErrAlreadyQuiesced = errors.New("service is already quiesced")
)

// Gate is a reusable quiesce participant for services that only need admission
// control and active-operation draining.
type Gate struct {
	mu                     sync.Mutex
	name                   string
	closed                 bool
	active                 int
	activeReads            int
	allowReadsDuringBackup bool
	reason                 string
	mode                   Mode
	source                 string
	since                  time.Time
	lastErr                string
	changed                chan struct{}
}

func NewGate(name string) *Gate {
	return &Gate{name: name, changed: make(chan struct{})}
}

func (g *Gate) Name() string {
	if g == nil {
		return ""
	}
	return g.name
}

// Enter admits one unit of service work. The returned release function must be
// called exactly once by callers that received a nil error.
func (g *Gate) Enter(ctx context.Context) (func(), error) {
	return g.enter(ctx, false)
}

// EnterRead admits one read-only unit of work. During backup quiesce, reads are
// admitted only when the quiesce request explicitly allowed reads. Read work is
// tracked for status but does not block backup quiesce acquisition.
func (g *Gate) EnterRead(ctx context.Context) (func(), error) {
	return g.enter(ctx, true)
}

func (g *Gate) enter(ctx context.Context, readOnly bool) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed && !(readOnly && g.readsAllowedDuringQuiesceLocked()) {
		return nil, ErrQuiesced
	}
	if readOnly {
		g.activeReads++
	} else {
		g.active++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if readOnly {
				if g.activeReads > 0 {
					g.activeReads--
				}
			} else if g.active > 0 {
				g.active--
			}
			g.notifyLocked()
			g.mu.Unlock()
		})
	}, nil
}

func (g *Gate) Quiesce(ctx context.Context, req Request) (Lease, error) {
	if g == nil {
		return LeaseFunc(func(context.Context) error { return nil }), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil, ErrAlreadyQuiesced
	}
	g.closed = true
	g.reason = req.Reason
	g.mode = req.Mode
	g.source = req.Source
	g.allowReadsDuringBackup = req.Mode == ModeBackup && req.AllowReadsDuringBackup
	g.since = time.Now().UTC()
	g.lastErr = ""
	g.notifyLocked()
	for g.active > 0 || (!g.allowReadsDuringBackup && g.activeReads > 0) {
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.closed = false
			g.reason = ""
			g.mode = ""
			g.source = ""
			g.allowReadsDuringBackup = false
			g.since = time.Time{}
			g.lastErr = ctx.Err().Error()
			g.notifyLocked()
			g.mu.Unlock()
			return nil, ctx.Err()
		case <-changed:
			g.mu.Lock()
		}
	}
	g.mu.Unlock()
	return &gateLease{gate: g}, nil
}

func (g *Gate) Status() ParticipantStatus {
	if g == nil {
		return ParticipantStatus{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return ParticipantStatus{Name: g.name, Quiesced: g.closed, Active: g.active + g.activeReads, Reason: g.reason, Mode: g.mode, Source: g.source, Since: g.since, LastError: g.lastErr}
}

func (g *Gate) readsAllowedDuringQuiesceLocked() bool {
	return g.closed && g.mode == ModeBackup && g.allowReadsDuringBackup
}

func (g *Gate) releaseLease() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		return nil
	}
	g.closed = false
	g.reason = ""
	g.mode = ""
	g.source = ""
	g.allowReadsDuringBackup = false
	g.since = time.Time{}
	g.notifyLocked()
	return nil
}

func (g *Gate) notifyLocked() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

type gateLease struct {
	gate *Gate
	once sync.Once
	err  error
}

func (l *gateLease) Release(ctx context.Context) error {
	if l == nil || l.gate == nil {
		return nil
	}
	l.once.Do(func() {
		l.err = l.gate.releaseLease()
	})
	return l.err
}
