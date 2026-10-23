package diagnostics

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// CancellationTrace records one bounded stage and duration when an operation's
// context is canceled. It intentionally carries no request or resource data.
type CancellationTrace struct {
	mu      sync.Mutex
	once    sync.Once
	logger  *slog.Logger
	message string
	stage   string
	started time.Time
	done    chan struct{}
}

func BeginCancellationTrace(ctx context.Context, logger *slog.Logger, stage, message string) (*CancellationTrace, func()) {
	trace := &CancellationTrace{logger: logger, message: message, stage: stage, started: time.Now(), done: make(chan struct{})}
	stopCallback := context.AfterFunc(ctx, func() { trace.log(ctx.Err()) })
	return trace, func() {
		if stopCallback() {
			trace.once.Do(func() { close(trace.done) })
		}
		<-trace.done
	}
}

func (t *CancellationTrace) Done() <-chan struct{} { return t.done }

func (t *CancellationTrace) SetStage(stage string) {
	t.mu.Lock()
	t.stage = stage
	t.mu.Unlock()
}

func (t *CancellationTrace) log(err error) {
	t.once.Do(func() {
		defer close(t.done)
		if t.logger == nil {
			return
		}
		t.mu.Lock()
		stage := t.stage
		t.mu.Unlock()
		failureClass := "canceled"
		if errors.Is(err, context.DeadlineExceeded) {
			failureClass = "deadline_exceeded"
		}
		t.logger.Warn(t.message, "stage", stage, "duration", time.Since(t.started).Round(time.Millisecond), "failure_class", failureClass)
	})
}
