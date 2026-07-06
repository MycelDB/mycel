package graphchange

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMultiSinkInvokesSinksInOrder(t *testing.T) {
	calls := []string{}
	sink := MultiSink{
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls = append(calls, "first")
			return nil
		}),
		nil,
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls = append(calls, "second")
			return nil
		}),
	}
	if err := sink.OnGraphCommitted(context.Background(), CommittedEvent{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestMultiSinkJoinsErrorsAndContinues(t *testing.T) {
	errA := errors.New("a")
	errB := errors.New("b")
	calls := 0
	sink := MultiSink{
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls++
			return errA
		}),
		SinkFunc(func(context.Context, CommittedEvent) error {
			calls++
			return errB
		}),
	}
	err := sink.OnGraphCommitted(context.Background(), CommittedEvent{})
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("expected joined errors, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}
