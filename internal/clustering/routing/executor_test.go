package routing

import (
	"context"
	"errors"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/partitioning"
)

func TestLocalExecutorForSpaceExecutesLocally(t *testing.T) {
	exec := NewLocalExecutor(64)
	called := false
	err := exec.ForSpace(context.Background(), "00000000-0000-0000-0000-000000000001", func(ctx context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("ForSpace() error = %v", err)
	}
	if !called {
		t.Fatal("expected callback to execute")
	}
}

func TestLocalExecutorForSpacePropagatesError(t *testing.T) {
	exec := NewLocalExecutor(64)
	want := errors.New("boom")
	err := exec.ForSpace(context.Background(), "00000000-0000-0000-0000-000000000001", func(ctx context.Context) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("ForSpace() error = %v want %v", err, want)
	}
}

func TestLocalExecutorForSpaceValue(t *testing.T) {
	exec := NewLocalExecutor(64)
	got, err := ForSpaceValue[string](exec, context.Background(), "00000000-0000-0000-0000-000000000001", func(ctx context.Context) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("ForSpaceValue() error = %v", err)
	}
	if got != "ok" {
		t.Fatalf("ForSpaceValue()=%q want ok", got)
	}
}

func TestLocalExecutorValidatesSpaceID(t *testing.T) {
	exec := NewLocalExecutor(64)
	if err := exec.ForSpace(context.Background(), " ", func(ctx context.Context) error { return nil }); err == nil {
		t.Fatal("expected blank space_id to fail")
	}
}

func TestLocalExecutorDefaultPartitionCount(t *testing.T) {
	exec := NewLocalExecutor(0)
	got, err := exec.PartitionForSpace("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatalf("PartitionForSpace() error = %v", err)
	}
	want, err := partitioning.PartitionForSpace("00000000-0000-0000-0000-000000000001", partitioning.DefaultPartitionCount)
	if err != nil {
		t.Fatalf("partitioning.PartitionForSpace() error = %v", err)
	}
	if got != want {
		t.Fatalf("PartitionForSpace()=%d want %d", got, want)
	}
}

func TestLocalExecutorRejectsNilCallback(t *testing.T) {
	exec := NewLocalExecutor(64)
	if err := exec.ForSpace(context.Background(), "00000000-0000-0000-0000-000000000001", nil); err == nil {
		t.Fatal("expected nil callback to fail")
	}
	if _, err := ForSpaceValue[string](exec, context.Background(), "00000000-0000-0000-0000-000000000001", nil); err == nil {
		t.Fatal("expected nil value callback to fail")
	}
}
