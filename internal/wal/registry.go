package wal

import (
	"context"
	"fmt"
	"sync"
)

// Applier applies a committed WAL record to durable state.
type Applier interface {
	ApplyWAL(ctx context.Context, rec Record) error
}

// ApplierFunc adapts a function to Applier.
type ApplierFunc func(context.Context, Record) error

func (f ApplierFunc) ApplyWAL(ctx context.Context, rec Record) error { return f(ctx, rec) }

// Registry maps WAL record types to their appliers.
type Registry struct {
	mu       sync.RWMutex
	appliers map[RecordType]Applier
}

func NewRegistry() *Registry { return &Registry{appliers: map[RecordType]Applier{}} }

func (r *Registry) Register(recordType RecordType, applier Applier) error {
	if recordType == "" || applier == nil {
		return fmt.Errorf("%w: record type and applier are required", ErrInvalidRecord)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.appliers[recordType]; exists {
		return fmt.Errorf("%w: duplicate applier for %s", ErrInvalidRecord, recordType)
	}
	r.appliers[recordType] = applier
	return nil
}

func (r *Registry) Applier(recordType RecordType) (Applier, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.appliers[recordType]
	return a, ok
}
