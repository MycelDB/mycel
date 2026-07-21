package routing

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type SpaceFunc[T any] func(context.Context) (T, error)

type PartitionExecutor interface {
	ForSpace(ctx context.Context, spaceID string, fn func(context.Context) error) error
	ForSpaceValue(ctx context.Context, spaceID string, fn SpaceFunc[any]) (any, error)
}

type LocalExecutor struct {
	PartitionCount uint32
}

func NewLocalExecutor(partitionCount uint32) LocalExecutor {
	return LocalExecutor{PartitionCount: partitionCount}
}

func (e LocalExecutor) ForSpace(ctx context.Context, spaceID string, fn func(context.Context) error) error {
	_, err := e.partition(spaceID)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("routing function is required")
	}
	return fn(ctx)
}

func ForSpaceValue[T any](e PartitionExecutor, ctx context.Context, spaceID string, fn SpaceFunc[T]) (T, error) {
	var zero T
	if e == nil {
		return zero, fmt.Errorf("partition executor is required")
	}
	if fn == nil {
		return zero, fmt.Errorf("routing function is required")
	}
	value, err := e.ForSpaceValue(ctx, spaceID, func(ctx context.Context) (any, error) { return fn(ctx) })
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("routed response has unexpected type %T", value)
	}
	return typed, nil
}

func (e LocalExecutor) ForSpaceValue(ctx context.Context, spaceID string, fn SpaceFunc[any]) (any, error) {
	_, err := e.partition(spaceID)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, fmt.Errorf("routing function is required")
	}
	return fn(ctx)
}

func (e LocalExecutor) PartitionForSpace(spaceID string) (partitioning.PartitionID, error) {
	return e.partition(spaceID)
}

func (e LocalExecutor) PartitionForSpaceID(spaceID domainspace.SpaceID) (partitioning.PartitionID, error) {
	return e.partitionID(spaceID)
}

func (e LocalExecutor) partition(spaceID string) (partitioning.PartitionID, error) {
	if strings.TrimSpace(spaceID) == "" {
		return 0, fmt.Errorf("space_id is required")
	}
	partitionCount := e.PartitionCount
	if partitionCount == 0 {
		partitionCount = partitioning.DefaultPartitionCount
	}
	return partitioning.PartitionForSpace(spaceID, partitionCount)
}

func (e LocalExecutor) partitionID(spaceID domainspace.SpaceID) (partitioning.PartitionID, error) {
	if spaceID == uuid.Nil {
		return 0, fmt.Errorf("space_id is required")
	}
	partitionCount := e.PartitionCount
	if partitionCount == 0 {
		partitionCount = partitioning.DefaultPartitionCount
	}
	return partitioning.PartitionForSpaceID(spaceID, partitionCount)
}
