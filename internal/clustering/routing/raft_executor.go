package routing

import (
	"context"
	"fmt"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
)

type Route struct {
	SpaceID     string
	PartitionID partitioning.PartitionID
	GroupID     consensus.GroupID
	Leader      consensus.NodeID
	LocalNode   consensus.NodeID
}

func (r Route) IsLocalLeader() bool { return r.Leader != 0 && r.Leader == r.LocalNode }

type LeaderResolver interface {
	LeaderForPartition(ctx context.Context, partitionID partitioning.PartitionID) (consensus.NodeID, error)
}

type Forwarder interface {
	ForwardForSpace(ctx context.Context, route Route, fn func(context.Context) error) error
	ForwardForSpaceValue(ctx context.Context, route Route, fn SpaceFunc[any]) (any, error)
}

type RaftExecutor struct {
	PartitionCount uint32
	LocalNode      consensus.NodeID
	Resolver       LeaderResolver
	Forwarder      Forwarder
}

func NewRaftExecutor(partitionCount uint32, localNode consensus.NodeID, resolver LeaderResolver, forwarder Forwarder) RaftExecutor {
	return RaftExecutor{PartitionCount: partitionCount, LocalNode: localNode, Resolver: resolver, Forwarder: forwarder}
}

func (e RaftExecutor) ForSpace(ctx context.Context, spaceID string, fn func(context.Context) error) error {
	route, err := e.RouteForSpace(ctx, spaceID)
	if err != nil {
		return err
	}
	if fn == nil {
		return fmt.Errorf("routing function is required")
	}
	if route.IsLocalLeader() {
		return fn(ctx)
	}
	if e.Forwarder == nil {
		return fmt.Errorf("partition %d leader %d is not local node %d", route.PartitionID, route.Leader, route.LocalNode)
	}
	return e.Forwarder.ForwardForSpace(ctx, route, fn)
}

func RaftForSpaceValue[T any](e RaftExecutor, ctx context.Context, spaceID string, fn SpaceFunc[T]) (T, error) {
	var zero T
	route, err := e.RouteForSpace(ctx, spaceID)
	if err != nil {
		return zero, err
	}
	if fn == nil {
		return zero, fmt.Errorf("routing function is required")
	}
	if route.IsLocalLeader() {
		return fn(ctx)
	}
	if e.Forwarder == nil {
		return zero, fmt.Errorf("partition %d leader %d is not local node %d", route.PartitionID, route.Leader, route.LocalNode)
	}
	value, err := e.Forwarder.ForwardForSpaceValue(ctx, route, func(ctx context.Context) (any, error) { return fn(ctx) })
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("forwarded response has unexpected type %T", value)
	}
	return typed, nil
}

func (e RaftExecutor) ForSpaceValue(ctx context.Context, spaceID string, fn SpaceFunc[any]) (any, error) {
	return RaftForSpaceValue[any](e, ctx, spaceID, fn)
}

func (e RaftExecutor) RouteForSpace(ctx context.Context, spaceID string) (Route, error) {
	partitionCount := e.PartitionCount
	if partitionCount == 0 {
		partitionCount = partitioning.DefaultPartitionCount
	}
	partitionID, err := partitioning.PartitionForSpace(spaceID, partitionCount)
	if err != nil {
		return Route{}, err
	}
	if e.LocalNode == 0 {
		return Route{}, fmt.Errorf("local raft node id is required")
	}
	if e.Resolver == nil {
		return Route{}, fmt.Errorf("leader resolver is required")
	}
	leader, err := e.Resolver.LeaderForPartition(ctx, partitionID)
	if err != nil {
		return Route{}, err
	}
	if leader == 0 {
		return Route{}, fmt.Errorf("partition %d has no leader", partitionID)
	}
	return Route{SpaceID: spaceID, PartitionID: partitionID, GroupID: consensus.PartitionGroupID(partitionID.Uint32()), Leader: leader, LocalNode: e.LocalNode}, nil
}
