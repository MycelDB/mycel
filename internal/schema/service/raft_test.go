package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/schema/dsl"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/schema/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRaftStateMachineAppliesSchemaPutAndDelete(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager(storage.NewMemoryStore())
	domainID := graph.DomainID(uuid.New())
	value, err := dsl.Parse(schemaRaftTestSource())
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	value.DomainID = domainID

	putCmd, err := mgr.buildSchemaPutRaftCommand(schemaPutRecord{Schema: value}, 64, "schema-put-1")
	if err != nil {
		t.Fatalf("build put command: %v", err)
	}
	sm := RaftStateMachine{Manager: mgr, PartitionCount: 64}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, putCmd); err != nil {
		t.Fatalf("apply schema put: %v", err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); err != nil {
		t.Fatalf("GetDomainSchema() after put error = %v", err)
	}

	deleteCmd, err := mgr.buildSchemaDeleteRaftCommand(schemaDeleteRecord{DomainID: domainID}, 64, "schema-delete-1")
	if err != nil {
		t.Fatalf("build delete command: %v", err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, deleteCmd); err != nil {
		t.Fatalf("apply schema delete: %v", err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("GetDomainSchema() after delete error = %v, want ErrSchemaNotFound", err)
	}
}

func TestPutDomainSchemaUsesRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mgr := NewManager(storage.NewMemoryStore())
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
		return RaftStateMachine{Manager: mgr, PartitionCount: 4}
	}}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	domainID := graph.DomainID(uuid.New())
	cmd, err := mgr.buildSchemaPutRaftCommand(schemaPutRecord{Schema: mustSchemaRaftTestValue(t, domainID)}, 4, "leader-check")
	if err != nil {
		t.Fatalf("build put command: %v", err)
	}
	group, ok := groups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok {
		t.Fatalf("partition group %d not found", cmd.PartitionID)
	}
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	mgr.EnableExperimentalRaft(groups, 4)
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, schemaRaftTestSource()); err != nil {
		t.Fatalf("PutDomainSchemaGWL() error = %v", err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); err != nil {
		t.Fatalf("GetDomainSchema() after raft put error = %v", err)
	}
	if err := mgr.DeleteDomainSchema(ctx, domainID); err != nil {
		t.Fatalf("DeleteDomainSchema() error = %v", err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("GetDomainSchema() after raft delete error = %v, want ErrSchemaNotFound", err)
	}
}

func TestSchemaRaftCommitFailsClosedWithoutLeader(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager(storage.NewMemoryStore())
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 64, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
		return RaftStateMachine{Manager: mgr, PartitionCount: 64}
	}}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("NewMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	mgr.EnableExperimentalRaft(groups, 64)

	domainID := graph.DomainID(uuid.New())
	if err := mgr.PutDomainSchemaGWL(ctx, domainID, schemaRaftTestSource()); status.Code(err) != codes.Unavailable {
		t.Fatalf("PutDomainSchemaGWL() error = %v, want Unavailable", err)
	}
	if _, err := mgr.GetDomainSchema(ctx, domainID); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("schema was applied despite failed raft proposal: %v", err)
	}
}

func mustSchemaRaftTestValue(t *testing.T, domainID graph.DomainID) schema.DomainSchema {
	t.Helper()
	value, err := dsl.Parse(schemaRaftTestSource())
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	value.DomainID = domainID
	return value
}

func schemaRaftTestSource() string {
	return `schema "Test" version "1" mode strict
node Person {
  record_type: enum person required
}`
}
