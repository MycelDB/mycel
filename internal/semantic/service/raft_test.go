package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clustermodel "github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticRaftStateMachineAppliesGlobalAndMaintenanceMutations(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	m.raftPartitionCount = 64
	sm := RaftStateMachine{Module: m, PartitionCount: 64}
	vectorStore := semanticVectorStore("global-raft")
	globalRec := semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(vectorStore)}
	globalPayload, err := json.Marshal(globalRec)
	if err != nil {
		t.Fatal(err)
	}
	globalCmd, err := m.buildSemanticGlobalRaftCommand(globalRec, globalPayload, "semantic-global-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, globalCmd); err != nil {
		t.Fatalf("apply global command: %v", err)
	}
	stores, err := m.globalBase.ListVectorStores(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasVectorStoreKey(stores, vectorStore.Key) {
		t.Fatalf("vector stores=%#v want key %q", stores, vectorStore.Key)
	}

	spaceID := domainspace.SpaceID(uuid.New())
	dirtyEvent := semanticDirtyEvent(spaceID)
	maintRec := maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: spaceID, Payload: raw(dirtyEvent)}
	maintPayload, err := json.Marshal(maintRec)
	if err != nil {
		t.Fatal(err)
	}
	maintCmd, err := m.buildSemanticMaintenanceRaftCommand(maintRec, maintPayload, "semantic-maintenance-"+spaceID.String()+"-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, maintCmd); err != nil {
		t.Fatalf("apply maintenance command: %v", err)
	}
	mgr, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != dirtyEvent.ID {
		t.Fatalf("dirty events=%#v want %s", events, dirtyEvent.ID)
	}
}

func TestSemanticGlobalUsesSystemRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wm, err := wal.Open(ctx, wal.Options{Dir: t.TempDir(), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	m := NewModule()
	host := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default(), WAL: wm, WALRegistry: wal.NewRegistry(), WALWaiter: wal.NewApplyWaiter()}
	if result := m.Init(ctx, host); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	systemGroup, _ := groups.Group(consensus.SystemGroupID)
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return systemGroup.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	vectorStore := semanticVectorStore("global-system-raft")
	vectorStore.ID = uuid.Nil
	storedVectorStore, err := m.GlobalManager().UpsertVectorStore(ctx, vectorStore)
	if err != nil {
		t.Fatalf("UpsertVectorStore() error = %v", err)
	}
	if storedVectorStore.ID == uuid.Nil || storedVectorStore.CreatedAt.IsZero() || storedVectorStore.UpdatedAt.IsZero() {
		t.Fatalf("UpsertVectorStore() returned non-canonical value: %+v", storedVectorStore)
	}
	stores, err := m.globalBase.ListVectorStores(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !hasVectorStoreKey(stores, vectorStore.Key) {
		t.Fatalf("vector stores=%#v want key %q", stores, vectorStore.Key)
	}
	secret, err := m.GlobalManager().UpsertSecret(ctx, domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerSystem, OwnerID: "system", Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: &domainsemantic.EncryptedSecretPayload{Algorithm: "AES-256-GCM", NonceB64: "nonce", CipherB64: "cipher"}, SecretSuffix: "test"})
	if err != nil {
		t.Fatalf("UpsertSecret() error = %v", err)
	}
	if secret.ID == uuid.Nil || secret.CreatedAt.IsZero() || secret.UpdatedAt.IsZero() {
		t.Fatalf("UpsertSecret() returned non-canonical value: %+v", secret)
	}
	credential, err := m.GlobalManager().UpsertCredential(ctx, domainsemantic.InferenceCredential{Key: "test-credential", ModelEndpointID: uuid.New(), OwnerType: domainsemantic.CredentialOwnerSystem, OwnerID: "system", AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secret.ID})
	if err != nil {
		t.Fatalf("UpsertCredential() error = %v", err)
	}
	if credential.ID == uuid.Nil || credential.SecretRef != secret.ID || credential.Status != domainsemantic.CredentialStatusActive || credential.CreatedAt.IsZero() || credential.UpdatedAt.IsZero() {
		t.Fatalf("UpsertCredential() returned non-canonical value: %+v", credential)
	}
}

func TestSemanticGlobalRaftFailsClosedWithoutSystemLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	m.EnableExperimentalRaft(groups, 4)
	rec := semanticMutationRecord{Kind: "vector_store.upsert", Payload: raw(semanticVectorStore("no-leader"))}
	if err := m.commitSemanticMutation(ctx, recordTypeSemanticGlobal, rec); status.Code(err) != codes.Unavailable {
		t.Fatalf("commitSemanticMutation() error = %v, want Unavailable", err)
	}
}

func TestSemanticRaftStateMachineAppliesMaintenanceMutation(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	event := semanticDirtyEvent(spaceID)
	rec := maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: spaceID, Payload: raw(event)}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, payload, "semantic-maintenance-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	mgr, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("events=%#v want %s", events, event.ID)
	}
}

func TestSemanticRaftStateMachineMissingWorkCompleteIsIdempotent(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	workID := uuid.New()
	rec := maintenanceMutationRecord{Kind: "work.complete", SpaceID: spaceID, Payload: raw(struct {
		ID     uuid.UUID                `json:"id"`
		Result storesemantic.WorkResult `json:"result"`
	}{ID: workID})}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, payload, "semantic-maintenance-missing-work-complete")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v, want nil", err)
	}
}

func TestSemanticRaftStateMachineMissingWorkFailIsIdempotent(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	workID := uuid.New()
	rec := maintenanceMutationRecord{Kind: "work.fail", SpaceID: spaceID, Payload: raw(struct {
		ID      uuid.UUID                 `json:"id"`
		Failure storesemantic.WorkFailure `json:"failure"`
	}{ID: workID, Failure: storesemantic.WorkFailure{Category: "stale", Message: "missing"}})}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, payload, "semantic-maintenance-missing-work-fail")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v, want nil", err)
	}
}

func TestSemanticMaintenanceManagerUsesRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	spaceID := domainspace.SpaceID(uuid.New())
	probe, _ := m.buildSemanticMaintenanceRaftCommand(maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: spaceID}, []byte(`{}`), "maintenance-probe")
	group, _ := groups.Group(consensus.PartitionGroupID(probe.PartitionID))
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	mgr, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	event := semanticDirtyEvent(spaceID)
	if _, err := mgr.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatalf("AppendGraphDirtyEvent() error = %v", err)
	}
	mgr, err = m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	events, err := mgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != event.ID {
		t.Fatalf("events=%#v want %s", events, event.ID)
	}
}

func TestExecuteLocalRaftSemanticReadSpaceAndMaintenance(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	idx := semanticIndex(spaceID)
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatal(err)
	}
	event := semanticDirtyEvent(spaceID)
	mm, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mm.AppendGraphDirtyEvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(raftSemanticReadRequest{Op: "list_indexes", SpaceID: spaceID})
	resPayload, err := m.ExecuteLocalRaftSemanticRead(ctx, spaceID.String(), payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftSemanticRead(list_indexes) error = %v", err)
	}
	var indexes raftSemanticIndexesResponse
	if err := json.Unmarshal(resPayload, &indexes); err != nil {
		t.Fatal(err)
	}
	if len(indexes.Indexes) != 1 || indexes.Indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v want %s", indexes.Indexes, idx.ID)
	}
	payload, _ = json.Marshal(raftSemanticReadRequest{Op: "list_dirty_events", SpaceID: spaceID})
	resPayload, err = m.ExecuteLocalRaftSemanticRead(ctx, spaceID.String(), payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftSemanticRead(list_dirty_events) error = %v", err)
	}
	var events raftSemanticDirtyEventsResponse
	if err := json.Unmarshal(resPayload, &events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Events[0].ID != event.ID {
		t.Fatalf("events=%#v want %s", events.Events, event.ID)
	}
}

func TestSemanticRaftDedupeSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	idx := semanticIndex(spaceID)
	rec := semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID, Payload: raw(idx)}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m1 := NewModule()
	if result := m1.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init first module failed: %v", result.Error)
	}
	m1.EnableExperimentalRaft(nil, 64)
	cmd, err := m1.buildSemanticSpaceRaftCommand(rec, payload, "semantic-durable-command")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m1, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	m2 := NewModule()
	if result := m2.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init second module failed: %v", result.Error)
	}
	m2.EnableExperimentalRaft(nil, 64)
	if err := (RaftStateMachine{Module: m2, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("duplicate ApplyCommand() after restart error = %v", err)
	}
	mgr, err := m2.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v want %s", indexes, idx.ID)
	}
}

func TestSemanticRaftRejectsSpaceMismatch(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	rec := semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID, Payload: raw(semanticIndex(spaceID))}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticSpaceRaftCommand(rec, payload, "semantic-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	rec.SpaceID = domainspace.SpaceID(uuid.New())
	cmd.Payload, err = json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err == nil {
		t.Fatal("ApplyCommand() error = nil, want space mismatch error")
	}
}

func TestSemanticRaftSpaceMutationReplicatesAcrossThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, mg := range groupsByNode {
					mg.Tick()
				}
			}
		}
	}()
	spaceID := domainspace.SpaceID(uuid.New())
	probe, err := modules[1].buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "probe")
	if err != nil {
		t.Fatal(err)
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, mg := range groupsByNode {
			mg.Tick()
		}
	}, func() bool {
		leaders := map[consensus.NodeID]int{}
		for _, mg := range groupsByNode {
			if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
				leaders[g.Leader()]++
			}
		}
		for _, count := range leaders {
			if count >= 2 {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	mgr, err := modules[1].SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	idx := semanticIndex(spaceID)
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	for nodeID, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			mgr, err := m.SpaceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			indexes, err := mgr.ListSemanticIndexes(ctx)
			return err == nil && len(indexes) == 1 && indexes[0].ID == idx.ID
		}); err != nil {
			t.Fatalf("node %d did not apply semantic index: %v", nodeID, err)
		}
	}
}

func TestSemanticRaftVectorRecordsForwardToPartitionLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	servers := map[consensus.NodeID]*grpc.Server{}
	addrs := make([]string, len(peers))
	for _, nodeID := range peers {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := grpc.NewServer()
		backend := clusterbackend.NewService(clustermodel.NodeIdentity{}, clustermodel.NodeStateClustered, nil)
		backend.SemanticReader = modules[nodeID]
		clusterpb.RegisterClusterBackendServiceServer(srv, backend)
		servers[nodeID] = srv
		addrs[int(nodeID)-1] = lis.Addr().String()
		go func() { _ = srv.Serve(lis) }()
	}
	defer func() {
		for _, srv := range servers {
			srv.Stop()
		}
	}()
	for _, nodeID := range peers {
		modules[nodeID].EnableExperimentalRaftNetworking(nodeID, addrs, "")
	}
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				for _, mg := range groupsByNode {
					mg.Tick()
				}
			}
		}
	}()
	spaceID := domainspace.SpaceID(uuid.New())
	probe, err := modules[1].buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "probe")
	if err != nil {
		t.Fatal(err)
	}
	leaderForPartition := func() consensus.NodeID {
		leaders := map[consensus.NodeID]int{}
		for _, mg := range groupsByNode {
			if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
				leaders[g.Leader()]++
			}
		}
		for leader, count := range leaders {
			if count >= 2 {
				return leader
			}
		}
		return 0
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, func() {
		for _, mg := range groupsByNode {
			mg.Tick()
		}
	}, func() bool { return leaderForPartition() != 0 }); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	leaderID := leaderForPartition()
	followerID := consensus.NodeID(1)
	if followerID == leaderID {
		followerID = 2
	}
	idx := semanticIndex(spaceID)
	mgr, err := modules[leaderID].SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	rec, err := modules[leaderID].localVectorBackend().Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: idx.DomainID, SemanticIndexID: idx.ID, NodeID: graphmodel.NodeID(uuid.New()), SourceHash: "sha256:test", SourceMode: "self", ModelEndpointID: idx.ModelEndpointID, ModelID: idx.ModelID, VectorStoreID: idx.VectorStoreID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatalf("leader vector upsert: %v", err)
	}
	local, err := modules[followerID].localVectorBackend().ListRecords(ctx, spaceID, idx.ID)
	if err != nil {
		t.Fatalf("follower local vector list: %v", err)
	}
	if len(local) != 0 {
		t.Fatalf("follower unexpectedly had local vector records: %#v", local)
	}
	forwarded, err := modules[followerID].ListVectorRecords(ctx, spaceID, idx.ID)
	if err != nil {
		t.Fatalf("follower forwarded vector list: %v", err)
	}
	if len(forwarded) != 1 || forwarded[0].ID != rec.ID {
		t.Fatalf("forwarded records=%#v want %s from leader %d via follower %d", forwarded, rec.ID, leaderID, followerID)
	}
}

func TestSemanticRaftSpaceMutationSurvivesLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	modules := map[consensus.NodeID]*Module{}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := NewModule()
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		for _, g := range mg.Groups() {
			for _, router := range routers {
				router.Register(g)
			}
		}
	}
	defer func() {
		for _, mg := range groupsByNode {
			mg.Stop()
		}
	}()
	active := map[consensus.NodeID]bool{1: true, 2: true, 3: true}
	tickActive := func() {
		for id, mg := range groupsByNode {
			if active[id] {
				mg.Tick()
			}
		}
	}
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				tickActive()
			}
		}
	}()
	spaceID := domainspace.SpaceID(uuid.New())
	probe, err := modules[1].buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "failover-probe")
	if err != nil {
		t.Fatal(err)
	}
	leaderForPartition := func() consensus.NodeID {
		counts := map[consensus.NodeID]int{}
		for id, mg := range groupsByNode {
			if !active[id] {
				continue
			}
			if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
				counts[g.Leader()]++
			}
		}
		for leader, count := range counts {
			if active[leader] && count >= 2 {
				return leader
			}
		}
		return 0
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, tickActive, func() bool { return leaderForPartition() != 0 }); err != nil {
		t.Fatalf("initial leader not elected: %v", err)
	}
	oldLeader := leaderForPartition()
	active[oldLeader] = false
	groupsByNode[oldLeader].Stop()
	for _, router := range routers {
		router.UnregisterNode(oldLeader)
	}
	if err := consensus.TickUntil(ctx, 20*time.Millisecond, tickActive, func() bool { l := leaderForPartition(); return l != 0 && l != oldLeader }); err != nil {
		t.Fatalf("new leader not elected after stopping %d: %v", oldLeader, err)
	}
	writer := consensus.NodeID(1)
	if writer == oldLeader {
		writer = 2
	}
	mgr, err := modules[writer].SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	idx := semanticIndex(spaceID)
	if _, err := mgr.UpsertSemanticIndex(ctx, idx); err != nil {
		t.Fatalf("UpsertSemanticIndex() after failover error = %v", err)
	}
	for nodeID, m := range modules {
		if !active[nodeID] {
			continue
		}
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			mgr, err := m.SpaceManager(ctx, spaceID)
			if err != nil {
				return false
			}
			indexes, err := mgr.ListSemanticIndexes(ctx)
			return err == nil && len(indexes) == 1 && indexes[0].ID == idx.ID
		}); err != nil {
			t.Fatalf("node %d did not apply post-failover semantic index: %v", nodeID, err)
		}
	}
}

func TestSemanticRaftStateMachineAppliesSpaceMutation(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	idx := semanticIndex(spaceID)
	rec := semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID, Payload: raw(idx)}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildSemanticSpaceRaftCommand(rec, payload, "semantic-1")
	if err != nil {
		t.Fatalf("buildSemanticSpaceRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].ID != idx.ID {
		t.Fatalf("indexes=%#v want %s", indexes, idx.ID)
	}
}

func TestSemanticSpaceManagerUsesRaftWhenEnabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	router := consensus.NewLocalMessageRouter()
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }}, ElectionTick: 5, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	spaceID := domainspace.SpaceID(uuid.New())
	probe, _ := m.buildSemanticSpaceRaftCommand(semanticMutationRecord{Kind: "semantic_index.upsert", SpaceID: spaceID}, []byte(`{}`), "probe")
	group, _ := groups.Group(consensus.PartitionGroupID(probe.PartitionID))
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	idx := semanticIndex(spaceID)
	idx.ID = uuid.Nil
	storedIndex, err := mgr.UpsertSemanticIndex(ctx, idx)
	if err != nil {
		t.Fatalf("UpsertSemanticIndex() error = %v", err)
	}
	if storedIndex.ID == uuid.Nil || storedIndex.CreatedAt.IsZero() || storedIndex.UpdatedAt.IsZero() {
		t.Fatalf("UpsertSemanticIndex() returned non-canonical value: %+v", storedIndex)
	}
	mgr, err = m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatal(err)
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0].ID != storedIndex.ID {
		t.Fatalf("indexes=%#v want %s", indexes, storedIndex.ID)
	}
}

func semanticIndex(spaceID domainspace.SpaceID) domainsemantic.SemanticIndex {
	now := time.Now().UTC()
	return domainsemantic.SemanticIndex{ID: domainsemantic.SemanticIndexID(uuid.New()), SpaceID: spaceID, DomainID: graphmodel.DomainID(uuid.New()), Key: "idx", Name: "Index", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true, CreatedAt: now, UpdatedAt: now}
}

func semanticDirtyEvent(spaceID domainspace.SpaceID) domainsemantic.GraphDirtyEvent {
	return domainsemantic.GraphDirtyEvent{ID: domainsemantic.GraphDirtyEventID(uuid.New()), TxnID: uuid.New(), GraphRevision: 1, SpaceID: spaceID, DomainIDs: []graphmodel.DomainID{graphmodel.DomainID(uuid.New())}, CommittedAt: time.Now().UTC()}
}

func semanticVectorStore(key string) domainsemantic.VectorStoreBackend {
	now := time.Now().UTC()
	return domainsemantic.VectorStoreBackend{ID: domainsemantic.VectorStoreID(uuid.New()), Key: key, Name: key, Type: domainsemantic.VectorStoreMycelFile, Config: map[string]any{}, PrivacyClass: domainsemantic.PrivacyClassLocalOnly, Enabled: true, CreatedAt: now, UpdatedAt: now}
}

func hasVectorStoreKey(stores []domainsemantic.VectorStoreBackend, key string) bool {
	for _, store := range stores {
		if store.Key == key {
			return true
		}
	}
	return false
}
