package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type denyingLocalWriteHost struct{ daemonruntime.Host }

type fakeBlobReferenceChecker struct{ err error }

func (f fakeBlobReferenceChecker) EnsureBlobReference(ctx context.Context, spaceID string, blobID string) error {
	return f.err
}

func (h *denyingLocalWriteHost) RequireLocalWriteAllowed() error {
	return status.Error(codes.Unavailable, "local writes denied")
}

func TestGraphCommitRejectsUnavailableBlobReference(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	m.SetBlobReferenceChecker(fakeBlobReferenceChecker{err: status.Error(codes.NotFound, "blob missing")})
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	if _, err := m.CreateNode(ctx, tx, NodeInput{BlobID: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CommitTransactionGraph() error = %v, want FailedPrecondition", err)
	}
	if m.overlays[tx.ID] == nil {
		t.Fatal("failed commit should preserve transaction overlay")
	}
	if rev, err := m.CurrentRevision(ctx, spaceID); err != nil || rev != 0 {
		t.Fatalf("CurrentRevision() = %d, %v; want 0, nil", rev, err)
	}
}

func TestGraphRaftCommandIDUsesIdempotencyMetadata(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("idempotency-key", "commit-123"))
	if got := graphRaftCommandID(ctx, "tx-123"); got != "graph-commit-idempotency-commit-123" {
		t.Fatalf("graphRaftCommandID() = %q", got)
	}
	if got := graphRaftCommandID(context.Background(), "tx-123"); got != "graph-commit-tx-123" {
		t.Fatalf("fallback graphRaftCommandID() = %q", got)
	}
}

func TestBuildGraphCommitRaftCommand(t *testing.T) {
	m := NewModule()
	spaceID := uuid.NewString()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: uuid.New(), DomainID: uuid.New(), Content: "hello"}}, OperationCount: 1}
	cmd, err := m.buildGraphCommitRaftCommand(record, 64, "graph-commit-1")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	if cmd.RecordType != recordTypeGraphCommit || cmd.SpaceID != spaceID || cmd.CommandID != "graph-commit-1" {
		t.Fatalf("unexpected command: %+v", cmd)
	}
	if err := cmd.Validate(64); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCommitTransactionGraphUsesRaftWhenEnabled(t *testing.T) {
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
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	spaceID := uuid.NewString()
	cmd, err := m.buildGraphCommitRaftCommand(graphCommitRecord{SpaceID: spaceID}, 4, "leader-check")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	group, ok := groups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok {
		t.Fatalf("partition group not found")
	}
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	m.EnableExperimentalRaft(groups, 4)
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	node, err := m.CreateNode(ctx, tx, NodeInput{Content: "raft node", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	commit, err := m.CommitTransactionGraph(ctx, tx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	if commit.OperationCount != 1 || commit.CommittedRevision != 1 {
		t.Fatalf("commit=%#v", commit)
	}
	readTx := graphTx(spaceID, domainID, commit.CommittedRevision)
	readTx.Mode = daemonsession.TransactionModeReadOnly
	got, err := m.GetNode(ctx, readTx, node.ID.String())
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if got.Content != "raft node" {
		t.Fatalf("node content=%q want raft node", got.Content)
	}
	readDiag := group.ReadDiagnostics()
	if readDiag.ReadIndexAttempts < 1 || readDiag.ReadIndexSuccesses < 1 || readDiag.ReadIndexFailures != 0 || readDiag.LastReadIndex == 0 || readDiag.LastAppliedWaitSuccess != readDiag.LastReadIndex {
		t.Fatalf("ReadDiagnostics()=%#v", readDiag)
	}
	req := raftReadRequest("get_node", readTx)
	req.ID = node.ID.String()
	payload, _ := json.Marshal(req)
	resPayload, err := m.ExecuteLocalRaftGraphRead(ctx, spaceID, payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftGraphRead(get_node) error = %v", err)
	}
	var readRes raftGraphNodeResponse
	if err := json.Unmarshal(resPayload, &readRes); err != nil {
		t.Fatal(err)
	}
	if readRes.Read == nil || !readRes.Read.Strong || readRes.Read.ReadIndex == 0 || readRes.Read.LeaderNodeID != 1 {
		t.Fatalf("forwarded read metadata=%#v", readRes.Read)
	}
}

func TestReadOnlyTransactionUsesLinearizableCurrentReadsNotHistoricalSnapshot(t *testing.T) {
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
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	spaceID := uuid.NewString()
	probe, err := m.buildGraphCommitRaftCommand(graphCommitRecord{SpaceID: spaceID}, 4, "f3-leader-check")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	group, ok := groups.Group(consensus.PartitionGroupID(probe.PartitionID))
	if !ok {
		t.Fatalf("partition group not found")
	}
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatalf("leader not elected: %v", err)
	}
	m.EnableExperimentalRaft(groups, 4)
	domainID := uuid.NewString()
	firstTx := graphTx(spaceID, domainID, 0)
	first, err := m.CreateNode(ctx, firstTx, NodeInput{Content: "revision 1", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode(first) error = %v", err)
	}
	firstCommit, err := m.CommitTransactionGraph(ctx, firstTx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph(first) error = %v", err)
	}
	if firstCommit.CommittedRevision != 1 {
		t.Fatalf("first committed revision=%d want 1", firstCommit.CommittedRevision)
	}

	readOnly := graphTx(spaceID, domainID, firstCommit.CommittedRevision)
	readOnly.Mode = daemonsession.TransactionModeReadOnly
	if _, err := m.GetNode(ctx, readOnly, first.ID.String()); err != nil {
		t.Fatalf("GetNode(first through read-only tx) error = %v", err)
	}

	secondTx := graphTx(spaceID, domainID, firstCommit.CommittedRevision)
	second, err := m.CreateNode(ctx, secondTx, NodeInput{Content: "revision 2", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode(second) error = %v", err)
	}
	secondCommit, err := m.CommitTransactionGraph(ctx, secondTx)
	if err != nil {
		t.Fatalf("CommitTransactionGraph(second) error = %v", err)
	}
	if secondCommit.CommittedRevision != 2 {
		t.Fatalf("second committed revision=%d want 2", secondCommit.CommittedRevision)
	}

	got, err := m.GetNode(ctx, readOnly, second.ID.String())
	if err != nil {
		t.Fatalf("read-only transaction should be a current-read context and see newer commits; GetNode(second) error = %v", err)
	}
	if got.Content != "revision 2" {
		t.Fatalf("read-only current read content=%q want revision 2", got.Content)
	}
	futureBase := readOnly
	futureBase.ID = uuid.NewString()
	futureBase.BaseRevision = secondCommit.CommittedRevision + 1
	if _, err := m.GetNode(ctx, futureBase, first.ID.String()); status.Code(err) != codes.Unavailable {
		t.Fatalf("future-base read-only transaction code=%v err=%v; want Unavailable", status.Code(err), err)
	}
	readDiag := group.ReadDiagnostics()
	if readDiag.ReadIndexAttempts < 3 || readDiag.ReadIndexSuccesses < 3 || readDiag.ReadIndexFailures != 0 {
		t.Fatalf("ReadDiagnostics()=%#v", readDiag)
	}
}

func TestExecuteLocalRaftGraphReadChildrenAndParent(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	parent, err := m.CreateNode(ctx, tx, NodeInput{Content: "parent", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode(parent) error = %v", err)
	}
	child, err := m.CreateNode(ctx, tx, NodeInput{Content: "child", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode(child) error = %v", err)
	}
	blobID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := m.CreateNode(ctx, tx, NodeInput{BlobID: blobID}); err != nil {
		t.Fatalf("CreateNode(blob) error = %v", err)
	}
	edge, err := m.CreateEdge(ctx, tx, EdgeInput{FromNodeID: parent.ID.String(), ToNodeID: child.ID.String(), Labels: []string{"contains"}, Properties: map[string]any{"order": 1000}})
	if err != nil {
		t.Fatalf("CreateEdge() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	readTx := graphTx(spaceID, domainID, 1)
	req := raftReadRequest("list_children", readTx)
	req.ID = parent.ID.String()
	mismatchReq := req
	mismatchReq.SpaceID = uuid.NewString()
	mismatchPayload, _ := json.Marshal(mismatchReq)
	if _, err := m.ExecuteLocalRaftGraphRead(ctx, spaceID, mismatchPayload); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ExecuteLocalRaftGraphRead(space mismatch) code = %v, err=%v; want InvalidArgument", status.Code(err), err)
	}
	payload, _ := json.Marshal(req)
	resPayload, err := m.ExecuteLocalRaftGraphRead(ctx, spaceID, payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftGraphRead(list_children) error = %v", err)
	}
	var children raftGraphEdgesResponse
	if err := json.Unmarshal(resPayload, &children); err != nil {
		t.Fatal(err)
	}
	if len(children.Edges) != 1 || children.Edges[0].ID != edge.ID {
		t.Fatalf("children=%#v want edge %s", children.Edges, edge.ID)
	}
	req = raftReadRequest("get_parent", readTx)
	req.ID = child.ID.String()
	payload, _ = json.Marshal(req)
	resPayload, err = m.ExecuteLocalRaftGraphRead(ctx, spaceID, payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftGraphRead(get_parent) error = %v", err)
	}
	var parentRes raftGraphOptionalEdgeResponse
	if err := json.Unmarshal(resPayload, &parentRes); err != nil {
		t.Fatal(err)
	}
	if parentRes.Edge == nil || parentRes.Edge.ID != edge.ID {
		t.Fatalf("parent edge=%#v want %s", parentRes.Edge, edge.ID)
	}
	req = raftGraphReadRequest{Op: "blob_ref_count", SpaceID: spaceID, ID: blobID}
	payload, _ = json.Marshal(req)
	resPayload, err = m.ExecuteLocalRaftGraphRead(ctx, spaceID, payload)
	if err != nil {
		t.Fatalf("ExecuteLocalRaftGraphRead(blob_ref_count) error = %v", err)
	}
	var countRes raftGraphCountResponse
	if err := json.Unmarshal(resPayload, &countRes); err != nil {
		t.Fatal(err)
	}
	if countRes.Count != 1 {
		t.Fatalf("blob ref count=%d want 1", countRes.Count)
	}
}

func TestGraphRaftDedupeSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := uuid.NewString()
	domainID := uuid.New()
	nodeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Content: "durable"}}, OperationCount: 1}
	m1 := NewModule()
	if result := m1.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init first module failed: %v", result.Error)
	}
	cmd, err := m1.buildGraphCommitRaftCommand(record, 64, "durable-command")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m1, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	m2 := NewModule()
	if result := m2.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init second module failed: %v", result.Error)
	}
	if err := (RaftStateMachine{Module: m2, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("duplicate ApplyCommand() after restart error = %v", err)
	}
	if rev, err := m2.CurrentRevision(ctx, spaceID); err != nil || rev != 1 {
		t.Fatalf("CurrentRevision() = %d, %v; want 1", rev, err)
	}
}

func TestGraphRaftStateMachineRejectsSpaceMismatch(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	record := graphCommitRecord{SpaceID: uuid.NewString(), BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: uuid.New(), DomainID: uuid.New(), Content: "bad"}}, OperationCount: 1}
	cmd, err := m.buildGraphCommitRaftCommand(record, 64, "mismatch")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	record.SpaceID = uuid.NewString()
	cmd.Payload, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err == nil {
		t.Fatal("ApplyCommand() error = nil, want space mismatch error")
	}
}

func TestGraphRaftStateMachineDedupesCommandID(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.New()
	nodeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Content: "deduped"}}, OperationCount: 1}
	cmd, err := m.buildGraphCommitRaftCommand(record, 64, "same-command")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	sm := RaftStateMachine{Module: m, PartitionCount: 64}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	if err := sm.ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("duplicate ApplyCommand() error = %v", err)
	}
	if rev, err := m.CurrentRevision(ctx, spaceID); err != nil || rev != 1 {
		t.Fatalf("CurrentRevision() = %d, %v; want 1", rev, err)
	}
}

func TestGraphMutationsFailClosedWhenLocalWritesRejected(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	host := &denyingLocalWriteHost{Host: daemonruntime.Host{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}}
	if result := m.Init(ctx, host); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	tx := graphTx(uuid.NewString(), uuid.NewString(), 0)
	if _, err := m.CreateNode(ctx, tx, NodeInput{Content: "blocked", Props: map[string]any{}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateNode() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestGraphRaftOperationsFailClosedWithoutLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	seedTx := graphTx(spaceID, domainID, 0)
	seed, err := m.CreateNode(ctx, seedTx, NodeInput{Content: "stale-local", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("seed CreateNode() error = %v", err)
	}
	if _, err := m.CommitTransactionGraph(ctx, seedTx); err != nil {
		t.Fatalf("seed CommitTransactionGraph() error = %v", err)
	}
	router := consensus.NewLocalMessageRouter()
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine { return RaftStateMachine{Module: m, PartitionCount: 4} }}, ElectionTick: 10, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	for _, g := range groups.Groups() {
		router.Register(g)
	}
	m.EnableExperimentalRaft(groups, 4)
	readTx := graphTx(spaceID, domainID, 1)
	if _, err := m.GetNode(ctx, readTx, seed.ID.String()); status.Code(err) != codes.Unavailable {
		t.Fatalf("GetNode() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
	if _, err := m.CurrentRevision(ctx, spaceID); status.Code(err) != codes.Unavailable {
		t.Fatalf("CurrentRevision() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
	if _, err := m.CreateEdge(ctx, readTx, EdgeInput{FromNodeID: seed.ID.String(), ToNodeID: uuid.NewString(), Labels: []string{"contains"}}); status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateEdge() code = %v, err=%v; want Unavailable", status.Code(err), err)
	}
}

func TestGraphRaftCommitReplicatesAcrossThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	modules := map[consensus.NodeID]*Module{}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	partitionCount := uint32(4)
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
	spaceID := uuid.NewString()
	probe, err := modules[1].buildGraphCommitRaftCommand(graphCommitRecord{SpaceID: spaceID}, partitionCount, "probe")
	if err != nil {
		t.Fatalf("build probe command: %v", err)
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
	writer := consensus.NodeID(0)
	for _, mg := range groupsByNode {
		if g, ok := mg.Group(consensus.PartitionGroupID(probe.PartitionID)); ok && g.Leader() != 0 {
			writer = g.Leader()
			break
		}
	}
	if writer == 0 {
		t.Fatal("partition leader not found")
	}
	nonWriter := consensus.NodeID(0)
	for _, peer := range peers {
		if peer != writer {
			nonWriter = peer
			break
		}
	}
	if nonWriter == 0 {
		t.Fatal("non-leader node not found")
	}
	domainID := uuid.NewString()
	nonLeaderTx := graphTx(spaceID, domainID, 0)
	nonLeaderNode := domaingraph.Node{ID: uuid.New(), DomainID: uuid.MustParse(domainID), Content: "unsafe", Props: map[string]any{}}
	modules[nonWriter].mu.Lock()
	modules[nonWriter].overlay(nonLeaderTx.ID).putNodes[nonLeaderNode.ID] = nonLeaderNode
	modules[nonWriter].overlays[nonLeaderTx.ID].opCount++
	modules[nonWriter].mu.Unlock()
	if _, _, err := modules[nonWriter].ListNodes(ctx, nonLeaderTx, 0, ""); status.Code(err) != codes.Unavailable {
		t.Fatalf("non-leader read-write ListNodes() code=%v want Unavailable (err=%v)", status.Code(err), err)
	}
	if _, err := modules[nonWriter].CommitTransactionGraph(ctx, nonLeaderTx); status.Code(err) != codes.Unavailable {
		t.Fatalf("non-leader CommitTransactionGraph() code=%v want Unavailable (err=%v)", status.Code(err), err)
	}
	if modules[nonWriter].overlays[nonLeaderTx.ID] == nil {
		t.Fatal("non-leader commit should fail before deleting transaction overlay")
	}
	tx := graphTx(spaceID, domainID, 0)
	node, err := modules[writer].CreateNode(ctx, tx, NodeInput{Content: "replicated", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := modules[writer].CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	readTx := graphTx(spaceID, domainID, 1)
	for id, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			got, err := m.node(ctx, readTx, node.ID)
			return err == nil && got.Content == "replicated"
		}); err != nil {
			t.Fatalf("node %d did not apply replicated graph commit: %v", id, err)
		}
	}
}

func TestGraphRaftCommitSurvivesLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	modules := map[consensus.NodeID]*Module{}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	partitionCount := uint32(4)
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
	spaceID := uuid.NewString()
	probe, err := modules[1].buildGraphCommitRaftCommand(graphCommitRecord{SpaceID: spaceID}, partitionCount, "failover-probe")
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
	writer := leaderForPartition()
	if writer == 0 {
		t.Fatal("new partition leader not found")
	}
	domainID := uuid.NewString()
	tx := graphTx(spaceID, domainID, 0)
	node, err := modules[writer].CreateNode(ctx, tx, NodeInput{Content: "after failover", Props: map[string]any{}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := modules[writer].CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() after failover error = %v", err)
	}
	readTx := graphTx(spaceID, domainID, 1)
	for id, m := range modules {
		if !active[id] {
			continue
		}
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			got, err := m.node(ctx, readTx, node.ID)
			return err == nil && got.Content == "after failover"
		}); err != nil {
			t.Fatalf("node %d did not apply post-failover commit: %v", id, err)
		}
	}
}

func TestGraphRaftStateMachineAppliesCommit(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.New()
	nodeID := uuid.New()
	record := graphCommitRecord{SpaceID: spaceID, BaseRevision: 0, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Content: "hello"}}, OperationCount: 1}
	cmd, err := m.buildGraphCommitRaftCommand(record, 64, "graph-commit-1")
	if err != nil {
		t.Fatalf("buildGraphCommitRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	tx := daemonsession.GraphTransaction{ID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID.String(), Mode: daemonsession.TransactionModeReadOnly, State: daemonsession.TransactionStateActive, BaseRevision: 1}
	got, err := m.GetNode(ctx, tx, nodeID.String())
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if got.Content != "hello" {
		t.Fatalf("node content=%q want hello", got.Content)
	}
}
