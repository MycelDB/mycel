package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
)

func TestBlobRaftDedupeSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := uuid.NewString()
	m1 := NewModule(nil)
	if result := m1.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init first module failed: %v", result.Error)
	}
	store, err := m1.store(spaceID)
	if err != nil {
		t.Fatal(err)
	}
	id, size, err := store.Put(ctx, bytes.NewReader([]byte("dedupe")))
	if err != nil {
		t.Fatal(err)
	}
	meta := BlobMeta{BlobID: string(id), SpaceID: spaceID, Digest: "sha256:" + string(id), SizeBytes: size, MimeType: "text/plain"}
	payload, err := jsonMarshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: descriptorFromMeta(meta)})
	if err != nil {
		t.Fatal(err)
	}
	m1.EnableExperimentalRaft(nil, 64)
	cmd, err := m1.buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, payload, "durable-blob-command")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m1, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	m2 := NewModule(nil)
	if result := m2.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init second module failed: %v", result.Error)
	}
	m2.EnableExperimentalRaft(nil, 64)
	if err := (RaftStateMachine{Module: m2, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("duplicate ApplyCommand() after restart error = %v", err)
	}
	if _, err := m2.meta(spaceID, meta.BlobID); err != nil {
		t.Fatalf("meta() error = %v", err)
	}
}

func TestBlobRaftStateMachineAppliesMetaPut(t *testing.T) {
	ctx := context.Background()
	m := NewModule(nil)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	store, err := m.store(spaceID)
	if err != nil {
		t.Fatal(err)
	}
	id, size, err := store.Put(ctx, bytes.NewReader([]byte("test")))
	if err != nil {
		t.Fatal(err)
	}
	blobID := string(id)
	meta := BlobMeta{BlobID: blobID, SpaceID: spaceID, Digest: "sha256:" + blobID, SizeBytes: size, MimeType: "text/plain"}
	payload, err := jsonMarshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: descriptorFromMeta(meta)})
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, payload, "blob-put-1")
	if err != nil {
		t.Fatalf("buildBlobRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	got, err := m.meta(spaceID, meta.BlobID)
	if err != nil {
		t.Fatalf("meta() error = %v", err)
	}
	if got.BlobID != meta.BlobID {
		t.Fatalf("BlobID=%q want %q", got.BlobID, meta.BlobID)
	}
}

func TestBlobRaftMetadataReplicatesAndFetchesPayloadAcrossThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	clusterID := "cluster-blob-raft"
	modules := map[consensus.NodeID]*Module{}
	addrs := make([]string, len(peers))
	servers := []*grpc.Server{}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()
	for _, nodeID := range peers {
		m := NewModule(nil)
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[int(nodeID)-1] = lis.Addr().String()
		grpcServer := grpc.NewServer()
		servers = append(servers, grpcServer)
		identity := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: fmt.Sprintf("node-%d", nodeID), ClusterID: clusterID, ClusterName: "test", ClusterAdmitted: true}
		svc := clusterbackend.NewService(identity, model.NodeStateClustered, nil).WithBlobPayloadProvider(BackendPayloadProvider{Module: m})
		clusterpb.RegisterClusterBackendServiceServer(grpcServer, svc)
		go func() { _ = grpcServer.Serve(lis) }()
	}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := modules[nodeID]
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		m.EnableExperimentalRaftNetworking(nodeID, addrs, "", clusterID)
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
	probe, err := modules[1].buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, []byte(`{}`), "probe")
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
	meta, err := modules[1].UploadBlob(ctx, UploadInput{SpaceID: spaceID, Reader: bytes.NewReader([]byte("replicated payload"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	for nodeID, m := range modules {
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			_, r, err := m.OpenBlob(ctx, spaceID, meta.BlobID)
			if err != nil {
				return false
			}
			defer r.Close()
			data, _ := io.ReadAll(r)
			return string(data) == "replicated payload"
		}); err != nil {
			t.Fatalf("node %d did not materialize replicated blob: %v", nodeID, err)
		}
	}
}

func TestBlobRaftUploadSurvivesLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peers := []consensus.NodeID{1, 2, 3}
	partitionCount := uint32(4)
	clusterID := "cluster-blob-failover"
	modules := map[consensus.NodeID]*Module{}
	addrs := make([]string, len(peers))
	servers := []*grpc.Server{}
	defer func() {
		for _, s := range servers {
			s.Stop()
		}
	}()
	for _, nodeID := range peers {
		m := NewModule(nil)
		if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
			t.Fatalf("init node %d failed: %v", nodeID, result.Error)
		}
		modules[nodeID] = m
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[int(nodeID)-1] = lis.Addr().String()
		grpcServer := grpc.NewServer()
		servers = append(servers, grpcServer)
		identity := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: fmt.Sprintf("node-%d", nodeID), ClusterID: clusterID, ClusterName: "test", ClusterAdmitted: true}
		svc := clusterbackend.NewService(identity, model.NodeStateClustered, nil).WithBlobPayloadProvider(BackendPayloadProvider{Module: m})
		clusterpb.RegisterClusterBackendServiceServer(grpcServer, svc)
		go func() { _ = grpcServer.Serve(lis) }()
	}
	routers := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { r, ok := routers[nodeID]; return r, ok })}
	groupsByNode := map[consensus.NodeID]*consensus.MultiGroup{}
	for _, nodeID := range peers {
		m := modules[nodeID]
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: nodeID, PeerNodeIDs: peers, PartitionCount: partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return RaftStateMachine{Module: m, PartitionCount: partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", nodeID, err)
		}
		groupsByNode[nodeID] = mg
		m.EnableExperimentalRaft(mg, partitionCount)
		m.EnableExperimentalRaftNetworking(nodeID, addrs, "", clusterID)
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
	probe, err := modules[1].buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, []byte(`{}`), "failover-probe")
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
	meta, err := modules[writer].UploadBlob(ctx, UploadInput{SpaceID: spaceID, Reader: bytes.NewReader([]byte("after blob failover"))})
	if err != nil {
		t.Fatalf("UploadBlob() after failover error = %v", err)
	}
	for nodeID, m := range modules {
		if !active[nodeID] {
			continue
		}
		if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
			_, r, err := m.OpenBlob(ctx, spaceID, meta.BlobID)
			if err != nil {
				return false
			}
			defer r.Close()
			data, _ := io.ReadAll(r)
			return string(data) == "after blob failover"
		}); err != nil {
			t.Fatalf("node %d did not materialize post-failover blob: %v", nodeID, err)
		}
	}
}

func TestBlobRaftStateMachineFetchesMissingPayloadFromPeer(t *testing.T) {
	ctx := context.Background()
	spaceID := uuid.NewString()
	source := NewModule(nil)
	if result := source.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init source failed: %v", result.Error)
	}
	meta, err := source.UploadBlob(ctx, UploadInput{SpaceID: spaceID, Reader: bytes.NewReader([]byte("remote payload"))})
	if err != nil {
		t.Fatalf("source UploadBlob() error = %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	identity := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node-1", ClusterID: "cluster-1", ClusterName: "test", ClusterAdmitted: true}
	svc := clusterbackend.NewService(identity, model.NodeStateClustered, nil).WithBlobPayloadProvider(BackendPayloadProvider{Module: source})
	clusterpb.RegisterClusterBackendServiceServer(grpcServer, svc)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()
	target := NewModule(nil)
	if result := target.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init target failed: %v", result.Error)
	}
	target.EnableExperimentalRaft(nil, 64)
	target.EnableExperimentalRaftNetworking(2, []string{lis.Addr().String(), ""}, "", "cluster-1")
	payload, err := jsonMarshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: descriptorFromMeta(meta)})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := target.buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, payload, "blob-fetch-1")
	if err != nil {
		t.Fatalf("buildBlobRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: target, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	_, r, err := target.OpenBlob(ctx, spaceID, meta.BlobID)
	if err != nil {
		t.Fatalf("target OpenBlob() error = %v", err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "remote payload" {
		t.Fatalf("payload=%q want remote payload", data)
	}
}

func TestBlobRaftStateMachineRejectsMetadataWithoutPayload(t *testing.T) {
	ctx := context.Background()
	m := NewModule(nil)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := uuid.NewString()
	blobID := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	meta := BlobMeta{BlobID: blobID, SpaceID: spaceID, Digest: "sha256:" + blobID, SizeBytes: 4, MimeType: "text/plain"}
	payload, err := jsonMarshal(blobMetaPutRecord{Meta: meta, PayloadDescriptor: descriptorFromMeta(meta)})
	if err != nil {
		t.Fatal(err)
	}
	m.EnableExperimentalRaft(nil, 64)
	cmd, err := m.buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, payload, "blob-put-missing")
	if err != nil {
		t.Fatalf("buildBlobRaftCommand() error = %v", err)
	}
	if err := (RaftStateMachine{Module: m, PartitionCount: 64}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err == nil {
		t.Fatal("ApplyCommand() error = nil, want missing payload error")
	}
}

func TestUploadBlobUsesRaftMetadataWhenEnabled(t *testing.T) {
	ctx := context.Background()
	m := NewModule(nil)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
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
	spaceID := uuid.NewString()
	cmd, _ := m.buildBlobRaftCommand(spaceID, recordTypeBlobMetaPut, []byte(`{}`), "probe")
	group, _ := groups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if err := consensus.TickUntil(ctx, 10*time.Millisecond, groups.Tick, func() bool { return group.Leader() == 1 }); err != nil {
		t.Fatal(err)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: spaceID, Reader: bytes.NewReader([]byte("hello"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	_, r, err := m.OpenBlob(ctx, spaceID, meta.BlobID)
	if err != nil {
		t.Fatalf("OpenBlob() error = %v", err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "hello" {
		t.Fatalf("payload=%q want hello", data)
	}
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }
