package clustering

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/registration"
	"github.com/myceldb/mycel/internal/clustering/topology"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

type Manager struct {
	mu        sync.RWMutex
	identity  model.NodeIdentity
	state     model.NodeState
	readiness ClusterReadiness

	topology       *topology.Registry
	membership     *membership.FileStore
	registration   *registration.Handler
	backend        *backend.Service
	logger         *slog.Logger
	dataDir        string
	raftMode       bool
	systemMetadata consensus.SystemMetadata
}

func NewManager(ctx context.Context, opts Options, logger *slog.Logger) (*Manager, error) {
	local, err := LoadOrCreate(ctx, opts)
	if err != nil {
		return nil, err
	}
	self := selfPeer(local.Identity)
	store := topology.NewFileStore(topology.PeersPath(opts.DataDir))
	registry, err := topology.NewRegistry(ctx, store, self)
	if err != nil {
		return nil, err
	}
	membershipStore := membership.NewFileStore(membership.Path(opts.DataDir), local.Identity.ClusterID, local.Identity.ClusterName)
	if local.Identity.ClusterAdmitted && local.Identity.ClusterBootstrap {
		now := time.Now().UTC()
		joined := now
		if err := membershipStore.UpsertMember(ctx, membership.Member{NodeName: local.Identity.NodeName, NodeID: local.Identity.NodeID, State: membership.MemberStateActive, BackendAdvertiseAddr: local.Identity.BackendAdvertiseAddr, Role: "member", ClusterBootstrap: true, NodePublicKeyFingerprint: local.Identity.NodePublicKeyFingerprint, CreatedAt: local.Identity.CreatedAt, UpdatedAt: now, JoinedAt: &joined}); err != nil {
			return nil, err
		}
	}
	readiness := initialReadiness(local.Identity, local.State, opts)
	m := &Manager{identity: local.Identity, state: local.State, readiness: readiness, topology: registry, membership: membershipStore, logger: logger, dataDir: opts.DataDir, raftMode: opts.RaftMode}
	m.backend = backend.NewService(m.identity, m.state, registry).WithMembership(membershipStore)
	m.registration = &registration.Handler{Topology: registry, Client: registration.BackendAdapter{Client: backend.Client{AuthToken: opts.BackendAuthToken}}, Identity: m.identity, State: m.state, Interval: 5 * time.Second, Timeout: 2 * time.Second, Logger: logger}
	return m, nil
}

func (m *Manager) Start(ctx context.Context) error {
	if m == nil || m.registration == nil {
		return nil
	}
	go func() { _ = m.registration.Run(ctx) }()
	return nil
}

func (m *Manager) Stop(ctx context.Context) error {
	if m == nil {
		return nil
	}
	return WriteLocalState(m.dataDir, model.NodeStateStopped, time.Now().UTC())
}

func (m *Manager) SetActivityEmitter(emitter backend.ActivityEmitter) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.WithActivityEmitter(emitter)
}

func (m *Manager) Identity() model.NodeIdentity {
	if m == nil {
		return model.NodeIdentity{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identity
}
func (m *Manager) State() model.NodeState {
	if m == nil {
		return ""
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}
func (m *Manager) Topology() *topology.Registry {
	if m == nil {
		return nil
	}
	return m.topology
}

func (m *Manager) Membership() *membership.FileStore {
	if m == nil {
		return nil
	}
	return m.membership
}

func (m *Manager) SystemMetadata() consensus.SystemMetadata {
	if m == nil {
		return consensus.SystemMetadata{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return cloneSystemMetadata(m.systemMetadata)
}

func (m *Manager) IsAdmitted() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identity.ClusterAdmitted
}

func (m *Manager) IsBootstrap() bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.identity.ClusterBootstrap
}

func (m *Manager) BackendService() clusterpb.ClusterBackendServiceServer {
	if m == nil {
		return nil
	}
	return m.backend
}

func (m *Manager) SetBackendRaftRouter(router consensus.MessageSender) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.WithRaftRouter(router)
}

func (m *Manager) SetBackendSpaceReader(reader backend.SpaceReader) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.WithSpaceReader(reader)
}

func (m *Manager) SetBackendGraphReader(reader any) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.GraphReader = reader
}

func (m *Manager) SetBackendSemanticReader(reader any) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.SemanticReader = reader
}

func (m *Manager) SetBackendAutomationRuntimeReader(reader any) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.AutomationRuntimeReader = reader
}

func (m *Manager) SetBackendBlobPayloadProvider(provider backend.BlobPayloadProvider) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.WithBlobPayloadProvider(provider)
}

func (m *Manager) SetBackendClusterBackupProvider(provider backend.ClusterBackupProvider) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.WithClusterBackupProvider(provider)
}

func (m *Manager) SetBackendClientRequestForwarder(handler backend.ForwardedClientRequestHandler) {
	if m == nil || m.backend == nil {
		return
	}
	m.backend.WithClientRequestForwarder(handler)
}

func (m *Manager) Registration() *registration.Handler {
	if m == nil {
		return nil
	}
	return m.registration
}

func (m *Manager) Readiness() ClusterReadiness {
	if m == nil {
		return ClusterReadiness{ReadinessBlockers: []string{"clustering manager is not available"}}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := m.readiness
	out.ReadinessBlockers = append([]string(nil), out.ReadinessBlockers...)
	return out
}

func (m *Manager) SetReadinessBlocker(blocker string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readiness = m.readiness.withBlocker(blocker)
	m.state = NodeStateInitializing
	if m.backend != nil {
		m.backend.State = m.state
	}
	if m.registration != nil {
		m.registration.State = m.state
	}
}

func (m *Manager) ApplySystemMetadata(ctx context.Context, meta consensus.SystemMetadata, raftNodeID consensus.NodeID) error {
	if m == nil {
		return fmt.Errorf("clustering manager is required")
	}
	if strings.TrimSpace(meta.ClusterID) == "" {
		return fmt.Errorf("system metadata cluster_id is required")
	}
	node, ok := systemNodeForRaftNode(meta, raftNodeID)
	if !ok {
		return fmt.Errorf("system metadata has no node for raft node id %d", raftNodeID)
	}
	m.mu.RLock()
	id := m.identity
	m.mu.RUnlock()
	if strings.TrimSpace(id.ClusterID) != "" && id.ClusterID != meta.ClusterID {
		return fmt.Errorf("local cluster_id %s conflicts with system metadata cluster_id %s", id.ClusterID, meta.ClusterID)
	}
	if strings.TrimSpace(id.NodeID) != "" && id.NodeID != node.NodeID {
		return fmt.Errorf("local node_id %s conflicts with system metadata node_id %s for raft node id %d", id.NodeID, node.NodeID, raftNodeID)
	}
	if strings.TrimSpace(id.BackendAdvertiseAddr) != "" && strings.TrimSpace(node.BackendAdvertiseAddr) != "" && strings.TrimSpace(id.BackendAdvertiseAddr) != strings.TrimSpace(node.BackendAdvertiseAddr) {
		return fmt.Errorf("local backend advertise addr %s conflicts with system metadata addr %s", id.BackendAdvertiseAddr, node.BackendAdvertiseAddr)
	}
	now := time.Now().UTC()
	id.NodeID = node.NodeID
	if strings.TrimSpace(id.NodeName) == "" {
		id.NodeName = node.NodeName
	}
	id.ClusterID = meta.ClusterID
	if strings.TrimSpace(meta.ClusterName) != "" {
		id.ClusterName = meta.ClusterName
	}
	if strings.TrimSpace(id.BackendAdvertiseAddr) == "" {
		id.BackendAdvertiseAddr = node.BackendAdvertiseAddr
	}
	id.ClusterAdmitted = true
	id.ClusterBootstrap = raftNodeID == 1
	id.UpdatedAt = now
	if err := ValidateIdentity(id); err != nil {
		return err
	}
	if err := writeIdentity(filepath.Join(ClusteringDir(m.dataDir), nodeFileName), id); err != nil {
		return err
	}
	if err := WriteLocalState(m.dataDir, NodeStateInitializing, now); err != nil {
		return err
	}
	if err := WritePeers(m.dataDir, id, nil, now); err != nil {
		return err
	}
	readiness := ClusterReadiness{ClientReady: false, MetadataApplied: true, MetadataValidated: true, PartitionGroupsStarted: false, AuthoritativeClusterID: meta.ClusterID, LocalClusterID: id.ClusterID, ExpectedMemberCount: meta.NodeCount, ReadinessBlockers: []string{"partition groups are not started"}}
	m.mu.Lock()
	m.identity = id
	m.state = NodeStateInitializing
	m.readiness = readiness
	m.systemMetadata = cloneSystemMetadata(meta)
	m.mu.Unlock()
	self := selfPeer(id)
	if m.topology != nil {
		if err := m.topology.Upsert(ctx, self); err != nil {
			return err
		}
	}
	m.membership = membership.NewFileStore(membership.Path(m.dataDir), id.ClusterID, id.ClusterName)
	for _, metaNode := range meta.Nodes {
		memberCreatedAt := now
		nodeID := metaNode.NodeID
		member := membership.Member{NodeName: metaNode.NodeName, NodeID: nodeID, State: membership.MemberStateActive, BackendAdvertiseAddr: metaNode.BackendAdvertiseAddr, Role: "member", ClusterBootstrap: metaNode.RaftNodeID == 1, CreatedAt: memberCreatedAt, UpdatedAt: now}
		if nodeID == id.NodeID {
			member.NodeName = id.NodeName
			member.BackendAdvertiseAddr = id.BackendAdvertiseAddr
			member.NodePublicKeyFingerprint = id.NodePublicKeyFingerprint
			member.CreatedAt = id.CreatedAt
		}
		joined := now
		member.JoinedAt = &joined
		if err := m.membership.UpsertMember(ctx, member); err != nil {
			return err
		}
		if m.topology != nil && nodeID != id.NodeID {
			seen := now
			if err := m.topology.Upsert(ctx, model.Peer{NodeID: metaNode.NodeID, NodeName: metaNode.NodeName, ClusterID: meta.ClusterID, ClusterName: meta.ClusterName, BackendAdvertiseAddr: metaNode.BackendAdvertiseAddr, State: model.PeerStateActive, Source: model.PeerSourceDiscovered, LastSeenAt: &seen}); err != nil {
				return err
			}
		}
	}
	if m.backend != nil {
		m.backend.Identity = id
		m.backend.State = NodeStateInitializing
		m.backend.Membership = m.membership
	}
	if m.registration != nil {
		m.registration.Identity = id
		m.registration.State = NodeStateInitializing
	}
	return nil
}

func (m *Manager) MarkPartitionGroupsStarted(actual int, expected int) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readiness.PartitionGroupsStarted = actual >= expected
	m.readiness.ClientReady = m.readiness.MetadataApplied && m.readiness.MetadataValidated && m.readiness.PartitionGroupsStarted
	m.readiness.ReadinessBlockers = removeReadinessBlocker(m.readiness.ReadinessBlockers, "partition groups are not started")
	if !m.readiness.PartitionGroupsStarted {
		m.readiness = m.readiness.withBlocker(fmt.Sprintf("partition groups not started: got %d expected %d", actual, expected))
	}
	if m.readiness.ClientReady {
		m.state = NodeStateClustered
		if m.backend != nil {
			m.backend.State = m.state
		}
		if m.registration != nil {
			m.registration.State = m.state
		}
		return WriteLocalState(m.dataDir, NodeStateClustered, time.Now().UTC())
	}
	return nil
}

func removeReadinessBlocker(blockers []string, blocker string) []string {
	out := blockers[:0]
	for _, existing := range blockers {
		if existing != blocker {
			out = append(out, existing)
		}
	}
	return out
}

func initialReadiness(id model.NodeIdentity, state model.NodeState, opts Options) ClusterReadiness {
	out := ClusterReadiness{LocalClusterID: id.ClusterID}
	if !opts.RaftMode {
		out.ExpectedMemberCount = 1
		out.ClientReady = state == NodeStateStandalone || state == NodeStateClustered
		out.MetadataApplied = true
		out.MetadataValidated = true
		out.PartitionGroupsStarted = true
		out.AuthoritativeClusterID = id.ClusterID
		return out
	}
	out.ExpectedMemberCount = opts.RaftNodeCount
	out.MetadataApplied = strings.TrimSpace(id.ClusterID) != "" && id.ClusterAdmitted
	out.MetadataValidated = false
	out.PartitionGroupsStarted = false
	out.ClientReady = false
	if !out.MetadataApplied {
		out = out.withBlocker("system metadata not applied")
	} else {
		out = out.withBlocker("system metadata not validated")
	}
	return out
}

func cloneSystemMetadata(meta consensus.SystemMetadata) consensus.SystemMetadata {
	out := meta
	if meta.Nodes != nil {
		out.Nodes = make(map[string]consensus.SystemNode, len(meta.Nodes))
		for k, v := range meta.Nodes {
			out.Nodes[k] = v
		}
	}
	if meta.Placement != nil {
		out.Placement = make(map[uint32]consensus.PartitionPlacement, len(meta.Placement))
		for k, v := range meta.Placement {
			vv := v
			vv.ReplicaNodeIDs = append([]string(nil), v.ReplicaNodeIDs...)
			out.Placement[k] = vv
		}
	}
	return out
}

func systemNodeForRaftNode(meta consensus.SystemMetadata, raftNodeID consensus.NodeID) (consensus.SystemNode, bool) {
	for _, node := range meta.Nodes {
		if node.RaftNodeID == uint64(raftNodeID) {
			return node, true
		}
	}
	return consensus.SystemNode{}, false
}

func selfPeer(id model.NodeIdentity) model.Peer {
	if id.BackendAdvertiseAddr == "" && id.NodeID == "" {
		return model.Peer{}
	}
	now := time.Now().UTC()
	return model.Peer{NodeID: id.NodeID, NodeName: id.NodeName, ClusterID: id.ClusterID, ClusterName: id.ClusterName, BackendAdvertiseAddr: id.BackendAdvertiseAddr, State: model.PeerStateSelf, Source: model.PeerSourceSelf, LastSeenAt: &now}
}

func ClusteringDir(dataDir string) string { return filepath.Join(dataDir, "meta", "clustering") }
