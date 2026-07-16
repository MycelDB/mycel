package clustering

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/registration"
	"github.com/myceldb/mycel/internal/clustering/topology"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

type Manager struct {
	identity     model.NodeIdentity
	state        model.NodeState
	topology     *topology.Registry
	membership   *membership.FileStore
	authority    Authority
	authorityOK  bool
	registration *registration.Handler
	backend      *backend.Service
	logger       *slog.Logger
	dataDir      string
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
	for _, seed := range opts.SeedPeers {
		if seed == "" {
			continue
		}
		if err := ValidateBackendAdvertiseAddr(seed); err != nil {
			return nil, err
		}
	}
	membershipStore := membership.NewFileStore(membership.Path(opts.DataDir), local.Identity.ClusterID, local.Identity.ClusterName)
	authority, authorityOK, err := LoadAuthority(ctx, AuthorityPath(opts.DataDir))
	if err != nil {
		return nil, err
	}
	if local.Identity.ClusterAdmitted && local.Identity.ClusterBootstrap {
		now := time.Now().UTC()
		joined := now
		if err := membershipStore.UpsertMember(ctx, membership.Member{NodeName: local.Identity.NodeName, NodeID: local.Identity.NodeID, State: membership.MemberStateActive, BackendAdvertiseAddr: local.Identity.BackendAdvertiseAddr, Role: "member", ClusterBootstrap: true, NodePublicKeyFingerprint: local.Identity.NodePublicKeyFingerprint, CreatedAt: local.Identity.CreatedAt, UpdatedAt: now, JoinedAt: &joined}); err != nil {
			return nil, err
		}
		if !authorityOK {
			authority, err = InitBootstrapAuthority(ctx, opts.DataDir, local.Identity, now)
			if err != nil {
				return nil, err
			}
			authorityOK = true
		} else if authority.ClusterID != local.Identity.ClusterID {
			return nil, fmt.Errorf("authority cluster_id %q does not match local cluster_id %q", authority.ClusterID, local.Identity.ClusterID)
		}
	} else if authorityOK && authority.ClusterID != local.Identity.ClusterID {
		return nil, fmt.Errorf("authority cluster_id %q does not match local cluster_id %q", authority.ClusterID, local.Identity.ClusterID)
	}
	m := &Manager{identity: local.Identity, state: local.State, topology: registry, membership: membershipStore, authority: authority, authorityOK: authorityOK, logger: logger, dataDir: opts.DataDir}
	m.backend = backend.NewService(m.identity, m.state, registry).WithMembership(membershipStore).WithAuthority(AuthorityToProto(authority, authorityOK))
	joinToken, err := LoadJoinToken(opts)
	if err != nil {
		return nil, err
	}
	m.registration = &registration.Handler{Topology: registry, Client: registration.BackendAdapter{Client: backend.Client{}}, Seeds: opts.SeedPeers, Identity: m.identity, State: m.state, Interval: 5 * time.Second, Timeout: 2 * time.Second, Logger: logger, JoinToken: joinToken, OnAdmitted: func(clusterID string) error {
		id, err := AdmitLocalNode(ctx, opts.DataDir, clusterID)
		if err != nil {
			return err
		}
		m.identity = id
		m.backend.Identity = id
		return nil
	}, OnAuthority: func(authorityProto *clusterpb.ClusterAuthority) error {
		authority, ok, err := AuthorityFromProto(authorityProto)
		if err != nil || !ok {
			return err
		}
		return m.SetAuthority(ctx, authority)
	}}
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

func (m *Manager) Identity() model.NodeIdentity {
	if m == nil {
		return model.NodeIdentity{}
	}
	return m.identity
}
func (m *Manager) State() model.NodeState {
	if m == nil {
		return ""
	}
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

func (m *Manager) IsAdmitted() bool {
	if m == nil {
		return false
	}
	return m.identity.ClusterAdmitted
}

func (m *Manager) IsBootstrap() bool {
	if m == nil {
		return false
	}
	return m.identity.ClusterBootstrap
}

func (m *Manager) Authority() (Authority, bool) {
	if m == nil {
		return Authority{}, false
	}
	return m.authority, m.authorityOK
}

func (m *Manager) LocalRole() NodeRole {
	if m == nil {
		return NodeRoleNone
	}
	return DeriveLocalRole(m.state, m.identity, m.authority, m.authorityOK)
}

func (m *Manager) SetAuthority(ctx context.Context, authority Authority) error {
	if m == nil {
		return nil
	}
	if authority.ClusterID != m.identity.ClusterID {
		return fmt.Errorf("authority cluster_id %q does not match local cluster_id %q", authority.ClusterID, m.identity.ClusterID)
	}
	if err := SaveAuthority(ctx, AuthorityPath(m.dataDir), authority); err != nil {
		return err
	}
	m.authority = authority
	m.authorityOK = true
	if m.backend != nil {
		m.backend.WithAuthority(AuthorityToProto(authority, true))
	}
	return nil
}

func (m *Manager) BackendService() clusterpb.ClusterBackendServiceServer {
	if m == nil {
		return nil
	}
	return m.backend
}
func (m *Manager) Registration() *registration.Handler {
	if m == nil {
		return nil
	}
	return m.registration
}

func selfPeer(id model.NodeIdentity) model.Peer {
	if id.BackendAdvertiseAddr == "" && id.NodeID == "" {
		return model.Peer{}
	}
	now := time.Now().UTC()
	return model.Peer{NodeID: id.NodeID, NodeName: id.NodeName, ClusterID: id.ClusterID, ClusterName: id.ClusterName, BackendAdvertiseAddr: id.BackendAdvertiseAddr, State: model.PeerStateSelf, Source: model.PeerSourceSelf, LastSeenAt: &now}
}

func ClusteringDir(dataDir string) string { return filepath.Join(dataDir, "meta", "clustering") }
