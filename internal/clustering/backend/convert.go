package backend

import (
	"fmt"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func IdentityToProto(id model.NodeIdentity) *clusterpb.NodeIdentity {
	return &clusterpb.NodeIdentity{Version: int32(id.Version), NodeId: id.NodeID, NodeName: id.NodeName, ClusterId: id.ClusterID, ClusterName: id.ClusterName, BackendAdvertiseAddr: id.BackendAdvertiseAddr, CreatedAt: formatTime(id.CreatedAt), UpdatedAt: formatTime(id.UpdatedAt), ClusterAdmitted: id.ClusterAdmitted, ClusterBootstrap: id.ClusterBootstrap, NodePublicKeyFingerprint: id.NodePublicKeyFingerprint}
}

func IdentityFromProto(p *clusterpb.NodeIdentity) (model.NodeIdentity, error) {
	if p == nil {
		return model.NodeIdentity{}, fmt.Errorf("node identity is required")
	}
	created, err := parseTime(p.GetCreatedAt())
	if err != nil {
		return model.NodeIdentity{}, fmt.Errorf("parse created_at: %w", err)
	}
	updated, err := parseTime(p.GetUpdatedAt())
	if err != nil {
		return model.NodeIdentity{}, fmt.Errorf("parse updated_at: %w", err)
	}
	return model.NodeIdentity{Version: int(p.GetVersion()), NodeID: p.GetNodeId(), NodeName: p.GetNodeName(), ClusterID: p.GetClusterId(), ClusterName: p.GetClusterName(), BackendAdvertiseAddr: p.GetBackendAdvertiseAddr(), ClusterAdmitted: p.GetClusterAdmitted(), ClusterBootstrap: p.GetClusterBootstrap(), NodePublicKeyFingerprint: p.GetNodePublicKeyFingerprint(), CreatedAt: created, UpdatedAt: updated}, nil
}

func PeerToProto(peer model.Peer) *clusterpb.Peer {
	last := ""
	if peer.LastSeenAt != nil {
		last = formatTime(*peer.LastSeenAt)
	}
	return &clusterpb.Peer{NodeId: peer.NodeID, NodeName: peer.NodeName, ClusterId: peer.ClusterID, ClusterName: peer.ClusterName, BackendAdvertiseAddr: peer.BackendAdvertiseAddr, State: PeerStateToProto(peer.State), Source: PeerSourceToProto(peer.Source), LastSeenAt: last}
}

func PeerFromProto(p *clusterpb.Peer) (model.Peer, error) {
	if p == nil {
		return model.Peer{}, fmt.Errorf("peer is required")
	}
	var last *time.Time
	if p.GetLastSeenAt() != "" {
		t, err := parseTime(p.GetLastSeenAt())
		if err != nil {
			return model.Peer{}, fmt.Errorf("parse last_seen_at: %w", err)
		}
		last = &t
	}
	return model.Peer{NodeID: p.GetNodeId(), NodeName: p.GetNodeName(), ClusterID: p.GetClusterId(), ClusterName: p.GetClusterName(), BackendAdvertiseAddr: p.GetBackendAdvertiseAddr(), State: PeerStateFromProto(p.GetState()), Source: PeerSourceFromProto(p.GetSource()), LastSeenAt: last}, nil
}

func SnapshotToProto(s model.Snapshot, identity model.NodeIdentity, state model.NodeState) *clusterpb.ClusterView {
	peers := make([]*clusterpb.Peer, 0, len(s.Peers))
	for _, peer := range s.Peers {
		peers = append(peers, PeerToProto(peer))
	}
	return &clusterpb.ClusterView{Version: int32(s.Version), Mode: ClusterModeToProto(modeForState(state)), LocalState: NodeStateToProto(state), LocalIdentity: IdentityToProto(identity), Peers: peers, UpdatedAt: formatTime(s.UpdatedAt)}
}

func modeForState(state model.NodeState) model.ClusterMode {
	if state == model.NodeStateClustered {
		return model.ClusterModeClustered
	}
	return model.ClusterModeStandalone
}

func SnapshotFromProto(v *clusterpb.ClusterView) (model.Snapshot, error) {
	if v == nil {
		return model.Snapshot{}, fmt.Errorf("cluster view is required")
	}
	peers := make([]model.Peer, 0, len(v.GetPeers()))
	for _, p := range v.GetPeers() {
		peer, err := PeerFromProto(p)
		if err != nil {
			return model.Snapshot{}, err
		}
		peers = append(peers, peer)
	}
	updated, err := parseOptionalTime(v.GetUpdatedAt())
	if err != nil {
		return model.Snapshot{}, err
	}
	return model.Snapshot{Version: int(v.GetVersion()), UpdatedAt: updated, Peers: peers}, nil
}

func NodeStateToProto(s model.NodeState) clusterpb.NodeLifecycleState {
	switch s {
	case model.NodeStateInitializing:
		return clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_INITIALIZING
	case model.NodeStateStandalone:
		return clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_STANDALONE
	case model.NodeStateClustered:
		return clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_CLUSTERED
	case model.NodeStateStopped:
		return clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_STOPPED
	case model.NodeStateFailed:
		return clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_FAILED
	default:
		return clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_UNSPECIFIED
	}
}

func NodeStateFromProto(s clusterpb.NodeLifecycleState) model.NodeState {
	switch s {
	case clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_INITIALIZING:
		return model.NodeStateInitializing
	case clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_STANDALONE:
		return model.NodeStateStandalone
	case clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_CLUSTERED:
		return model.NodeStateClustered
	case clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_STOPPED:
		return model.NodeStateStopped
	case clusterpb.NodeLifecycleState_NODE_LIFECYCLE_STATE_FAILED:
		return model.NodeStateFailed
	default:
		return ""
	}
}

func PeerStateToProto(s model.PeerState) clusterpb.PeerMembershipState {
	switch s {
	case model.PeerStateSelf:
		return clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_SELF
	case model.PeerStateSeed:
		return clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_SEED
	case model.PeerStateActive:
		return clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_ACTIVE
	case model.PeerStateUnreachable:
		return clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_UNREACHABLE
	default:
		return clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_UNSPECIFIED
	}
}

func PeerStateFromProto(s clusterpb.PeerMembershipState) model.PeerState {
	switch s {
	case clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_SELF:
		return model.PeerStateSelf
	case clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_SEED:
		return model.PeerStateSeed
	case clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_ACTIVE:
		return model.PeerStateActive
	case clusterpb.PeerMembershipState_PEER_MEMBERSHIP_STATE_UNREACHABLE:
		return model.PeerStateUnreachable
	default:
		return ""
	}
}

func PeerSourceToProto(s model.PeerSource) clusterpb.PeerSource {
	switch s {
	case model.PeerSourceSelf:
		return clusterpb.PeerSource_PEER_SOURCE_SELF
	case model.PeerSourceSeed:
		return clusterpb.PeerSource_PEER_SOURCE_SEED
	case model.PeerSourceDiscovered:
		return clusterpb.PeerSource_PEER_SOURCE_DISCOVERED
	default:
		return clusterpb.PeerSource_PEER_SOURCE_UNSPECIFIED
	}
}

func PeerSourceFromProto(s clusterpb.PeerSource) model.PeerSource {
	switch s {
	case clusterpb.PeerSource_PEER_SOURCE_SELF:
		return model.PeerSourceSelf
	case clusterpb.PeerSource_PEER_SOURCE_SEED:
		return model.PeerSourceSeed
	case clusterpb.PeerSource_PEER_SOURCE_DISCOVERED:
		return model.PeerSourceDiscovered
	default:
		return ""
	}
}

func ClusterModeToProto(m model.ClusterMode) clusterpb.ClusterMode {
	if m == model.ClusterModeClustered {
		return clusterpb.ClusterMode_CLUSTER_MODE_CLUSTERED
	}
	return clusterpb.ClusterMode_CLUSTER_MODE_STANDALONE
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func parseOptionalTime(s string) (time.Time, error) { return parseTime(s) }
