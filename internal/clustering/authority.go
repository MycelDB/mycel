package clustering

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

const AuthorityVersion = 1

type AuthoritySource string

const (
	AuthoritySourceBootstrap AuthoritySource = "bootstrap"
	AuthoritySourceManual    AuthoritySource = "manual"
	AuthoritySourceElection  AuthoritySource = "election"
	AuthoritySourceRecovered AuthoritySource = "recovered"
)

type NodeRole string

const (
	NodeRoleNone      NodeRole = "none"
	NodeRolePrimary   NodeRole = "primary"
	NodeRoleFollower  NodeRole = "follower"
	NodeRoleCandidate NodeRole = "candidate"
	NodeRoleObserver  NodeRole = "observer"
	NodeRoleLearner   NodeRole = "learner"
)

type AuthorityPrimary struct {
	NodeID               string `json:"node_id"`
	NodeName             string `json:"node_name,omitempty"`
	BackendAdvertiseAddr string `json:"backend_advertise_addr,omitempty"`
}

type Authority struct {
	Version        int              `json:"version"`
	ClusterID      string           `json:"cluster_id"`
	Primary        AuthorityPrimary `json:"primary"`
	AuthorityEpoch int64            `json:"authority_epoch"`
	Term           int64            `json:"term,omitempty"`
	Source         AuthoritySource  `json:"source"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

func AuthorityPath(dataDir string) string {
	return filepath.Join(ClusteringDir(dataDir), "authority.json")
}

func LoadAuthority(ctx context.Context, path string) (Authority, bool, error) {
	if err := ctx.Err(); err != nil {
		return Authority{}, false, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Authority{}, false, nil
		}
		return Authority{}, false, err
	}
	var authority Authority
	if err := json.Unmarshal(raw, &authority); err != nil {
		return Authority{}, false, err
	}
	if authority.Version == 0 {
		authority.Version = AuthorityVersion
	}
	return authority, true, nil
}

func SaveAuthority(ctx context.Context, path string, authority Authority) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if authority.Version == 0 {
		authority.Version = AuthorityVersion
	}
	if authority.ClusterID == "" {
		return fmt.Errorf("authority cluster_id is required")
	}
	if authority.Primary.NodeID == "" {
		return fmt.Errorf("authority primary node_id is required")
	}
	if authority.AuthorityEpoch <= 0 {
		return fmt.Errorf("authority epoch must be positive")
	}
	if authority.Source == "" {
		authority.Source = AuthoritySourceRecovered
	}
	if authority.UpdatedAt.IsZero() {
		authority.UpdatedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create authority directory: %w", err)
	}
	raw, err := json.MarshalIndent(authority, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func InitBootstrapAuthority(ctx context.Context, dataDir string, identity model.NodeIdentity, now time.Time) (Authority, error) {
	path := AuthorityPath(dataDir)
	if existing, ok, err := LoadAuthority(ctx, path); err != nil {
		return Authority{}, err
	} else if ok {
		if existing.ClusterID != identity.ClusterID {
			return Authority{}, fmt.Errorf("authority cluster_id %q does not match local cluster_id %q", existing.ClusterID, identity.ClusterID)
		}
		return existing, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	authority := Authority{
		Version:   AuthorityVersion,
		ClusterID: identity.ClusterID,
		Primary: AuthorityPrimary{
			NodeID:               identity.NodeID,
			NodeName:             identity.NodeName,
			BackendAdvertiseAddr: identity.BackendAdvertiseAddr,
		},
		AuthorityEpoch: 1,
		Term:           0,
		Source:         AuthoritySourceBootstrap,
		UpdatedAt:      now.UTC(),
	}
	if err := SaveAuthority(ctx, path, authority); err != nil {
		return Authority{}, err
	}
	return authority, nil
}

func AuthorityToProto(authority Authority, ok bool) *clusterpb.ClusterAuthority {
	if !ok {
		return nil
	}
	return &clusterpb.ClusterAuthority{Version: int32(authority.Version), ClusterId: authority.ClusterID, Primary: &clusterpb.AuthorityPrimary{NodeId: authority.Primary.NodeID, NodeName: authority.Primary.NodeName, BackendAdvertiseAddr: authority.Primary.BackendAdvertiseAddr}, AuthorityEpoch: authority.AuthorityEpoch, Term: authority.Term, Source: string(authority.Source), UpdatedAt: formatAuthorityTime(authority.UpdatedAt)}
}

func AuthorityFromProto(proto *clusterpb.ClusterAuthority) (Authority, bool, error) {
	if proto == nil || proto.GetClusterId() == "" || proto.GetPrimary().GetNodeId() == "" {
		return Authority{}, false, nil
	}
	updated, err := parseAuthorityTime(proto.GetUpdatedAt())
	if err != nil {
		return Authority{}, false, fmt.Errorf("parse authority updated_at: %w", err)
	}
	version := int(proto.GetVersion())
	if version == 0 {
		version = AuthorityVersion
	}
	return Authority{Version: version, ClusterID: proto.GetClusterId(), Primary: AuthorityPrimary{NodeID: proto.GetPrimary().GetNodeId(), NodeName: proto.GetPrimary().GetNodeName(), BackendAdvertiseAddr: proto.GetPrimary().GetBackendAdvertiseAddr()}, AuthorityEpoch: proto.GetAuthorityEpoch(), Term: proto.GetTerm(), Source: AuthoritySource(proto.GetSource()), UpdatedAt: updated}, true, nil
}

func formatAuthorityTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseAuthorityTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func DeriveLocalRole(lifecycle model.NodeState, identity model.NodeIdentity, authority Authority, authorityKnown bool) NodeRole {
	if lifecycle == model.NodeStateStandalone || !identity.ClusterAdmitted || !authorityKnown || authority.Primary.NodeID == "" {
		return NodeRoleNone
	}
	if identity.NodeID != "" && identity.NodeID == authority.Primary.NodeID {
		return NodeRolePrimary
	}
	if lifecycle == model.NodeStateClustered {
		return NodeRoleFollower
	}
	return NodeRoleNone
}
