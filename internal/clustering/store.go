package clustering

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

const nodeFileName = "node.json"

func LoadOrCreate(ctx context.Context, opts Options) (LocalNode, error) {
	if err := ctx.Err(); err != nil {
		return LocalNode{State: NodeStateFailed}, err
	}
	if strings.TrimSpace(opts.DataDir) == "" {
		return LocalNode{State: NodeStateFailed}, fmt.Errorf("data dir is required for clustering identity")
	}
	if err := ValidateBackendAdvertiseAddr(opts.BackendAdvertiseAddr); err != nil {
		return LocalNode{State: NodeStateFailed}, err
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	dir := filepath.Join(opts.DataDir, "meta", "clustering")
	path := filepath.Join(dir, nodeFileName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return LocalNode{State: NodeStateFailed}, fmt.Errorf("create clustering metadata directory: %w", err)
	}
	id, err := readIdentity(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return LocalNode{State: NodeStateFailed}, err
		}
		now := nowFn().UTC()
		clustered := strings.TrimSpace(opts.ClusterName) != "" || strings.TrimSpace(opts.BackendAdvertiseAddr) != ""
		id = NodeIdentity{Version: NodeIdentityVersion, NodeID: "node_" + uuid.NewString(), NodeName: strings.TrimSpace(opts.NodeName), ClusterID: "cluster_" + uuid.NewString(), ClusterName: strings.TrimSpace(opts.ClusterName), BackendAdvertiseAddr: strings.TrimSpace(opts.BackendAdvertiseAddr), ClusterAdmitted: clustered, ClusterBootstrap: clustered, NodePublicKeyFingerprint: strings.TrimSpace(opts.NodePublicKeyFingerprint), CreatedAt: now, UpdatedAt: now}
		if err := ValidateIdentity(id); err != nil {
			return LocalNode{State: NodeStateFailed}, err
		}
		if err := writeIdentity(path, id); err != nil {
			return LocalNode{State: NodeStateFailed}, err
		}
		state := stateFor(id)
		if err := WriteLocalState(opts.DataDir, state, now); err != nil {
			return LocalNode{State: NodeStateFailed}, err
		}
		if err := WritePeers(opts.DataDir, id, nil, now); err != nil {
			return LocalNode{State: NodeStateFailed}, err
		}
		return LocalNode{Identity: id, State: state}, nil
	}
	if err := ValidateIdentity(id); err != nil {
		return LocalNode{State: NodeStateFailed}, err
	}
	changed := false
	if v := strings.TrimSpace(opts.NodeName); v != "" && id.NodeName != v {
		id.NodeName = v
		changed = true
	}
	if v := strings.TrimSpace(opts.ClusterName); v != "" && id.ClusterName != v {
		id.ClusterName = v
		changed = true
	}
	if v := strings.TrimSpace(opts.BackendAdvertiseAddr); v != "" && id.BackendAdvertiseAddr != v {
		id.BackendAdvertiseAddr = v
		changed = true
	}
	if v := strings.TrimSpace(opts.NodePublicKeyFingerprint); v != "" && id.NodePublicKeyFingerprint != v {
		id.NodePublicKeyFingerprint = v
		changed = true
	}
	if changed {
		id.UpdatedAt = nowFn().UTC()
		if err := ValidateIdentity(id); err != nil {
			return LocalNode{State: NodeStateFailed}, err
		}
		if err := writeIdentity(path, id); err != nil {
			return LocalNode{State: NodeStateFailed}, err
		}
	}
	state := stateFor(id)
	now := nowFn().UTC()
	if err := WriteLocalState(opts.DataDir, state, now); err != nil {
		return LocalNode{State: NodeStateFailed}, err
	}
	if err := WritePeers(opts.DataDir, id, nil, now); err != nil {
		return LocalNode{State: NodeStateFailed}, err
	}
	return LocalNode{Identity: id, State: state}, nil
}

func stateFor(id NodeIdentity) NodeState {
	if strings.TrimSpace(id.ClusterName) != "" || strings.TrimSpace(id.BackendAdvertiseAddr) != "" {
		return NodeStateClustered
	}
	return NodeStateStandalone
}

func readIdentity(path string) (NodeIdentity, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return NodeIdentity{}, err
	}
	var id NodeIdentity
	if err := json.Unmarshal(raw, &id); err != nil {
		return NodeIdentity{}, fmt.Errorf("read clustering node identity: %w", err)
	}
	return id, nil
}

func writeIdentity(path string, id NodeIdentity) error {
	raw, err := json.MarshalIndent(id, "", "  ")
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
