package clustering

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

const LocalStateVersion = 1

type ClusterMode = model.ClusterMode

const (
	ClusterModeStandalone = model.ClusterModeStandalone
	ClusterModeClustered  = model.ClusterModeClustered
)

type LocalState struct {
	Version   int         `json:"version"`
	Mode      ClusterMode `json:"mode"`
	State     NodeState   `json:"state"`
	UpdatedAt time.Time   `json:"updated_at"`
}

func StatePath(dataDir string) string {
	return filepath.Join(dataDir, "meta", "clustering", "local_state.json")
}

func ModeForState(state NodeState) ClusterMode {
	if state == NodeStateClustered {
		return ClusterModeClustered
	}
	return ClusterModeStandalone
}

func WriteLocalState(dataDir string, state NodeState, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	path := StatePath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create clustering state directory: %w", err)
	}
	local := LocalState{Version: LocalStateVersion, Mode: ModeForState(state), State: state, UpdatedAt: now.UTC()}
	raw, err := json.MarshalIndent(local, "", "  ")
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
