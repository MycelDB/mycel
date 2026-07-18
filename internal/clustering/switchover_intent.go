package clustering

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type SwitchoverIntentPhase string

const (
	SwitchoverIntentStarted          SwitchoverIntentPhase = "started"
	SwitchoverIntentTargetInstalling SwitchoverIntentPhase = "target_installing"
	SwitchoverIntentTargetInstalled  SwitchoverIntentPhase = "target_installed"
	SwitchoverIntentLocalInstalled   SwitchoverIntentPhase = "local_installed"
	SwitchoverIntentCompleted        SwitchoverIntentPhase = "completed"
	SwitchoverIntentFailed           SwitchoverIntentPhase = "failed"
)

type SwitchoverIntent struct {
	Version          int                   `json:"version"`
	OperationID      string                `json:"operation_id"`
	ClusterID        string                `json:"cluster_id"`
	OldPrimaryNodeID string                `json:"old_primary_node_id"`
	NewPrimaryNodeID string                `json:"new_primary_node_id"`
	NewAuthority     Authority             `json:"new_authority"`
	FinalLSN         uint64                `json:"final_lsn"`
	Phase            SwitchoverIntentPhase `json:"phase"`
	Error            string                `json:"error,omitempty"`
	CreatedAt        time.Time             `json:"created_at"`
	UpdatedAt        time.Time             `json:"updated_at"`
}

func SwitchoverIntentPath(dataDir string) string {
	return filepath.Join(ClusteringDir(dataDir), "switchover-intent.json")
}

func LoadSwitchoverIntent(ctx context.Context, dataDir string) (SwitchoverIntent, bool, error) {
	if err := ctx.Err(); err != nil {
		return SwitchoverIntent{}, false, err
	}
	raw, err := os.ReadFile(SwitchoverIntentPath(dataDir))
	if os.IsNotExist(err) {
		return SwitchoverIntent{}, false, nil
	}
	if err != nil {
		return SwitchoverIntent{}, false, err
	}
	var intent SwitchoverIntent
	if err := json.Unmarshal(raw, &intent); err != nil {
		return SwitchoverIntent{}, false, err
	}
	return intent, true, nil
}

func SaveSwitchoverIntent(ctx context.Context, dataDir string, intent SwitchoverIntent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if intent.Version == 0 {
		intent.Version = 1
	}
	now := time.Now().UTC()
	if intent.CreatedAt.IsZero() {
		intent.CreatedAt = now
	}
	intent.UpdatedAt = now
	raw, err := json.MarshalIndent(intent, "", "  ")
	if err != nil {
		return err
	}
	path := SwitchoverIntentPath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ClearSwitchoverIntent(ctx context.Context, dataDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := os.Remove(SwitchoverIntentPath(dataDir))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
