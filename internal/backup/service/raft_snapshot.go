package service

import (
	"context"
	"encoding/json"
	"fmt"

	backupcore "github.com/myceldb/mycel/internal/backup"
)

const backupRaftSnapshotVersion = 1

type backupRaftSnapshot struct {
	Version int               `json:"version"`
	Policy  backupcore.Policy `json:"policy"`
	Runtime backupRuntimeNote `json:"runtime"`
}

type backupRuntimeNote struct {
	RunningExecutions string `json:"running_executions"`
	CompletedArchives string `json:"completed_archives"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	policy := backupcore.Policy{}
	if s.Module != nil && s.Module.manager != nil {
		policy = s.Module.Policy()
	}
	return json.Marshal(backupRaftSnapshot{Version: backupRaftSnapshotVersion, Policy: policy, Runtime: backupRuntimeNote{RunningExecutions: "excluded_local_only", CompletedArchives: "local_artifacts_not_authoritative"}})
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil || len(data) == 0 {
		return nil
	}
	var snap backupRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != backupRaftSnapshotVersion {
		return fmt.Errorf("unsupported backup raft snapshot version %d", snap.Version)
	}
	if s.Module.manager == nil {
		return fmt.Errorf("backup manager is not initialized")
	}
	updated, err := s.Module.manager.UpdatePolicy(context.Background(), snap.Policy)
	if err != nil {
		return err
	}
	s.Module.mu.Lock()
	s.Module.policy = updated
	s.Module.lastError = ""
	s.Module.mu.Unlock()
	return s.Module.reconcileSchedulerForPolicy(context.Background(), updated)
}
