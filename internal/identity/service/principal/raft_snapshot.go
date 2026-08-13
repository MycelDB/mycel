package principal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
)

const principalRaftSnapshotVersion = 1

type principalRaftSnapshot struct {
	Version         int      `json:"version"`
	StoreJSON       []byte   `json:"store_json"`
	SessionsJSON    []byte   `json:"sessions_json,omitempty"`
	AppliedCommands []string `json:"applied_commands,omitempty"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil || s.Module.dataDir == "" {
		return json.Marshal(principalRaftSnapshot{Version: principalRaftSnapshotVersion})
	}
	store, err := os.ReadFile(filepath.Join(s.Module.dataDir, "identity", StoreFilename))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sessions, err := os.ReadFile(filepath.Join(s.Module.dataDir, "identity", "sessions", "refresh_sessions.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	s.Module.mu.Lock()
	applied := make([]string, 0, len(s.Module.raftAppliedCommands))
	for id := range s.Module.raftAppliedCommands {
		applied = append(applied, id)
	}
	s.Module.mu.Unlock()
	return json.Marshal(principalRaftSnapshot{Version: principalRaftSnapshotVersion, StoreJSON: store, SessionsJSON: sessions, AppliedCommands: applied})
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil || len(data) == 0 {
		return nil
	}
	var snap principalRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != principalRaftSnapshotVersion {
		return fmt.Errorf("unsupported principal raft snapshot version %d", snap.Version)
	}
	identityDir := filepath.Join(s.Module.dataDir, "identity")
	if err := os.MkdirAll(filepath.Join(identityDir, "sessions"), 0o700); err != nil {
		return err
	}
	if len(snap.StoreJSON) == 0 {
		snap.StoreJSON = []byte(`{"principals":[],"role_bindings":[],"capability_grants":[]}`)
	}
	if err := os.WriteFile(filepath.Join(identityDir, StoreFilename), snap.StoreJSON, 0o600); err != nil {
		return err
	}
	if len(snap.SessionsJSON) == 0 {
		snap.SessionsJSON = []byte(`{"refresh_sessions":[]}`)
	}
	if err := os.WriteFile(filepath.Join(identityDir, "sessions", "refresh_sessions.json"), snap.SessionsJSON, 0o600); err != nil {
		return err
	}
	store, err := OpenExistingStore(identityDir)
	if err != nil {
		return err
	}
	sessions := storesession.NewManager()
	if err := sessions.Init(context.Background(), filepath.Join(identityDir, "sessions")); err != nil {
		return err
	}
	s.Module.mu.Lock()
	s.Module.store = store
	s.Module.sessions = sessions
	s.Module.raftAppliedCommands = map[string]struct{}{}
	for _, id := range snap.AppliedCommands {
		if id != "" {
			s.Module.raftAppliedCommands[id] = struct{}{}
		}
	}
	s.Module.mu.Unlock()
	return nil
}
