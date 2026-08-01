package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
)

const adminRaftSnapshotVersion = 1

type adminRaftSnapshot struct {
	Version         int      `json:"version"`
	AdminsJSON      []byte   `json:"admins_json"`
	SessionsJSON    []byte   `json:"sessions_json,omitempty"`
	AppliedCommands []string `json:"applied_commands,omitempty"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil || s.Module.dataDir == "" {
		return json.Marshal(adminRaftSnapshot{Version: adminRaftSnapshotVersion})
	}
	admins, err := os.ReadFile(filepath.Join(s.Module.dataDir, "admins", StoreFilename))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sessions, err := os.ReadFile(filepath.Join(s.Module.dataDir, "admins", "sessions", "refresh_sessions.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	s.Module.mu.Lock()
	applied := make([]string, 0, len(s.Module.raftAppliedCommands))
	for id := range s.Module.raftAppliedCommands {
		applied = append(applied, id)
	}
	s.Module.mu.Unlock()
	return json.Marshal(adminRaftSnapshot{Version: adminRaftSnapshotVersion, AdminsJSON: admins, SessionsJSON: sessions, AppliedCommands: applied})
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil || len(data) == 0 {
		return nil
	}
	var snap adminRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != adminRaftSnapshotVersion {
		return fmt.Errorf("unsupported admin raft snapshot version %d", snap.Version)
	}
	adminDir := filepath.Join(s.Module.dataDir, "admins")
	if err := os.MkdirAll(filepath.Join(adminDir, "sessions"), 0o700); err != nil {
		return err
	}
	if len(snap.AdminsJSON) == 0 {
		snap.AdminsJSON = []byte(`{"admins":[]}`)
	}
	if err := os.WriteFile(filepath.Join(adminDir, StoreFilename), snap.AdminsJSON, 0o600); err != nil {
		return err
	}
	if len(snap.SessionsJSON) == 0 {
		snap.SessionsJSON = []byte(`{"refresh_sessions":[]}`)
	}
	if err := os.WriteFile(filepath.Join(adminDir, "sessions", "refresh_sessions.json"), snap.SessionsJSON, 0o600); err != nil {
		return err
	}
	store, err := OpenExistingStore(adminDir)
	if err != nil {
		return err
	}
	sessions := storesession.NewManager()
	if err := sessions.Init(context.Background(), filepath.Join(adminDir, "sessions")); err != nil {
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
