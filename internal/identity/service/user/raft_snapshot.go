package user

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
)

const userRaftSnapshotVersion = 1

type userRaftSnapshot struct {
	Version         int      `json:"version"`
	UsersJSON       []byte   `json:"users_json"`
	SessionsJSON    []byte   `json:"sessions_json,omitempty"`
	AppliedCommands []string `json:"applied_commands,omitempty"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil || s.Module.dataDir == "" {
		return json.Marshal(userRaftSnapshot{Version: userRaftSnapshotVersion})
	}
	users, err := os.ReadFile(filepath.Join(s.Module.dataDir, "users", StoreFilename))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	sessions, err := os.ReadFile(filepath.Join(s.Module.dataDir, "users", "sessions", "refresh_sessions.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	s.Module.mu.Lock()
	applied := make([]string, 0, len(s.Module.raftAppliedCommands))
	for id := range s.Module.raftAppliedCommands {
		applied = append(applied, id)
	}
	s.Module.mu.Unlock()
	return json.Marshal(userRaftSnapshot{Version: userRaftSnapshotVersion, UsersJSON: users, SessionsJSON: sessions, AppliedCommands: applied})
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil || len(data) == 0 {
		return nil
	}
	var snap userRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != userRaftSnapshotVersion {
		return fmt.Errorf("unsupported user raft snapshot version %d", snap.Version)
	}
	userDir := filepath.Join(s.Module.dataDir, "users")
	if err := os.MkdirAll(filepath.Join(userDir, "sessions"), 0o700); err != nil {
		return err
	}
	if len(snap.UsersJSON) == 0 {
		snap.UsersJSON = []byte(`{"users":[]}`)
	}
	if err := os.WriteFile(filepath.Join(userDir, StoreFilename), snap.UsersJSON, 0o600); err != nil {
		return err
	}
	if len(snap.SessionsJSON) == 0 {
		snap.SessionsJSON = []byte(`{"refresh_sessions":[]}`)
	}
	if err := os.WriteFile(filepath.Join(userDir, "sessions", "refresh_sessions.json"), snap.SessionsJSON, 0o600); err != nil {
		return err
	}
	store, err := OpenExistingStore(userDir)
	if err != nil {
		return err
	}
	sessions := storesession.NewManager()
	if err := sessions.Init(context.Background(), filepath.Join(userDir, "sessions")); err != nil {
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
