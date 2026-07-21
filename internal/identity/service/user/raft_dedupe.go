package user

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

func (m *Module) loadRaftAppliedCommands() {
	if m.dataDir == "" {
		return
	}
	data, err := os.ReadFile(m.raftAppliedCommandsPath())
	if err != nil {
		return
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	for _, id := range ids {
		if id != "" {
			m.raftAppliedCommands[id] = struct{}{}
		}
	}
}
func (m *Module) raftCommandApplied(commandID string) bool {
	if commandID == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.raftAppliedCommands[commandID]
	return ok
}
func (m *Module) rememberRaftAppliedCommand(ctx context.Context, commandID string) error {
	if commandID == "" {
		return nil
	}
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	m.raftAppliedCommands[commandID] = struct{}{}
	ids := make([]string, 0, len(m.raftAppliedCommands))
	for id := range m.raftAppliedCommands {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	if m.dataDir == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	payload, err := json.MarshalIndent(ids, "", "  ")
	if err != nil {
		return err
	}
	path := m.raftAppliedCommandsPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func (m *Module) raftAppliedCommandsPath() string {
	return filepath.Join(m.dataDir, "users", "raft-applied-commands.json")
}
