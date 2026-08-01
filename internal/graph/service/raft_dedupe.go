package service

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
	path := m.raftAppliedCommandsPath()
	data, err := os.ReadFile(path)
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

func (m *Module) rememberRaftAppliedCommand(ctx context.Context, commandID string) error {
	if commandID == "" {
		return nil
	}
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	m.raftAppliedCommands[commandID] = struct{}{}
	m.mu.Unlock()
	return m.persistRaftAppliedCommands(ctx)
}

func (m *Module) persistRaftAppliedCommands(ctx context.Context) error {
	m.mu.Lock()
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
	return filepath.Join(m.dataDir, "raft-applied-commands.json")
}
