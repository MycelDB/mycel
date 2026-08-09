package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type raftAppliedCommandLogRecord struct {
	CommandID string    `json:"command_id"`
	At        time.Time `json:"at"`
}

func (m *Module) loadRaftAppliedCommands() {
	if m.dataDir == "" {
		return
	}
	_ = os.MkdirAll(filepath.Dir(m.raftAppliedCommandsPath()), 0o700)
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	data, err := os.ReadFile(m.raftAppliedCommandsPath())
	if err == nil {
		var ids []string
		if err := json.Unmarshal(data, &ids); err == nil {
			for _, id := range ids {
				if strings.TrimSpace(id) != "" {
					m.raftAppliedCommands[id] = struct{}{}
				}
			}
		}
	}
	logData, err := os.ReadFile(m.raftAppliedCommandsLogPath())
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(logData)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var record raftAppliedCommandLogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		if strings.TrimSpace(record.CommandID) != "" {
			m.raftAppliedCommands[record.CommandID] = struct{}{}
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
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return nil
	}
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	if _, ok := m.raftAppliedCommands[commandID]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if err := m.appendRaftAppliedCommand(ctx, commandID); err != nil {
		return err
	}
	m.mu.Lock()
	m.raftAppliedCommands[commandID] = struct{}{}
	m.mu.Unlock()
	return nil
}

func (m *Module) appendRaftAppliedCommand(ctx context.Context, commandID string) error {
	if m.dataDir == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	record := raftAppliedCommandLogRecord{CommandID: commandID, At: time.Now().UTC()}
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	path := m.raftAppliedCommandsLogPath()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(filepath.Dir(path), 0o700); mkErr != nil {
				return mkErr
			}
			f, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		}
		if err != nil {
			return err
		}
	}
	defer f.Close()
	if _, err := f.Write(append(payload, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func (m *Module) persistRaftAppliedCommands(ctx context.Context) error {
	if m.dataDir == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	m.mu.Lock()
	ids := make([]string, 0, len(m.raftAppliedCommands))
	for id := range m.raftAppliedCommands {
		ids = append(ids, id)
	}
	m.mu.Unlock()
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
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if err := os.Remove(m.raftAppliedCommandsLogPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *Module) raftAppliedCommandsPath() string {
	return filepath.Join(m.dataDir, "meta", "semantic", "raft-applied-commands.json")
}

func (m *Module) raftAppliedCommandsLogPath() string {
	return filepath.Join(m.dataDir, "meta", "semantic", "raft-applied-commands-000001.ksem")
}
