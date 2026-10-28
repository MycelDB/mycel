package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/filestore"
	"github.com/myceldb/mycel/internal/fsperm"
)

type raftAppliedCommandLogRecord struct {
	CommandID string    `json:"command_id"`
	At        time.Time `json:"at"`
}

func (m *Module) loadRaftAppliedCommands() error {
	if m.dataDir == "" {
		return nil
	}
	m.raftAppliedCommandsMu.Lock()
	defer m.raftAppliedCommandsMu.Unlock()

	if err := os.MkdirAll(filepath.Dir(m.raftAppliedCommandsPath()), fsperm.PrivateDir); err != nil {
		return err
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	data, err := os.ReadFile(m.raftAppliedCommandsPath())
	if err == nil {
		var ids []string
		if err := json.Unmarshal(data, &ids); err != nil {
			return fmt.Errorf("decode semantic applied-command snapshot: %w", err)
		}
		for _, id := range ids {
			if id = strings.TrimSpace(id); id != "" {
				m.raftAppliedCommands[id] = struct{}{}
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read semantic applied-command snapshot: %w", err)
	}
	return m.loadRaftAppliedCommandLogLocked()
}

// loadRaftAppliedCommandLogLocked accepts complete newline-delimited records,
// repairs a complete final record missing only its newline, and truncates a
// malformed final fragment. Malformed records before the final fragment are
// corruption and fail startup closed.
func (m *Module) loadRaftAppliedCommandLogLocked() error {
	path := m.raftAppliedCommandsLogPath()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read semantic applied-command journal: %w", err)
	}
	for offset := 0; offset < len(data); {
		relativeEnd := bytes.IndexByte(data[offset:], '\n')
		if relativeEnd < 0 {
			record, decodeErr := decodeRaftAppliedCommandLogRecord(data[offset:])
			if decodeErr != nil {
				if err := truncateAndSyncFile(path, int64(offset)); err != nil {
					return fmt.Errorf("repair torn semantic applied-command journal at offset %d: %w", offset, err)
				}
				return nil
			}
			m.raftAppliedCommands[record.CommandID] = struct{}{}
			if err := appendAndSyncFile(path, []byte{'\n'}); err != nil {
				return fmt.Errorf("repair semantic applied-command journal newline: %w", err)
			}
			return nil
		}
		end := offset + relativeEnd
		record, err := decodeRaftAppliedCommandLogRecord(data[offset:end])
		if err != nil {
			return fmt.Errorf("corrupt semantic applied-command journal record at offset %d: %w", offset, err)
		}
		m.raftAppliedCommands[record.CommandID] = struct{}{}
		offset = end + 1
	}
	return nil
}

func decodeRaftAppliedCommandLogRecord(line []byte) (raftAppliedCommandLogRecord, error) {
	var record raftAppliedCommandLogRecord
	if len(bytes.TrimSpace(line)) == 0 {
		return record, fmt.Errorf("empty record")
	}
	if err := json.Unmarshal(line, &record); err != nil {
		return record, err
	}
	record.CommandID = strings.TrimSpace(record.CommandID)
	if record.CommandID == "" {
		return record, fmt.Errorf("command_id is required")
	}
	return record, nil
}

func (m *Module) raftCommandApplied(commandID string) bool {
	if commandID == "" {
		return false
	}
	m.raftAppliedCommandsMu.Lock()
	defer m.raftAppliedCommandsMu.Unlock()
	_, ok := m.raftAppliedCommands[commandID]
	return ok
}

func (m *Module) rememberRaftAppliedCommand(ctx context.Context, commandID string) error {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return nil
	}
	m.raftAppliedCommandsMu.Lock()
	defer m.raftAppliedCommandsMu.Unlock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	if _, ok := m.raftAppliedCommands[commandID]; ok {
		return nil
	}
	if err := m.appendRaftAppliedCommandLocked(ctx, commandID); err != nil {
		return err
	}
	m.raftAppliedCommands[commandID] = struct{}{}
	return nil
}

func (m *Module) appendRaftAppliedCommandLocked(ctx context.Context, commandID string) error {
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
	payload = append(payload, '\n')
	path := m.raftAppliedCommandsLogPath()
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	rollback := func(writeErr error) error {
		_ = f.Close()
		return errors.Join(writeErr, truncateAndSyncFile(path, info.Size()))
	}
	if written, err := f.Write(payload); err != nil {
		return rollback(err)
	} else if written != len(payload) {
		return rollback(io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		return rollback(err)
	}
	if err := f.Close(); err != nil {
		return errors.Join(err, truncateAndSyncFile(path, info.Size()))
	}
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return errors.Join(err, truncateAndSyncFile(path, info.Size()))
	}
	return nil
}

func appendAndSyncFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	if written, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	} else if written != len(data) {
		_ = f.Close()
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func truncateAndSyncFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (m *Module) persistRaftAppliedCommands(ctx context.Context) error {
	m.raftAppliedCommandsMu.Lock()
	defer m.raftAppliedCommandsMu.Unlock()
	return m.persistRaftAppliedCommandsLocked(ctx)
}

func (m *Module) persistRaftAppliedCommandsLocked(ctx context.Context) error {
	if m.dataDir == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	ids := make([]string, 0, len(m.raftAppliedCommands))
	for id := range m.raftAppliedCommands {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	payload, err := json.MarshalIndent(ids, "", "  ")
	if err != nil {
		return err
	}
	path := m.raftAppliedCommandsPath()
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	if err := filestore.WriteFileAtomic(path, payload, fsperm.PrivateFile); err != nil {
		return err
	}
	if m.raftAppliedCommandsPersistHook != nil {
		m.raftAppliedCommandsPersistHook("before_log_remove")
	}
	if err := os.Remove(m.raftAppliedCommandsLogPath()); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	if err := syncDirectory(filepath.Dir(m.raftAppliedCommandsLogPath())); err != nil {
		return fmt.Errorf("sync semantic applied-command journal removal: %w", err)
	}
	return nil
}

func (m *Module) raftAppliedCommandsPath() string {
	return filepath.Join(m.dataDir, "meta", "semantic", "raft-applied-commands.json")
}

func (m *Module) raftAppliedCommandsLogPath() string {
	return filepath.Join(m.dataDir, "meta", "semantic", "raft-applied-commands-000001.ksem")
}
