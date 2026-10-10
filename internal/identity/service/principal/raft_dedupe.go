package principal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/fsperm"
)

const (
	raftCommandStatusApplied           = "applied"
	raftCommandStatusApplicationError  = "application_error"
	raftCommandErrorDuplicatePrincipal = "duplicate_principal"
)

type RaftAppliedCommand struct {
	RaftIndex    uint64 `json:"raft_index,omitempty"`
	RaftTerm     uint64 `json:"raft_term,omitempty"`
	CommandID    string `json:"command_id"`
	CommandHash  string `json:"command_hash,omitempty"`
	RecordType   string `json:"record_type,omitempty"`
	Status       string `json:"status"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	AppliedAt    string `json:"applied_at,omitempty"`
}

func (m *Module) loadRaftAppliedCommands() {
	if m.dataDir == "" {
		return
	}
	data, err := os.ReadFile(m.raftAppliedCommandsPath())
	if err != nil {
		return
	}
	records := decodeRaftAppliedCommands(data)
	if len(records) == 0 {
		return
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]RaftAppliedCommand{}
	}
	for _, rec := range records {
		if rec.CommandID != "" {
			m.raftAppliedCommands[rec.CommandID] = rec
		}
	}
}

func decodeRaftAppliedCommands(data []byte) []RaftAppliedCommand {
	var records []RaftAppliedCommand
	if json.Unmarshal(data, &records) == nil {
		out := make([]RaftAppliedCommand, 0, len(records))
		for _, rec := range records {
			if rec.CommandID == "" {
				continue
			}
			if rec.Status == "" {
				rec.Status = raftCommandStatusApplied
			}
			out = append(out, rec)
		}
		return out
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil {
		return nil
	}
	out := make([]RaftAppliedCommand, 0, len(ids))
	for _, id := range ids {
		if id != "" {
			out = append(out, RaftAppliedCommand{CommandID: id, Status: raftCommandStatusApplied})
		}
	}
	return out
}

func (m *Module) raftAppliedCommandRecord(commandID string) (RaftAppliedCommand, bool) {
	if commandID == "" {
		return RaftAppliedCommand{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.raftAppliedCommands[commandID]
	return rec, ok
}

func (m *Module) rememberRaftCommandOutcome(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, status string, applyErr error) error {
	if cmd.CommandID == "" {
		return nil
	}
	if status == "" {
		status = raftCommandStatusApplied
	}
	rec := RaftAppliedCommand{
		RaftIndex:   apply.RaftIndex,
		RaftTerm:    apply.RaftTerm,
		CommandID:   cmd.CommandID,
		CommandHash: raftCommandHash(cmd),
		RecordType:  string(cmd.RecordType),
		Status:      status,
		AppliedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if applyErr != nil {
		rec.ErrorCode = raftApplicationErrorCode(applyErr)
		rec.ErrorMessage = applyErr.Error()
	}
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]RaftAppliedCommand{}
	}
	m.raftAppliedCommands[cmd.CommandID] = rec
	records := m.raftAppliedCommandRecordsLocked()
	m.mu.Unlock()
	return m.persistRaftAppliedCommands(ctx, records)
}

func (m *Module) raftAppliedCommandRecordsLocked() []RaftAppliedCommand {
	records := make([]RaftAppliedCommand, 0, len(m.raftAppliedCommands))
	for _, rec := range m.raftAppliedCommands {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].CommandID < records[j].CommandID })
	return records
}

func (m *Module) raftAppliedCommandRecords() []RaftAppliedCommand {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.raftAppliedCommandRecordsLocked()
}

func (m *Module) persistRaftAppliedCommands(ctx context.Context, records []RaftAppliedCommand) error {
	if m.dataDir == "" {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	payload, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return err
	}
	path := m.raftAppliedCommandsPath()
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, fsperm.PrivateFile); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func raftCommandHash(cmd consensus.RaftCommand) string {
	copyCmd := cmd
	copyCmd.Payload = append([]byte(nil), cmd.Payload...)
	data, _ := json.Marshal(copyCmd)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func verifyRaftCommandHash(rec RaftAppliedCommand, cmd consensus.RaftCommand) error {
	if rec.CommandHash == "" {
		return nil
	}
	actual := raftCommandHash(cmd)
	if rec.CommandHash != actual {
		return fmt.Errorf("raft command id %q was already applied with a different command hash", cmd.CommandID)
	}
	return nil
}

func isRecordableRaftApplicationError(err error) bool {
	return errors.Is(err, ErrDuplicatePrincipal)
}

func raftApplicationErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrDuplicatePrincipal):
		return raftCommandErrorDuplicatePrincipal
	default:
		return "application_error"
	}
}

func (m *Module) raftAppliedCommandsPath() string {
	return filepath.Join(m.dataDir, "identity", "raft-applied-commands.json")
}
