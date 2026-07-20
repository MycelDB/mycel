package space

import (
	"bytes"
	"fmt"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

func (m *Module) raftCreateResult(commandID string) (CreateSpaceResult, bool) {
	m.raftMu.Lock()
	defer m.raftMu.Unlock()
	if m.raftCreateByID == nil {
		return CreateSpaceResult{}, false
	}
	result, ok := m.raftCreateByID[commandID]
	return result, ok
}

func (m *Module) raftAppliedCreate(cmd consensus.RaftCommand) (CreateSpaceResult, bool, error) {
	m.raftMu.Lock()
	defer m.raftMu.Unlock()
	if m.raftCreateByID == nil {
		return CreateSpaceResult{}, false, nil
	}
	result, ok := m.raftCreateByID[cmd.CommandID]
	if !ok {
		return CreateSpaceResult{}, false, nil
	}
	if !bytes.Equal(m.raftHashByID[cmd.CommandID], cmd.CommandHash) {
		return CreateSpaceResult{}, true, fmt.Errorf("command_id %q was already applied with different payload", cmd.CommandID)
	}
	return result, true, nil
}

func (m *Module) rememberRaftCreate(cmd consensus.RaftCommand, result CreateSpaceResult) {
	m.raftMu.Lock()
	defer m.raftMu.Unlock()
	if m.raftCreateByID == nil {
		m.raftCreateByID = map[string]CreateSpaceResult{}
		m.raftHashByID = map[string][]byte{}
	}
	m.raftCreateByID[cmd.CommandID] = result
	m.raftHashByID[cmd.CommandID] = append([]byte(nil), cmd.CommandHash...)
}
