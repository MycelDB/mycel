package consensus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/diagnostics"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type PersistentStorage struct {
	mu            sync.Mutex
	dir           string
	memory        raftStorage
	entries       []raftpb.Entry
	confState     raftpb.ConfState
	diagGroup     GroupID
	diagNode      NodeID
	diagSubsystem string
}

type raftStorage interface {
	InitialState() (raftpb.HardState, raftpb.ConfState, error)
	Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	Term(i uint64) (uint64, error)
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
	Snapshot() (raftpb.Snapshot, error)
	SetHardState(st raftpb.HardState) error
	Append(entries []raftpb.Entry) error
	ApplySnapshot(snap raftpb.Snapshot) error
	CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	Compact(compactIndex uint64) error
}

func NewPersistentStorage(dir string) (*PersistentStorage, error) {
	if dir == "" {
		return nil, fmt.Errorf("raft storage dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	mem := raft.NewMemoryStorage()
	s := &PersistentStorage{dir: dir, memory: mem}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *PersistentStorage) SetDiagnosticsLabels(groupID GroupID, nodeID NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.diagGroup = groupID
	s.diagNode = nodeID
}

func (s *PersistentStorage) SetDiagnosticsSubsystem(subsystem string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.diagSubsystem = subsystem
}

func (s *PersistentStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hs, cs, err := s.memory.InitialState()
	if err != nil {
		return hs, cs, err
	}
	if len(s.confState.Voters) > 0 || len(s.confState.Learners) > 0 || len(s.confState.VotersOutgoing) > 0 || len(s.confState.LearnersNext) > 0 {
		cs = s.confState
	}
	return hs, cs, nil
}
func (s *PersistentStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memory.Entries(lo, hi, maxSize)
}
func (s *PersistentStorage) Term(i uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memory.Term(i)
}
func (s *PersistentStorage) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memory.LastIndex()
}
func (s *PersistentStorage) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memory.FirstIndex()
}
func (s *PersistentStorage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.memory.Snapshot()
}

func (s *PersistentStorage) SetHardState(st raftpb.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.memory.SetHardState(st); err != nil {
		return err
	}
	return writeProtoAtomic(filepath.Join(s.dir, "hard_state.pb"), &st)
}

func (s *PersistentStorage) Append(entries []raftpb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	diag := diagnostics.CommitTimingEnabled()
	var started, memoryStarted, reloadStarted, writeStarted time.Time
	var memoryDuration, reloadDuration, writeDuration time.Duration
	if diag {
		started = time.Now()
		memoryStarted = started
	}
	if err := s.memory.Append(entries); err != nil {
		return err
	}
	if diag {
		memoryDuration = time.Since(memoryStarted)
		reloadStarted = time.Now()
	}
	if err := s.reloadEntriesFromMemory(); err != nil {
		return err
	}
	if diag {
		reloadDuration = time.Since(reloadStarted)
		writeStarted = time.Now()
	}
	writtenBytes, err := writeEntriesAtomicMeasured(filepath.Join(s.dir, "entries.pb"), s.entries)
	if diag {
		writeDuration = time.Since(writeStarted)
	}
	if err != nil {
		return err
	}
	if diag {
		firstIndex, lastIndex := entryIndexRange(s.entries)
		diagnostics.LogCommitTiming("raft storage append persisted",
			"subsystem", s.diagSubsystem,
			"group_id", string(s.diagGroup),
			"local_node_id", uint64(s.diagNode),
			"storage_dir", s.dir,
			"appended_entries", len(entries),
			"appended_record_types", entryRecordTypes(entries),
			"retained_entries", len(s.entries),
			"first_index", firstIndex,
			"last_index", lastIndex,
			"written_bytes", writtenBytes,
			"memory_append_ms", memoryDuration.Milliseconds(),
			"reload_entries_ms", reloadDuration.Milliseconds(),
			"write_entries_ms", writeDuration.Milliseconds(),
			"duration_ms", time.Since(started).Milliseconds())
	}
	return nil
}

func (s *PersistentStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.memory.ApplySnapshot(snap); err != nil {
		return err
	}
	s.entries = nil
	s.confState = snap.Metadata.ConfState
	if err := writeProtoAtomic(filepath.Join(s.dir, "snapshot.pb"), &snap); err != nil {
		return err
	}
	if err := writeProtoAtomic(filepath.Join(s.dir, "conf_state.pb"), &s.confState); err != nil {
		return err
	}
	return writeEntriesAtomic(filepath.Join(s.dir, "entries.pb"), s.entries)
}

func (s *PersistentStorage) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := s.memory.CreateSnapshot(i, cs, data)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	if cs != nil {
		s.confState = *cs
	}
	if err := writeProtoAtomic(filepath.Join(s.dir, "snapshot.pb"), &snap); err != nil {
		return raftpb.Snapshot{}, err
	}
	if err := writeProtoAtomic(filepath.Join(s.dir, "conf_state.pb"), &s.confState); err != nil {
		return raftpb.Snapshot{}, err
	}
	return snap, nil
}

func (s *PersistentStorage) Compact(compactIndex uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.memory.Compact(compactIndex); err != nil {
		return err
	}
	if err := s.reloadEntriesFromMemory(); err != nil {
		return err
	}
	return writeEntriesAtomic(filepath.Join(s.dir, "entries.pb"), s.entries)
}

func (s *PersistentStorage) SetConfState(cs raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confState = cs
	return writeProtoAtomic(filepath.Join(s.dir, "conf_state.pb"), &s.confState)
}

func (s *PersistentStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range []string{"hard_state.pb", "entries.pb", "conf_state.pb", "snapshot.pb"} {
		path := filepath.Join(s.dir, name)
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *PersistentStorage) load() error {
	if data, err := os.ReadFile(filepath.Join(s.dir, "conf_state.pb")); err == nil && len(data) > 0 {
		if err := s.confState.Unmarshal(data); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if data, err := os.ReadFile(filepath.Join(s.dir, "snapshot.pb")); err == nil && len(data) > 0 {
		var snap raftpb.Snapshot
		if err := snap.Unmarshal(data); err != nil {
			return err
		}
		if err := s.memory.ApplySnapshot(snap); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if data, err := os.ReadFile(filepath.Join(s.dir, "hard_state.pb")); err == nil && len(data) > 0 {
		var st raftpb.HardState
		if err := st.Unmarshal(data); err != nil {
			return err
		}
		if err := s.memory.SetHardState(st); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	entries, err := readEntries(filepath.Join(s.dir, "entries.pb"))
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		if err := s.memory.Append(entries); err != nil {
			return err
		}
		s.entries = entries
	}
	return nil
}

func entryIndexRange(entries []raftpb.Entry) (uint64, uint64) {
	if len(entries) == 0 {
		return 0, 0
	}
	return entries[0].Index, entries[len(entries)-1].Index
}

func entryRecordTypes(entries []raftpb.Entry) []string {
	if len(entries) == 0 {
		return nil
	}
	out := []string{}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		recordType := ""
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				recordType = "entry_normal_empty"
			} else if cmd, err := DecodeCommand(entry.Data); err == nil {
				recordType = string(cmd.RecordType)
			} else {
				recordType = "entry_normal_decode_error"
			}
		case raftpb.EntryConfChange:
			recordType = "entry_conf_change"
		case raftpb.EntryConfChangeV2:
			recordType = "entry_conf_change_v2"
		default:
			recordType = entry.Type.String()
		}
		if recordType == "" {
			continue
		}
		if _, ok := seen[recordType]; ok {
			continue
		}
		seen[recordType] = struct{}{}
		out = append(out, recordType)
	}
	return out
}

func (s *PersistentStorage) reloadEntriesFromMemory() error {
	first, err := s.memory.FirstIndex()
	if err != nil {
		return err
	}
	last, err := s.memory.LastIndex()
	if err != nil {
		return err
	}
	if last < first {
		s.entries = nil
		return nil
	}
	entries, err := s.memory.Entries(first, last+1, ^uint64(0))
	if err != nil {
		return err
	}
	s.entries = append([]raftpb.Entry(nil), entries...)
	return nil
}

func writeProtoAtomic(path string, msg interface{ Marshal() ([]byte, error) }) error {
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	return writeAtomic(path, data)
}
func writeEntriesAtomic(path string, entries []raftpb.Entry) error {
	_, err := writeEntriesAtomicMeasured(path, entries)
	return err
}

func writeEntriesAtomicMeasured(path string, entries []raftpb.Entry) (int, error) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint64(len(entries))); err != nil {
		return 0, err
	}
	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return 0, err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint64(len(data))); err != nil {
			return 0, err
		}
		buf.Write(data)
	}
	if err := writeAtomic(path, buf.Bytes()); err != nil {
		return 0, err
	}
	return buf.Len(), nil
}
func readEntries(path string) ([]raftpb.Entry, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	r := bytes.NewReader(data)
	var count uint64
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return nil, err
	}
	entries := make([]raftpb.Entry, 0, count)
	for i := uint64(0); i < count; i++ {
		var n uint64
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		var ent raftpb.Entry
		if err := ent.Unmarshal(payload); err != nil {
			return nil, err
		}
		entries = append(entries, ent)
	}
	return entries, nil
}
func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
