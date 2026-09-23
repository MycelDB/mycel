package consensus

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/fsperm"

	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type PersistentStorage struct {
	mu         sync.Mutex
	dir        string
	groupID    GroupID
	encryption *encryption.Service
	memory     raftStorage
	entries    []raftpb.Entry
	confState  raftpb.ConfState
}

// PersistentStorageOptions configures file-backed Raft storage.
type PersistentStorageOptions struct {
	Dir        string
	GroupID    GroupID
	Encryption *encryption.Service
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
	return NewPersistentStorageWithOptions(PersistentStorageOptions{Dir: dir, GroupID: GroupID(filepath.Base(dir))})
}

func NewPersistentStorageWithOptions(opts PersistentStorageOptions) (*PersistentStorage, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("raft storage dir is required")
	}
	if opts.GroupID == "" {
		opts.GroupID = GroupID(filepath.Base(opts.Dir))
	}
	if err := os.MkdirAll(opts.Dir, fsperm.SharedDir); err != nil {
		return nil, err
	}
	mem := raft.NewMemoryStorage()
	s := &PersistentStorage{dir: opts.Dir, groupID: opts.GroupID, encryption: opts.Encryption, memory: mem}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
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
	return s.writeProtoAtomic("hard_state.pb", &st)
}

func (s *PersistentStorage) Append(entries []raftpb.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	appendOnly := s.canAppendEntriesLog(entries)
	if err := s.memory.Append(entries); err != nil {
		return err
	}
	if appendOnly {
		if err := s.appendEntriesLog("entries.log", entries); err != nil {
			return err
		}
		s.entries = append(s.entries, entries...)
		return nil
	}
	if err := s.reloadEntriesFromMemory(); err != nil {
		return err
	}
	return s.writeEntries("entries.pb", "entries.log", s.entries)
}

func (s *PersistentStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.memory.ApplySnapshot(snap); err != nil {
		return err
	}
	s.entries = nil
	s.confState = snap.Metadata.ConfState
	if err := s.writeProtoAtomic("snapshot.pb", &snap); err != nil {
		return err
	}
	if err := s.writeProtoAtomic("conf_state.pb", &s.confState); err != nil {
		return err
	}
	return s.writeEntries("entries.pb", "entries.log", s.entries)
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
	if err := s.writeProtoAtomic("snapshot.pb", &snap); err != nil {
		return raftpb.Snapshot{}, err
	}
	if err := s.writeProtoAtomic("conf_state.pb", &s.confState); err != nil {
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
	return s.writeEntries("entries.pb", "entries.log", s.entries)
}

func (s *PersistentStorage) SetConfState(cs raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confState = cs
	return s.writeProtoAtomic("conf_state.pb", &s.confState)
}

func (s *PersistentStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, name := range []string{"hard_state.pb", "entries.pb", "entries.log", "conf_state.pb", "snapshot.pb"} {
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
	if data, err := s.readFile("conf_state.pb"); err == nil && len(data) > 0 {
		if err := s.confState.Unmarshal(data); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if data, err := s.readFile("snapshot.pb"); err == nil && len(data) > 0 {
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
	if data, err := s.readFile("hard_state.pb"); err == nil && len(data) > 0 {
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
	entries, err := s.readEntriesLog("entries.log")
	if os.IsNotExist(err) {
		entries, err = s.readEntries("entries.pb")
	}
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

func (s *PersistentStorage) writeProtoAtomic(name string, msg interface{ Marshal() ([]byte, error) }) error {
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	return s.writeFileAtomic(name, data)
}

func (s *PersistentStorage) writeEntriesAtomic(name string, entries []raftpb.Entry) error {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.BigEndian, uint64(len(entries))); err != nil {
		return err
	}
	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		if err := binary.Write(&buf, binary.BigEndian, uint64(len(data))); err != nil {
			return err
		}
		buf.Write(data)
	}
	return s.writeFileAtomic(name, buf.Bytes())
}

func (s *PersistentStorage) canAppendEntriesLog(entries []raftpb.Entry) bool {
	if len(entries) == 0 {
		return false
	}
	if len(s.entries) == 0 {
		return true
	}
	if _, err := os.Stat(filepath.Join(s.dir, "entries.log")); err != nil {
		return false
	}
	return entries[0].Index == s.entries[len(s.entries)-1].Index+1
}

func (s *PersistentStorage) writeEntries(legacyName, logName string, entries []raftpb.Entry) error {
	return s.writeEntriesLogAtomic(logName, entries)
}

func (s *PersistentStorage) encryptionEnabled() bool {
	return s.encryption != nil && s.encryption.Enabled()
}

var entriesLogMagic = []byte("mycel-raft-entries-v2\n")

func (s *PersistentStorage) writeEntriesLogAtomic(name string, entries []raftpb.Entry) error {
	var buf bytes.Buffer
	buf.Write(entriesLogMagic)
	if err := s.encodeEntriesLogFrames(&buf, name, entries, 0); err != nil {
		return err
	}
	path := filepath.Join(s.dir, name)
	return writeAtomic(path, buf.Bytes())
}

func (s *PersistentStorage) appendEntriesLog(name string, entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	path := filepath.Join(s.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, fsperm.SharedFile)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if _, err := f.Write(entriesLogMagic); err != nil {
			return err
		}
	}
	return s.encodeEntriesLogFrames(f, name, entries, len(s.entries))
}

func (s *PersistentStorage) encodeEntriesLogFrames(w io.Writer, name string, entries []raftpb.Entry, ordinalOffset int) error {
	var header [12]byte
	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		if s.encryptionEnabled() {
			data, err = s.encryption.EncryptRecord(context.Background(), data, s.entriesLogFrameAAD(name, ordinalOffset+i))
			if err != nil {
				return err
			}
		}
		binary.BigEndian.PutUint64(header[:8], uint64(len(data)))
		binary.BigEndian.PutUint32(header[8:], crc32.ChecksumIEEE(data))
		if _, err := w.Write(header[:]); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *PersistentStorage) entriesLogFrameAAD(name string, ordinal int) []byte {
	return []byte(fmt.Sprintf("raft-storage:v2:group=%s:file=%s:ordinal=%d", s.groupID, name, ordinal))
}

func (s *PersistentStorage) readEntriesLog(name string) ([]raftpb.Entry, error) {
	path := filepath.Join(s.dir, name)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	magic := make([]byte, len(entriesLogMagic))
	if _, err := io.ReadFull(f, magic); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, nil
		}
		return nil, err
	}
	if !bytes.Equal(magic, entriesLogMagic) {
		return nil, fmt.Errorf("invalid raft entries log header")
	}
	entries := []raftpb.Entry{}
	lastGood := int64(len(entriesLogMagic))
	ordinal := 0
	var header [12]byte
	for {
		n, err := io.ReadFull(f, header[:])
		if err == io.EOF || (err == io.ErrUnexpectedEOF && n == 0) {
			break
		}
		if err != nil {
			_ = os.Truncate(path, lastGood)
			break
		}
		nbytes := binary.BigEndian.Uint64(header[:8])
		wantCRC := binary.BigEndian.Uint32(header[8:])
		payload := make([]byte, nbytes)
		if _, err := io.ReadFull(f, payload); err != nil {
			_ = os.Truncate(path, lastGood)
			break
		}
		if got := crc32.ChecksumIEEE(payload); got != wantCRC {
			return nil, fmt.Errorf("raft entries log checksum mismatch at offset %d", lastGood)
		}
		if s.encryptionEnabled() {
			var err error
			payload, err = s.encryption.DecryptRecord(context.Background(), payload, s.entriesLogFrameAAD(name, ordinal))
			if err != nil {
				return nil, err
			}
		}
		var ent raftpb.Entry
		if err := ent.Unmarshal(payload); err != nil {
			return nil, err
		}
		entries = append(entries, ent)
		lastGood += int64(len(header)) + int64(nbytes)
		ordinal++
	}
	return entries, nil
}

func (s *PersistentStorage) readEntries(name string) ([]raftpb.Entry, error) {
	data, err := s.readFile(name)
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
func (s *PersistentStorage) readFile(name string) ([]byte, error) {
	path := filepath.Join(s.dir, name)
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return data, err
	}
	if s.encryption == nil {
		return data, nil
	}
	return s.encryption.DecryptRecord(context.Background(), data, s.fileAAD(name))
}

func (s *PersistentStorage) writeFileAtomic(name string, data []byte) error {
	path := filepath.Join(s.dir, name)
	if s.encryption != nil && s.encryption.Enabled() {
		var err error
		data, err = s.encryption.EncryptRecord(context.Background(), data, s.fileAAD(name))
		if err != nil {
			return err
		}
	}
	return writeAtomic(path, data)
}

func (s *PersistentStorage) fileAAD(name string) []byte {
	return []byte(fmt.Sprintf("raft-storage:v1:group=%s:file=%s", s.groupID, name))
}

func writeAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, fsperm.SharedFile); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
