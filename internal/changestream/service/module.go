package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
)

const (
	defaultHistoryLimit     = 1024
	defaultSubscriberBuffer = 64
)

type Module struct {
	mu           sync.Mutex
	dataDir      string
	historyLimit int
	history      map[string][]Event
	current      map[string]int64
	loaded       map[string]bool
	subscribers  map[string]map[string]chan Event
	gate         *quiesce.Gate
	observers    []func(context.Context, Event)
}

func NewModule() *Module {
	return &Module{historyLimit: defaultHistoryLimit, history: map[string][]Event{}, current: map[string]int64{}, loaded: map[string]bool{}, subscribers: map[string]map[string]chan Event{}, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	if m.historyLimit <= 0 {
		m.historyLimit = defaultHistoryLimit
	}
	if m.history == nil {
		m.history = map[string][]Event{}
	}
	if m.current == nil {
		m.current = map[string]int64{}
	}
	if m.loaded == nil {
		m.loaded = map[string]bool{}
	}
	if m.subscribers == nil {
		m.subscribers = map[string]map[string]chan Event{}
	}
	m.dataDir = filepath.Join(host.DataDir(), "change-stream")
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if registrar, ok := host.(runtime.QuiesceRegistrar); ok {
		if err := registrar.RegisterQuiesceParticipant(m.gate); err != nil {
			return runtime.Abort(ModuleName, "quiesce", "register change-stream quiesce participant", err)
		}
	}
	if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
		return runtime.Abort(ModuleName, "storage", "create change stream data directory", err)
	}
	if logger := host.Log(); logger != nil {
		logger.Info("change stream module initialized", "storage", "file", "path", m.dataDir, "history_limit", m.historyLimit)
	}
	return runtime.OK(ModuleName)
}

func (m *Module) CurrentRevision(spaceID string, domainID string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := streamKey(spaceID, domainID)
	_ = m.loadStreamLocked(key, spaceID, domainID)
	return m.current[key]
}

func (m *Module) Subscribe(ctx context.Context, input SubscribeInput) (*Subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	spaceID := strings.TrimSpace(input.SpaceID)
	domainID := strings.TrimSpace(input.DomainID)
	if spaceID == "" || domainID == "" {
		return nil, fmt.Errorf("%w: space_id and domain_id are required", ErrInvalidInput)
	}
	key := streamKey(spaceID, domainID)
	id := uuid.NewString()
	m.mu.Lock()
	if err := m.loadStreamLocked(key, spaceID, domainID); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	replay := []Event{}
	if input.AfterRevision != nil {
		oldest := int64(0)
		if events := m.history[key]; len(events) > 0 {
			oldest = events[0].Revision
		}
		if oldest > 0 && *input.AfterRevision < oldest-1 {
			m.mu.Unlock()
			return nil, ErrOutOfRange
		}
		for _, event := range m.history[key] {
			if event.Revision > *input.AfterRevision {
				replay = append(replay, cloneEvent(event))
			}
		}
	}
	ch := make(chan Event, defaultSubscriberBuffer+len(replay))
	for _, event := range replay {
		ch <- event
	}
	if m.subscribers[key] == nil {
		m.subscribers[key] = map[string]chan Event{}
	}
	m.subscribers[key][id] = ch
	m.mu.Unlock()
	cancel := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if subs := m.subscribers[key]; subs != nil {
			delete(subs, id)
			if len(subs) == 0 {
				delete(m.subscribers, key)
			}
		}
	}
	return &Subscription{Events: ch, Cancel: cancel}, nil
}

func (m *Module) AddObserver(observer func(context.Context, Event)) {
	if observer == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observers = append(m.observers, observer)
}

func (m *Module) PublishCommit(ctx context.Context, commit daemonsession.TransactionCommit, changes []GraphChange) {
	release, err := m.enterWrite(ctx)
	if err != nil {
		return
	}
	defer release()
	if ctx.Err() != nil || strings.TrimSpace(commit.SpaceID) == "" || strings.TrimSpace(commit.DomainID) == "" || commit.CommittedRevision <= 0 {
		return
	}
	changes = cloneGraphChanges(changes)
	changes = append(changes, GraphChange{Type: ChangeTypeRevisionAdvanced})
	event := Event{EventID: uuid.NewString(), SpaceID: commit.SpaceID, DomainID: commit.DomainID, Revision: commit.CommittedRevision, CommitID: commit.ID, EventTime: commit.CommittedAt, Changes: changes}
	if event.EventTime.IsZero() {
		event.EventTime = time.Now().UTC()
	}
	key := streamKey(commit.SpaceID, commit.DomainID)
	m.mu.Lock()
	if err := m.loadStreamLocked(key, commit.SpaceID, commit.DomainID); err != nil {
		m.mu.Unlock()
		return
	}
	m.current[key] = commit.CommittedRevision
	m.history[key] = append(m.history[key], cloneEvent(event))
	if len(m.history[key]) > m.historyLimit {
		m.history[key] = append([]Event(nil), m.history[key][len(m.history[key])-m.historyLimit:]...)
	}
	_ = m.persistStreamLocked(key, commit.SpaceID, commit.DomainID)
	subs := make([]chan Event, 0, len(m.subscribers[key]))
	for _, ch := range m.subscribers[key] {
		subs = append(subs, ch)
	}
	observers := append([]func(context.Context, Event){}, m.observers...)
	m.mu.Unlock()
	for _, observer := range observers {
		observer(ctx, cloneEvent(event))
	}
	for _, ch := range subs {
		safeOfferEvent(ch, cloneEvent(event))
	}
}

func (m *Module) enterWrite(ctx context.Context) (func(), error) {
	if m.gate == nil {
		return func() {}, nil
	}
	return m.gate.Enter(ctx)
}

func safeOfferEvent(ch chan Event, event Event) {
	defer func() { _ = recover() }()
	select {
	case ch <- event:
	default:
		// Avoid blocking commit paths under slow subscribers. Slow consumers can resume from durable history.
	}
}

func (m *Module) loadStreamLocked(key string, spaceID string, domainID string) error {
	if m.loaded[key] {
		return nil
	}
	m.loaded[key] = true
	path := m.eventLogPath(spaceID, domainID)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 16*1024*1024)
	events := []Event{}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		events = append(events, cloneEvent(event))
		if event.Revision > m.current[key] {
			m.current[key] = event.Revision
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(events) > m.historyLimit {
		events = append([]Event(nil), events[len(events)-m.historyLimit:]...)
	}
	m.history[key] = events
	return nil
}

func (m *Module) persistStreamLocked(key string, spaceID string, domainID string) error {
	path := m.eventLogPath(spaceID, domainID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, event := range m.history[key] {
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return m.persistCheckpointLocked(key, spaceID, domainID)
}

func (m *Module) persistCheckpointLocked(key string, spaceID string, domainID string) error {
	path := m.checkpointPath(spaceID, domainID)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	payload := struct {
		SpaceID         string    `json:"space_id"`
		DomainID        string    `json:"domain_id"`
		CurrentRevision int64     `json:"current_revision"`
		CheckpointTime  time.Time `json:"checkpoint_time"`
	}{SpaceID: spaceID, DomainID: domainID, CurrentRevision: m.current[key], CheckpointTime: time.Now().UTC()}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o600)
}

func (m *Module) eventLogPath(spaceID string, domainID string) string {
	return filepath.Join(m.dataDir, safeSegment(spaceID), safeSegment(domainID)+".jsonl")
}

func (m *Module) checkpointPath(spaceID string, domainID string) string {
	return filepath.Join(m.dataDir, safeSegment(spaceID), safeSegment(domainID)+".checkpoint.json")
}

func safeSegment(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, "\\", "_")
	value = strings.ReplaceAll(value, ":", "_")
	if value == "" {
		return "_"
	}
	return value
}

func streamKey(spaceID string, domainID string) string {
	return strings.TrimSpace(spaceID) + ":" + strings.TrimSpace(domainID)
}

func cloneEvent(in Event) Event {
	out := in
	out.Changes = cloneGraphChanges(in.Changes)
	return out
}

func cloneGraphChanges(in []GraphChange) []GraphChange {
	out := make([]GraphChange, 0, len(in))
	for _, change := range in {
		copy := change
		if change.Node != nil {
			node := *change.Node
			if change.Node.Props != nil {
				node.Props = cloneMap(change.Node.Props)
			}
			copy.Node = &node
		}
		if change.Edge != nil {
			edge := *change.Edge
			edge.Labels = append([]string(nil), change.Edge.Labels...)
			edge.Properties = cloneMap(change.Edge.Properties)
			edge.Payload = cloneMap(change.Edge.Payload)
			edge.Meta = cloneMap(change.Edge.Meta)
			copy.Edge = &edge
		}
		out = append(out, copy)
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
