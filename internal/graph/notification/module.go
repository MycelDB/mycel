package notification

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/runtime"
)

const (
	ModuleName                = "graph_change_notification"
	DefaultRetentionMaxEvents = 10_000
	DefaultRetentionMaxAge    = 24 * time.Hour
	defaultDeliveryBuffer     = 64
)

var (
	ErrInvalidInput = errors.New("invalid graph-change notification input")
	ErrOutOfRange   = errors.New("graph-change notification resume revision is no longer available")
)

type Registrar interface {
	RegisterConsumer(ctx context.Context, spec ConsumerSpec, consumer Consumer) (Registration, error)
}

type Manager interface {
	Registrar
	CurrentRevision(ctx context.Context, spaceID string, domainID string) (uint64, error)
}

type Registration interface {
	Close() error
}

type Consumer interface {
	HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error
	HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error
}

type ConsumerSpec struct {
	ConsumerName string
	Scope        graphchange.Scope
	Filter       graphchange.Filter
	Projection   graphchange.Projection
	Start        StartPosition
}

type StartPosition struct {
	AfterRevision *uint64
}

type Diagnostics struct {
	Registrations   int
	EventsPublished uint64
	EventsDelivered uint64
	EventsDropped   uint64
	HandlerFailures uint64
	HandlerPanics   uint64
	LastFailure     string
	LastFailureAt   time.Time
}

type LeaderGate func(context.Context, graphchange.CommittedEvent) error

type Module struct {
	mu             sync.Mutex
	dataDir        string
	retentionCount int
	retentionAge   time.Duration
	leaderGate     LeaderGate
	history        map[string][]graphchange.CommittedEvent
	loaded         map[string]bool
	current        map[string]uint64
	registrations  map[string]*registration
	diagnostics    Diagnostics
}

type registration struct {
	module   *Module
	id       string
	spec     ConsumerSpec
	consumer Consumer
	ch       chan graphchange.CommittedEvent
	done     chan struct{}
	once     sync.Once
}

func NewModule() *Module {
	return &Module{
		retentionCount: DefaultRetentionMaxEvents,
		retentionAge:   DefaultRetentionMaxAge,
		history:        map[string][]graphchange.CommittedEvent{},
		loaded:         map[string]bool{},
		current:        map[string]uint64{},
		registrations:  map[string]*registration{},
	}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) WithLeaderGate(gate LeaderGate) *Module {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.leaderGate = gate
	return m
}

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	if err := ctx.Err(); err != nil {
		return runtime.Abort(ModuleName, "context", "initialize graph-change notification", err)
	}
	if m.retentionCount <= 0 {
		m.retentionCount = DefaultRetentionMaxEvents
	}
	if m.retentionAge <= 0 {
		m.retentionAge = DefaultRetentionMaxAge
	}
	if m.history == nil {
		m.history = map[string][]graphchange.CommittedEvent{}
	}
	if m.loaded == nil {
		m.loaded = map[string]bool{}
	}
	if m.current == nil {
		m.current = map[string]uint64{}
	}
	if m.registrations == nil {
		m.registrations = map[string]*registration{}
	}
	m.dataDir = filepath.Join(host.DataDir(), "graph-change-notification")
	if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
		return runtime.Abort(ModuleName, "storage", "create graph-change notification data directory", err)
	}
	if logger := host.Log(); logger != nil {
		logger.Info("graph-change notification module initialized", "storage", "file", "path", m.dataDir, "retention_events", m.retentionCount, "retention_age", m.retentionAge)
	}
	return runtime.OK(ModuleName)
}

func (m *Module) SetDataDirForTest(dataDir string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dataDir = dataDir
}

func (m *Module) SetRetentionForTest(maxEvents int, maxAge time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retentionCount = maxEvents
	m.retentionAge = maxAge
}

func (m *Module) Diagnostics() Diagnostics {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.diagnostics
	out.Registrations = len(m.registrations)
	return out
}

func (m *Module) CurrentRevision(ctx context.Context, spaceID string, domainID string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	key := scopeKey(spaceID, domainID)
	if key == "" {
		return 0, fmt.Errorf("%w: space_id and domain_id are required", ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dataDir == "" {
		m.dataDir = filepath.Join(os.TempDir(), "mycel-graph-change-notification")
	}
	if err := m.loadHistoryLocked(key, spaceID, domainID); err != nil {
		return 0, err
	}
	return m.current[key], nil
}

func (m *Module) RegisterConsumer(ctx context.Context, spec ConsumerSpec, consumer Consumer) (Registration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if consumer == nil {
		return nil, fmt.Errorf("%w: consumer is required", ErrInvalidInput)
	}
	spec.ConsumerName = strings.TrimSpace(spec.ConsumerName)
	if spec.ConsumerName == "" {
		return nil, fmt.Errorf("%w: consumer_name is required", ErrInvalidInput)
	}
	key := scopeKey(spec.Scope.SpaceID, spec.Scope.DomainID)
	if key == "" {
		return nil, fmt.Errorf("%w: scope space_id and domain_id are required", ErrInvalidInput)
	}
	id := uuid.NewString()
	reg := &registration{module: m, id: id, spec: spec, consumer: consumer, ch: make(chan graphchange.CommittedEvent, defaultDeliveryBuffer), done: make(chan struct{})}

	m.mu.Lock()
	if err := m.loadHistoryLocked(key, spec.Scope.SpaceID, spec.Scope.DomainID); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	replay := []graphchange.CommittedEvent{}
	var gap *graphchange.Gap
	if spec.Start.AfterRevision != nil {
		oldest := uint64(0)
		if events := m.history[key]; len(events) > 0 {
			oldest = eventRevision(events[0])
		}
		if oldest > 0 && *spec.Start.AfterRevision < oldest-1 {
			value := graphchange.Gap{SpaceID: spec.Scope.SpaceID, DomainID: spec.Scope.DomainID, RequestedAfterRevision: *spec.Start.AfterRevision, OldestAvailableRevision: oldest, CurrentRevision: m.current[key]}
			gap = &value
		} else {
			for _, event := range m.history[key] {
				if eventRevision(event) > *spec.Start.AfterRevision && matchesSpec(event, spec) {
					replay = append(replay, event.ApplyProjection(spec.Projection))
				}
			}
		}
	}
	m.registrations[id] = reg
	m.mu.Unlock()

	go reg.run()
	if gap != nil {
		go reg.deliverGap(*gap)
	} else {
		for _, event := range replay {
			reg.offer(event)
		}
	}
	return reg, nil
}

func (m *Module) OnGraphCommitted(ctx context.Context, event graphchange.CommittedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event.ID == uuid.Nil {
		event.ID = uuid.New()
	}
	event.Normalize()
	if event.Empty() {
		return nil
	}
	m.mu.Lock()
	leaderGate := m.leaderGate
	m.mu.Unlock()
	if leaderGate != nil {
		if err := leaderGate(ctx, event); err != nil {
			m.mu.Lock()
			m.recordFailureLocked(err)
			m.mu.Unlock()
			return err
		}
	}
	if event.CommittedAt.IsZero() {
		event.CommittedAt = time.Now().UTC()
	}
	spaceID := event.SpaceID.String()
	domainIDs := event.DomainIDs
	if event.DomainID != uuid.Nil && !containsDomainID(domainIDs, event.DomainID) {
		domainIDs = append(domainIDs, event.DomainID)
	}
	if len(domainIDs) == 0 {
		return fmt.Errorf("%w: event domain_id is required", ErrInvalidInput)
	}

	m.mu.Lock()
	if m.dataDir == "" {
		m.dataDir = filepath.Join(os.TempDir(), "mycel-graph-change-notification")
	}
	registrations := make([]*registration, 0, len(m.registrations))
	for _, domainID := range domainIDs {
		key := scopeKey(spaceID, domainID.String())
		if err := m.loadHistoryLocked(key, spaceID, domainID.String()); err != nil {
			m.recordFailureLocked(err)
			m.mu.Unlock()
			return err
		}
		m.history[key] = append(m.history[key], event)
		m.current[key] = eventRevision(event)
		m.compactHistoryLocked(key)
		if err := m.persistHistoryLocked(key, spaceID, domainID.String()); err != nil {
			m.recordFailureLocked(err)
			m.mu.Unlock()
			return err
		}
	}
	m.diagnostics.EventsPublished++
	for _, reg := range m.registrations {
		if matchesSpec(event, reg.spec) {
			registrations = append(registrations, reg)
		}
	}
	m.mu.Unlock()

	for _, reg := range registrations {
		reg.offer(event.ApplyProjection(reg.spec.Projection))
	}
	return nil
}

func (r *registration) Close() error {
	r.once.Do(func() {
		r.module.mu.Lock()
		delete(r.module.registrations, r.id)
		r.module.mu.Unlock()
		close(r.done)
	})
	return nil
}

func (r *registration) offer(event graphchange.CommittedEvent) {
	select {
	case <-r.done:
		return
	case r.ch <- event:
		return
	default:
		r.module.mu.Lock()
		r.module.diagnostics.EventsDropped++
		r.module.mu.Unlock()
	}
}

func (r *registration) run() {
	for {
		select {
		case <-r.done:
			return
		case event := <-r.ch:
			r.deliver(event)
		}
	}
}

func (r *registration) deliver(event graphchange.CommittedEvent) {
	defer func() {
		if value := recover(); value != nil {
			r.module.mu.Lock()
			r.module.diagnostics.HandlerPanics++
			r.module.diagnostics.LastFailure = fmt.Sprintf("consumer %s panic: %v", r.spec.ConsumerName, value)
			r.module.diagnostics.LastFailureAt = time.Now().UTC()
			r.module.mu.Unlock()
		}
	}()
	if err := r.consumer.HandleGraphChange(context.Background(), event); err != nil {
		r.module.mu.Lock()
		r.module.diagnostics.HandlerFailures++
		r.module.diagnostics.LastFailure = fmt.Sprintf("consumer %s handler: %v", r.spec.ConsumerName, err)
		r.module.diagnostics.LastFailureAt = time.Now().UTC()
		r.module.mu.Unlock()
		return
	}
	r.module.mu.Lock()
	r.module.diagnostics.EventsDelivered++
	r.module.mu.Unlock()
}

func (r *registration) deliverGap(gap graphchange.Gap) {
	defer func() {
		if value := recover(); value != nil {
			r.module.mu.Lock()
			r.module.diagnostics.HandlerPanics++
			r.module.diagnostics.LastFailure = fmt.Sprintf("consumer %s gap panic: %v", r.spec.ConsumerName, value)
			r.module.diagnostics.LastFailureAt = time.Now().UTC()
			r.module.mu.Unlock()
		}
	}()
	if err := r.consumer.HandleGraphChangeGap(context.Background(), gap); err != nil {
		r.module.mu.Lock()
		r.module.diagnostics.HandlerFailures++
		r.module.diagnostics.LastFailure = fmt.Sprintf("consumer %s gap handler: %v", r.spec.ConsumerName, err)
		r.module.diagnostics.LastFailureAt = time.Now().UTC()
		r.module.mu.Unlock()
	}
}

func (m *Module) recordFailureLocked(err error) {
	m.diagnostics.LastFailure = err.Error()
	m.diagnostics.LastFailureAt = time.Now().UTC()
}

func (m *Module) loadHistoryLocked(key, spaceID, domainID string) error {
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
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event graphchange.CommittedEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return err
		}
		event.Normalize()
		m.history[key] = append(m.history[key], event)
		if rev := eventRevision(event); rev > m.current[key] {
			m.current[key] = rev
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	m.compactHistoryLocked(key)
	return nil
}

func (m *Module) compactHistoryLocked(key string) {
	events := m.history[key]
	if len(events) == 0 {
		return
	}
	if m.retentionAge > 0 {
		cutoff := time.Now().UTC().Add(-m.retentionAge)
		start := 0
		for start < len(events) && !events[start].CommittedAt.IsZero() && events[start].CommittedAt.Before(cutoff) {
			start++
		}
		events = append([]graphchange.CommittedEvent(nil), events[start:]...)
	}
	if m.retentionCount > 0 && len(events) > m.retentionCount {
		events = append([]graphchange.CommittedEvent(nil), events[len(events)-m.retentionCount:]...)
	}
	m.history[key] = events
}

func (m *Module) persistHistoryLocked(key, spaceID, domainID string) error {
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
	return nil
}

func (m *Module) eventLogPath(spaceID, domainID string) string {
	return filepath.Join(m.dataDir, safeSegment(spaceID), safeSegment(domainID)+".jsonl")
}

func matchesSpec(event graphchange.CommittedEvent, spec ConsumerSpec) bool {
	event.Normalize()
	if spec.Scope.SpaceID != "" && event.SpaceID.String() != strings.TrimSpace(spec.Scope.SpaceID) {
		return false
	}
	if spec.Scope.DomainID != "" && !eventHasDomain(event, strings.TrimSpace(spec.Scope.DomainID)) {
		return false
	}
	if len(spec.Scope.NodeIDs) > 0 && !eventHasAnyNode(event, spec.Scope.NodeIDs) {
		return false
	}
	if len(spec.Scope.EdgeIDs) > 0 && !eventHasAnyEdge(event, spec.Scope.EdgeIDs) {
		return false
	}
	if len(spec.Filter.EventTypes) > 0 && !eventHasAnyType(event, spec.Filter.EventTypes) {
		return false
	}
	if len(spec.Filter.Labels) > 0 && !eventHasAnyLabel(event, spec.Filter.Labels) {
		return false
	}
	if len(spec.Filter.Fields) > 0 && !eventHasAnyField(event, spec.Filter.Fields) {
		return false
	}
	return true
}

func eventHasDomain(event graphchange.CommittedEvent, domainID string) bool {
	if event.DomainID.String() == domainID {
		return true
	}
	for _, id := range event.DomainIDs {
		if id.String() == domainID {
			return true
		}
	}
	return false
}

func eventHasAnyNode(event graphchange.CommittedEvent, nodeIDs []string) bool {
	want := stringSet(nodeIDs)
	for _, id := range event.AffectedNodeIDs {
		if want[id.String()] {
			return true
		}
	}
	for _, change := range event.Changes {
		if want[change.NodeID] {
			return true
		}
		for _, id := range change.AffectedNodeIDs {
			if want[id] {
				return true
			}
		}
	}
	return false
}

func eventHasAnyEdge(event graphchange.CommittedEvent, edgeIDs []string) bool {
	want := stringSet(edgeIDs)
	for _, id := range event.AffectedEdgeIDs {
		if want[id.String()] {
			return true
		}
	}
	for _, change := range event.Changes {
		if want[change.EdgeID] {
			return true
		}
		for _, id := range change.AffectedEdgeIDs {
			if want[id] {
				return true
			}
		}
	}
	for _, change := range event.ChangedEdges {
		if want[change.EdgeID.String()] {
			return true
		}
	}
	return false
}

func eventHasAnyType(event graphchange.CommittedEvent, types []graphchange.ChangeType) bool {
	want := map[graphchange.ChangeType]bool{}
	for _, typ := range types {
		want[typ] = true
	}
	for _, change := range event.Changes {
		if want[change.Type] {
			return true
		}
	}
	return false
}

func eventHasAnyLabel(event graphchange.CommittedEvent, labels []string) bool {
	want := stringSet(labels)
	for _, change := range event.Changes {
		if nodeHasAnyLabel(change.Node, want) || nodeHasAnyLabel(change.OldNode, want) || edgeHasAnyLabel(change.Edge, want) || edgeHasAnyLabel(change.OldEdge, want) {
			return true
		}
	}
	for _, edge := range event.ChangedEdges {
		for _, label := range edge.Labels {
			if want[label] {
				return true
			}
		}
	}
	return false
}

func eventHasAnyField(event graphchange.CommittedEvent, fields []string) bool {
	want := stringSet(fields)
	for _, change := range event.Changes {
		for _, field := range change.ChangedFields {
			if want[field] {
				return true
			}
		}
	}
	return false
}

func nodeHasAnyLabel(node *graph.Node, labels map[string]bool) bool {
	if node == nil {
		return false
	}
	for _, label := range node.Labels {
		if labels[label] {
			return true
		}
	}
	return false
}

func edgeHasAnyLabel(edge *graph.Edge, labels map[string]bool) bool {
	if edge == nil {
		return false
	}
	for _, label := range edge.Labels {
		if labels[label] {
			return true
		}
	}
	return false
}

func containsDomainID(values []graph.DomainID, value graph.DomainID) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func eventRevision(event graphchange.CommittedEvent) uint64 {
	if event.Revision != 0 {
		return event.Revision
	}
	return event.GraphRevision
}

func scopeKey(spaceID, domainID string) string {
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	if spaceID == "" || domainID == "" {
		return ""
	}
	return spaceID + ":" + domainID
}

func stringSet(values []string) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = true
		}
	}
	return out
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
