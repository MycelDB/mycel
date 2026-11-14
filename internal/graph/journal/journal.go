// Package journal stores the authoritative, compact graph-change handoff for a
// graph Raft partition. Entries contain identifiers and revision metadata only;
// derived consumers read graph truth separately when materializing work.
package journal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/fsperm"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const journalVersion = 1

// Entry is the compact durable handoff for one committed graph revision. It is
// intentionally free of graph content and full snapshots.
type Entry struct {
	Version         int          `json:"version"`
	ID              string       `json:"id"`
	SpaceID         string       `json:"space_id"`
	DomainID        string       `json:"domain_id"`
	Revision        uint64       `json:"revision"`
	TransactionID   string       `json:"transaction_id,omitempty"`
	CreatedNodeIDs  []string     `json:"created_node_ids,omitempty"`
	UpdatedNodeIDs  []string     `json:"updated_node_ids,omitempty"`
	DeletedNodeIDs  []string     `json:"deleted_node_ids,omitempty"`
	ChangedEdges    []EdgeChange `json:"changed_edges,omitempty"`
	AffectedNodeIDs []string     `json:"affected_node_ids,omitempty"`
	AffectedEdgeIDs []string     `json:"affected_edge_ids,omitempty"`
	CommittedAt     time.Time    `json:"committed_at,omitempty"`
}

// CompactionFence authorizes removal of entries older than Through for one
// graph scope. Callers must provide a fence at or beyond every durable derived
// consumer cursor before compacting.
type CompactionFence struct {
	Scope   graphchange.Scope
	Through uint64
}

// EdgeChange is the compact journal representation of a changed edge.
type EdgeChange struct {
	EdgeID string   `json:"edge_id"`
	Labels []string `json:"labels,omitempty"`
	Change string   `json:"change,omitempty"`
	FromID string   `json:"from_id,omitempty"`
	ToID   string   `json:"to_id,omitempty"`
}

type record struct {
	Version  int    `json:"version"`
	Sequence uint64 `json:"sequence"`
	Entry    Entry  `json:"entry"`
}

// Journal is a per-Raft-partition append-only graph-change journal.
type Journal struct {
	mu        sync.Mutex
	path      string
	partition uint32
	next      uint64
	entries   []Entry
}

// Open opens or creates the journal at root/partition-XXXXXXXX.jsonl.
func Open(root string, partition uint32) (*Journal, error) {
	if root == "" {
		return nil, errors.New("graph change journal root is required")
	}
	if err := os.MkdirAll(root, fsperm.PrivateDir); err != nil {
		return nil, err
	}
	j := &Journal{path: filepath.Join(root, fmt.Sprintf("partition-%08d.jsonl", partition)), partition: partition}
	if err := j.load(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Journal) load() error {
	file, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(bufio.NewReader(file))
	for {
		var rec record
		if err := decoder.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("decode graph change journal: %w", err)
		}
		if rec.Version != journalVersion || rec.Entry.Version != journalVersion {
			return fmt.Errorf("unsupported graph change journal version")
		}
		if rec.Sequence == 0 || rec.Entry.Revision == 0 {
			return fmt.Errorf("invalid graph change journal record")
		}
		j.entries = append(j.entries, cloneEntry(rec.Entry))
		if rec.Sequence >= j.next {
			j.next = rec.Sequence + 1
		}
	}
	return nil
}

// Append durably records event once. Repeated application of the same Raft
// command is idempotent by scope and graph revision.
func (j *Journal) Append(ctx context.Context, event graphchange.CommittedEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entry, err := EntryFromEvent(event)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, existing := range j.entries {
		if existing.SpaceID == entry.SpaceID && existing.DomainID == entry.DomainID && existing.Revision == entry.Revision {
			return nil
		}
	}
	if j.next == 0 {
		j.next = 1
	}
	rec := record{Version: journalVersion, Sequence: j.next, Entry: cloneEntry(entry)}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(payload, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	j.entries = append(j.entries, entry)
	j.next++
	return nil
}

// Entries returns a stable copy of all retained entries.
func (j *Journal) Entries() []Entry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Entry, len(j.entries))
	for i := range j.entries {
		out[i] = cloneEntry(j.entries[i])
	}
	return out
}

// EntriesAfter returns entries for a scope whose graph revision is greater than
// after. Entries are sorted by graph revision and then journal order.
func (j *Journal) EntriesAfter(ctx context.Context, scope graphchange.Scope, after uint64) ([]Entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]Entry, 0)
	for _, entry := range j.entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.SpaceID != scope.SpaceID || (scope.DomainID != "" && entry.DomainID != scope.DomainID) || entry.Revision <= after {
			continue
		}
		out = append(out, cloneEntry(entry))
	}
	sort.SliceStable(out, func(i, k int) bool { return out[i].Revision < out[k].Revision })
	return out, nil
}

// Compact removes entries older than each supplied scope fence. The fence
// entry itself is retained as a replay marker so a new consumer can discover a
// scope and fail closed rather than silently starting from an empty history.
func (j *Journal) Compact(ctx context.Context, fences []CompactionFence) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(fences) == 0 {
		return 0, nil
	}
	cutoffs := map[string]uint64{}
	for _, fence := range fences {
		if fence.Through == 0 || fence.Scope.SpaceID == "" || fence.Scope.DomainID == "" {
			continue
		}
		key := fence.Scope.SpaceID + "\x00" + fence.Scope.DomainID
		if current, ok := cutoffs[key]; !ok || fence.Through < current {
			cutoffs[key] = fence.Through
		}
	}
	if len(cutoffs) == 0 {
		return 0, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	kept := make([]Entry, 0, len(j.entries))
	removed := 0
	for _, entry := range j.entries {
		key := entry.SpaceID + "\x00" + entry.DomainID
		cutoff, ok := cutoffs[key]
		if ok && entry.Revision < cutoff {
			removed++
			continue
		}
		kept = append(kept, cloneEntry(entry))
	}
	if removed == 0 {
		return 0, nil
	}
	if err := j.rewriteLocked(kept); err != nil {
		return 0, err
	}
	return removed, nil
}

// CurrentRevision returns the highest journaled revision for a scope.
func (j *Journal) CurrentRevision(ctx context.Context, scope graphchange.Scope) (uint64, error) {
	entries, err := j.EntriesAfter(ctx, scope, 0)
	if err != nil {
		return 0, err
	}
	var current uint64
	for _, entry := range entries {
		if entry.Revision > current {
			current = entry.Revision
		}
	}
	return current, nil
}

// Restore replaces the journal contents from a Raft snapshot.
func (j *Journal) Restore(ctx context.Context, entries []Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cloned := make([]Entry, 0, len(entries))
	seen := map[string]struct{}{}
	for _, entry := range entries {
		if entry.Version == 0 {
			entry.Version = journalVersion
		}
		if entry.Version != journalVersion || entry.Revision == 0 || entry.SpaceID == "" || entry.DomainID == "" {
			return errors.New("invalid graph change journal snapshot entry")
		}
		key := entry.SpaceID + "\x00" + entry.DomainID + "\x00" + fmt.Sprint(entry.Revision)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		cloned = append(cloned, cloneEntry(entry))
	}
	sort.Slice(cloned, func(i, k int) bool { return cloned[i].Revision < cloned[k].Revision })
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := writeJournalEntries(j.path, cloned); err != nil {
		return err
	}
	j.entries = cloned
	j.next = uint64(len(cloned)) + 1
	return nil
}

// EntryFromEvent converts a graph event to a compact journal entry.
func EntryFromEvent(event graphchange.CommittedEvent) (Entry, error) {
	event = event.NormalizedCopy()
	if event.SpaceID == uuid.Nil || event.DomainID == uuid.Nil || event.GraphRevision == 0 {
		return Entry{}, errors.New("graph change journal event requires scope and revision")
	}
	entry := Entry{Version: journalVersion, ID: event.ID.String(), SpaceID: event.SpaceID.String(), DomainID: event.DomainID.String(), Revision: event.GraphRevision, TransactionID: event.TransactionID.String(), CreatedNodeIDs: uuidStrings(event.CreatedNodeIDs), UpdatedNodeIDs: uuidStrings(event.UpdatedNodeIDs), DeletedNodeIDs: uuidStrings(event.DeletedNodeIDs), AffectedNodeIDs: uuidStrings(event.AffectedNodeIDs), AffectedEdgeIDs: uuidStrings(event.AffectedEdgeIDs), CommittedAt: event.CommittedAt}
	for _, edge := range event.ChangedEdges {
		entry.ChangedEdges = append(entry.ChangedEdges, EdgeChange{EdgeID: edge.EdgeID.String(), Labels: append([]string(nil), edge.Labels...), Change: edge.Change, FromID: edge.FromID.String(), ToID: edge.ToID.String()})
	}
	return entry, nil
}

// Event converts a compact journal entry back to the graph-neutral event type.
func (e Entry) Event() (graphchange.CommittedEvent, error) {
	if e.Version != journalVersion || e.Revision == 0 {
		return graphchange.CommittedEvent{}, errors.New("invalid graph change journal entry")
	}
	spaceID, err := uuid.Parse(e.SpaceID)
	if err != nil {
		return graphchange.CommittedEvent{}, err
	}
	domainID, err := uuid.Parse(e.DomainID)
	if err != nil {
		return graphchange.CommittedEvent{}, err
	}
	event := graphchange.CommittedEvent{ID: parseOptionalUUID(e.ID), SpaceID: domainspace.SpaceID(spaceID), DomainID: graph.DomainID(domainID), DomainIDs: []graph.DomainID{graph.DomainID(domainID)}, GraphRevision: e.Revision, Revision: e.Revision, TransactionID: parseOptionalUUID(e.TransactionID), CreatedNodeIDs: parseUUIDList(e.CreatedNodeIDs), UpdatedNodeIDs: parseUUIDList(e.UpdatedNodeIDs), DeletedNodeIDs: parseUUIDList(e.DeletedNodeIDs), AffectedNodeIDs: parseUUIDList(e.AffectedNodeIDs), AffectedEdgeIDs: parseUUIDList(e.AffectedEdgeIDs), CommittedAt: e.CommittedAt}
	for _, edge := range e.ChangedEdges {
		event.ChangedEdges = append(event.ChangedEdges, graphchange.EdgeChange{EdgeID: graph.EdgeID(parseUUID(edge.EdgeID)), Labels: append([]string(nil), edge.Labels...), Change: edge.Change, FromID: graph.NodeID(parseUUID(edge.FromID)), ToID: graph.NodeID(parseUUID(edge.ToID))})
	}
	// Older or compact producers may omit event identifiers. Derive stable
	// scope/revision identifiers so replay remains idempotent for semantic
	// dirty-event appenders that deduplicate by transaction ID.
	stableID := uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprintf("graph-change-journal:%s:%s:%d", e.SpaceID, e.DomainID, e.Revision)))
	if event.ID == uuid.Nil {
		event.ID = stableID
	}
	if event.TransactionID == uuid.Nil {
		event.TransactionID = stableID
	}
	event.TxnID = event.TransactionID
	event.Normalize()
	return event, nil
}

func writeJournalEntries(path string, entries []Entry) error {
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	cleanup := func(err error) error {
		_ = file.Close()
		_ = os.Remove(tmp)
		return err
	}
	for sequence, entry := range entries {
		payload, err := json.Marshal(record{Version: journalVersion, Sequence: uint64(sequence) + 1, Entry: cloneEntry(entry)})
		if err != nil {
			return cleanup(err)
		}
		if _, err := file.Write(append(payload, '\n')); err != nil {
			return cleanup(err)
		}
	}
	if err := file.Sync(); err != nil {
		return cleanup(err)
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

func (j *Journal) rewriteLocked(entries []Entry) error {
	if err := writeJournalEntries(j.path, entries); err != nil {
		return err
	}
	j.entries = append([]Entry(nil), entries...)
	j.next = uint64(len(entries)) + 1
	return nil
}

func cloneEntry(entry Entry) Entry {
	entry.CreatedNodeIDs = append([]string(nil), entry.CreatedNodeIDs...)
	entry.UpdatedNodeIDs = append([]string(nil), entry.UpdatedNodeIDs...)
	entry.DeletedNodeIDs = append([]string(nil), entry.DeletedNodeIDs...)
	entry.AffectedNodeIDs = append([]string(nil), entry.AffectedNodeIDs...)
	entry.AffectedEdgeIDs = append([]string(nil), entry.AffectedEdgeIDs...)
	entry.ChangedEdges = append([]EdgeChange(nil), entry.ChangedEdges...)
	for i := range entry.ChangedEdges {
		entry.ChangedEdges[i].Labels = append([]string(nil), entry.ChangedEdges[i].Labels...)
	}
	return entry
}

func uuidStrings(values []uuid.UUID) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != uuid.Nil {
			out = append(out, value.String())
		}
	}
	return out
}

func parseUUID(value string) uuid.UUID {
	parsed, _ := uuid.Parse(value)
	return parsed
}

func parseOptionalUUID(value string) uuid.UUID {
	if value == "" {
		return uuid.Nil
	}
	return parseUUID(value)
}

func parseUUIDList(values []string) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(values))
	for _, value := range values {
		if parsed := parseUUID(value); parsed != uuid.Nil {
			out = append(out, parsed)
		}
	}
	return out
}
