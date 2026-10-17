package execution

import (
	"context"
	"fmt"
	"math"
	"sync"

	querylogical "github.com/myceldb/mycel/internal/query/logical"
)

const (
	defaultMaxCandidates         int64 = 50_000
	defaultMaxIndexEntries       int64 = 50_000
	defaultMaxStartNodes         int64 = 1_000
	defaultMaxNodesLoaded        int64 = 50_000
	defaultMaxEdgesLoaded        int64 = 50_000
	defaultMaxAdjacencyCalls     int64 = 10_000
	defaultMaxIntermediateRows   int64 = 10_000
	defaultMaxOutputRows         int64 = 10_000
	defaultMaxSemanticCandidates int64 = 1_000
	defaultMaxMemoryBytes        int64 = 64 << 20
	defaultMaxTraversalDepth           = 8
)

// Budget is resolved before execution. Zero input fields are replaced with
// finite defaults; no operator interprets zero as an unbounded allowance.
type Budget struct {
	MaxCandidates         int64
	MaxIndexEntries       int64
	MaxStartNodes         int64
	MaxNodesLoaded        int64
	MaxEdgesLoaded        int64
	MaxAdjacencyCalls     int64
	MaxIntermediateRows   int64
	MaxOutputRows         int64
	MaxSemanticCandidates int64
	MaxMemoryBytes        int64
	MaxTraversalDepth     int
	BatchSize             int
}

// ResolveBudget applies finite defaults and optionally tightens caller bounds
// with static physical-plan limits. Negative limits are rejected by NewTracker.
func ResolveBudget(requested Budget, static Budget) Budget {
	resolved := Budget{
		MaxCandidates:         resolveLimit(requested.MaxCandidates, defaultMaxCandidates),
		MaxIndexEntries:       resolveLimit(requested.MaxIndexEntries, defaultMaxIndexEntries),
		MaxStartNodes:         resolveLimit(requested.MaxStartNodes, defaultMaxStartNodes),
		MaxNodesLoaded:        resolveLimit(requested.MaxNodesLoaded, defaultMaxNodesLoaded),
		MaxEdgesLoaded:        resolveLimit(requested.MaxEdgesLoaded, defaultMaxEdgesLoaded),
		MaxAdjacencyCalls:     resolveLimit(requested.MaxAdjacencyCalls, defaultMaxAdjacencyCalls),
		MaxIntermediateRows:   resolveLimit(requested.MaxIntermediateRows, defaultMaxIntermediateRows),
		MaxOutputRows:         resolveLimit(requested.MaxOutputRows, defaultMaxOutputRows),
		MaxSemanticCandidates: resolveLimit(requested.MaxSemanticCandidates, defaultMaxSemanticCandidates),
		MaxMemoryBytes:        resolveLimit(requested.MaxMemoryBytes, defaultMaxMemoryBytes),
		MaxTraversalDepth:     resolveIntLimit(requested.MaxTraversalDepth, defaultMaxTraversalDepth),
		BatchSize:             resolveIntLimit(requested.BatchSize, DefaultBatchSize),
	}
	resolved.MaxCandidates = tighten(resolved.MaxCandidates, static.MaxCandidates)
	resolved.MaxIndexEntries = tighten(resolved.MaxIndexEntries, static.MaxIndexEntries)
	resolved.MaxStartNodes = tighten(resolved.MaxStartNodes, static.MaxStartNodes)
	resolved.MaxNodesLoaded = tighten(resolved.MaxNodesLoaded, static.MaxNodesLoaded)
	resolved.MaxEdgesLoaded = tighten(resolved.MaxEdgesLoaded, static.MaxEdgesLoaded)
	resolved.MaxAdjacencyCalls = tighten(resolved.MaxAdjacencyCalls, static.MaxAdjacencyCalls)
	resolved.MaxIntermediateRows = tighten(resolved.MaxIntermediateRows, static.MaxIntermediateRows)
	resolved.MaxOutputRows = tighten(resolved.MaxOutputRows, static.MaxOutputRows)
	resolved.MaxSemanticCandidates = tighten(resolved.MaxSemanticCandidates, static.MaxSemanticCandidates)
	resolved.MaxMemoryBytes = tighten(resolved.MaxMemoryBytes, static.MaxMemoryBytes)
	resolved.MaxTraversalDepth = tightenInt(resolved.MaxTraversalDepth, static.MaxTraversalDepth)
	resolved.BatchSize = tightenInt(resolved.BatchSize, static.BatchSize)
	return resolved
}

func resolveLimit(value int64, fallback int64) int64 {
	if value == 0 {
		return fallback
	}
	return value
}

func resolveIntLimit(value int, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

func tighten(value int64, cap int64) int64 {
	if cap > 0 && cap < value {
		return cap
	}
	return value
}

func tightenInt(value int, cap int) int {
	if cap > 0 && cap < value {
		return cap
	}
	return value
}

// Usage is a deterministic snapshot of work consumed by one execution.
type Usage struct {
	CandidatesExamined int64
	IndexEntries       int64
	StartNodes         int64
	NodesLoaded        int64
	EdgesLoaded        int64
	AdjacencyCalls     int64
	NodeReadCalls      int64
	IntermediateRows   int64
	OutputRows         int64
	SemanticCandidates int64
	RetainedMemory     int64
	PeakRetainedMemory int64
	TraversalDepth     int
	Truncated          bool
	TruncationReason   string
}

// Tracker is shared by all operators in one execution.
type Tracker struct {
	mu     sync.Mutex
	limits Budget
	usage  Usage
}

func NewTracker(limits Budget) (*Tracker, error) {
	if err := validateBudget(limits); err != nil {
		return nil, err
	}
	return &Tracker{limits: limits}, nil
}

func validateBudget(b Budget) error {
	checks := []struct {
		name  string
		value int64
	}{
		{"max candidates", b.MaxCandidates},
		{"max index entries", b.MaxIndexEntries},
		{"max start nodes", b.MaxStartNodes},
		{"max nodes loaded", b.MaxNodesLoaded},
		{"max edges loaded", b.MaxEdgesLoaded},
		{"max adjacency calls", b.MaxAdjacencyCalls},
		{"max intermediate rows", b.MaxIntermediateRows},
		{"max output rows", b.MaxOutputRows},
		{"max semantic candidates", b.MaxSemanticCandidates},
		{"max memory bytes", b.MaxMemoryBytes},
		{"max traversal depth", int64(b.MaxTraversalDepth)},
		{"batch size", int64(b.BatchSize)},
	}
	for _, check := range checks {
		if check.value <= 0 {
			return querylogical.NewError(querylogical.ErrorInvalidQuery, "configure query budget", fmt.Sprintf("%s must be positive", check.name), nil)
		}
	}
	return nil
}

func (t *Tracker) Limits() Budget {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.limits
}

func (t *Tracker) Snapshot() Usage {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.usage
}

// RemainingNodesLoaded reports how many additional node payloads can be read.
func (t *Tracker) RemainingNodesLoaded() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return max(0, t.limits.MaxNodesLoaded-t.usage.NodesLoaded)
}

// RemainingEdgesLoaded reports how many additional edge payloads can be read.
func (t *Tracker) RemainingEdgesLoaded() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return max(0, t.limits.MaxEdgesLoaded-t.usage.EdgesLoaded)
}

func (t *Tracker) ExamineCandidates(delta int64) error {
	return t.add("candidate", delta, &t.usage.CandidatesExamined, t.limits.MaxCandidates)
}

func (t *Tracker) ScanIndexEntries(delta int64) error {
	return t.add("index entry", delta, &t.usage.IndexEntries, t.limits.MaxIndexEntries)
}

func (t *Tracker) UseStartNodes(delta int64) error {
	return t.add("start node", delta, &t.usage.StartNodes, t.limits.MaxStartNodes)
}

func (t *Tracker) LoadNodes(delta int64) error {
	return t.add("node load", delta, &t.usage.NodesLoaded, t.limits.MaxNodesLoaded)
}

func (t *Tracker) LoadEdges(delta int64) error {
	return t.add("edge load", delta, &t.usage.EdgesLoaded, t.limits.MaxEdgesLoaded)
}

func (t *Tracker) UseAdjacency(delta int64) error {
	return t.add("adjacency call", delta, &t.usage.AdjacencyCalls, t.limits.MaxAdjacencyCalls)
}

func (t *Tracker) RecordNodeReads(delta int64) error {
	if delta < 0 {
		return invalidState("track query resources", "node read delta must be non-negative")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usage.NodeReadCalls += delta
	return nil
}

func (t *Tracker) ProduceIntermediate(delta int64) error {
	return t.add("intermediate row", delta, &t.usage.IntermediateRows, t.limits.MaxIntermediateRows)
}

func (t *Tracker) ProduceOutput(delta int64) error {
	return t.add("output row", delta, &t.usage.OutputRows, t.limits.MaxOutputRows)
}

func (t *Tracker) UseSemanticCandidates(delta int64) error {
	return t.add("semantic candidate", delta, &t.usage.SemanticCandidates, t.limits.MaxSemanticCandidates)
}

func (t *Tracker) ObserveTraversalDepth(depth int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if depth > t.limits.MaxTraversalDepth {
		return exhausted("traversal depth", int64(depth), int64(t.limits.MaxTraversalDepth))
	}
	if depth > t.usage.TraversalDepth {
		t.usage.TraversalDepth = depth
	}
	return nil
}

func (t *Tracker) MarkTruncated(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usage.Truncated = true
	if t.usage.TruncationReason == "" {
		t.usage.TruncationReason = reason
	}
}

func (t *Tracker) ReserveMemory(bytes int64) error {
	if bytes < 0 {
		return invalidState("reserve query memory", "byte count must be non-negative")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if bytes > t.limits.MaxMemoryBytes-t.usage.RetainedMemory {
		return exhausted("retained memory bytes", exceededUsage(t.limits.MaxMemoryBytes), t.limits.MaxMemoryBytes)
	}
	next := t.usage.RetainedMemory + bytes
	t.usage.RetainedMemory = next
	if next > t.usage.PeakRetainedMemory {
		t.usage.PeakRetainedMemory = next
	}
	return nil
}

func (t *Tracker) ReleaseMemory(bytes int64) {
	if bytes <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.usage.RetainedMemory -= bytes
	if t.usage.RetainedMemory < 0 {
		t.usage.RetainedMemory = 0
	}
}

func (t *Tracker) add(resource string, delta int64, current *int64, limit int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if delta < 0 {
		return invalidState("track query resources", resource+" delta must be non-negative")
	}
	if delta > limit-*current {
		return exhausted(resource, exceededUsage(limit), limit)
	}
	next := *current + delta
	*current = next
	return nil
}

func exceededUsage(limit int64) int64 {
	if limit == math.MaxInt64 {
		return limit
	}
	return limit + 1
}

func exhausted(resource string, used int64, limit int64) error {
	return querylogical.NewError(querylogical.ErrorResourceExhausted, "execute query", fmt.Sprintf("%s budget exceeded: used %d, limit %d", resource, used, limit), nil)
}

func invalidState(operation string, message string) error {
	return querylogical.NewError(querylogical.ErrorInternal, operation, message, nil)
}

func canceled(ctx context.Context, operation string) error {
	if err := ctx.Err(); err != nil {
		return querylogical.NewError(querylogical.ErrorCanceled, operation, "query context canceled", err)
	}
	return nil
}
