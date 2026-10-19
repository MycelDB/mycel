package query

import (
	"errors"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

// ErrScanBudgetExceeded reports that a dependency cannot produce a correct
// page without exceeding the caller-supplied examine bound.
var ErrScanBudgetExceeded = errors.New("graph scan examine budget exceeded")

// NodeScan describes a finite ID-ordered domain node scan.
type NodeScan struct {
	Limit        int
	Cursor       string
	ExamineLimit int
}

// LabelScan describes a paginated label-index scan.
type LabelScan struct {
	Label  string
	Limit  int
	Cursor string
}

// TagScan describes a paginated tag-index scan.
type TagScan struct {
	Tag    string
	Limit  int
	Cursor string
}

// OrderedNodePropertyScan describes a bounded ordered property-index scan.
type OrderedNodePropertyScan struct {
	IndexName     string
	Direction     schema.IndexSortDirection
	Limit         int
	Cursor        string
	ExamineLimit  int
	HasLow        bool
	Low           any
	LowExclusive  bool
	HasHigh       bool
	High          any
	HighExclusive bool
}

// AdjacencyDirection identifies the side of an edge adjacency scan.
type AdjacencyDirection string

const (
	AdjacencyDirectionOut AdjacencyDirection = "out"
	AdjacencyDirectionIn  AdjacencyDirection = "in"
)

// AdjacencyScan describes a paginated edge adjacency-index scan.
type AdjacencyScan struct {
	NodeID       string
	Label        string
	Direction    AdjacencyDirection
	Limit        int
	Cursor       string
	ExamineLimit int
}

// SubtreeScan describes an indexed hierarchy traversal.
type SubtreeScan struct {
	Roots        []graph.Node
	Label        string
	Direction    AdjacencyDirection
	MinDepth     int
	MaxDepth     int
	MaxNodes     int
	MaxEdges     int
	TargetLabels []string
}

// SubtreeRoot contains one root and the hierarchy projected beneath it.
type SubtreeRoot struct {
	Root          graph.Node
	Nodes         []graph.Node
	ParentByChild map[string]string
	OrderByChild  map[string]any
}

// SubtreeResult contains the hierarchy and graph projection returned by a subtree scan.
type SubtreeResult struct {
	Roots            []SubtreeRoot
	GraphNodes       []graph.Node
	GraphEdges       []graph.Edge
	Truncated        bool
	TruncationReason string
}

// IndexedReadStats describes index work performed by a graph read.
type IndexedReadStats struct {
	Plan                string
	IndexName           string
	IndexEntriesScanned int
	NodesLoaded         int
	EdgesLoaded         int
	FullScan            bool
	NextCursorKind      string
	AdjacencyScanCalls  int
	NodeReadCalls       int
}
