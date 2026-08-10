package graphstorage

import (
	"errors"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

var ErrIndexUnavailable = errors.New("graph storage: index unavailable")

type IndexBuildState string

const (
	IndexBuildStateBuilding IndexBuildState = "building"
	IndexBuildStateReady    IndexBuildState = "ready"
	IndexBuildStateFailed   IndexBuildState = "failed"
	IndexBuildStateStale    IndexBuildState = "stale"
	IndexBuildStateRetired  IndexBuildState = "retired"
)

type IndexMetadata struct {
	Name                     string
	DomainID                 graph.DomainID
	SchemaHash               string
	TargetKind               schema.IndexTargetKind
	TargetType               string
	Labels                   []string
	Field                    schema.FieldPath
	Kind                     schema.IndexKind
	Direction                schema.IndexSortDirection
	BuildState               IndexBuildState
	LastIndexedGraphRevision uint64
	KeyEncodingVersion       int
	Error                    string
}

type OrderedNodePropertyScan struct {
	DomainID      graph.DomainID
	IndexName     string
	Direction     schema.IndexSortDirection
	Limit         int
	Cursor        string
	HasLow        bool
	Low           any
	LowExclusive  bool
	HasHigh       bool
	High          any
	HighExclusive bool
}

type OrderedEdgePropertyScan struct {
	DomainID  graph.DomainID
	IndexName string
	Direction schema.IndexSortDirection
	Limit     int
	Cursor    string
}

type NodeIndexEntry struct {
	NodeID graph.NodeID
	Value  any
	Cursor string
}

type EdgeIndexEntry struct {
	EdgeID graph.EdgeID
	Value  any
	Cursor string
}

type LabelScan struct {
	DomainID graph.DomainID
	Label    string
	Limit    int
	Cursor   string
}

type AdjacencyDirection string

const (
	AdjacencyDirectionOut AdjacencyDirection = "out"
	AdjacencyDirectionIn  AdjacencyDirection = "in"
)

type AdjacencyScan struct {
	DomainID  graph.DomainID
	NodeID    graph.NodeID
	Label     string
	Direction AdjacencyDirection
	Limit     int
	Cursor    string
}
