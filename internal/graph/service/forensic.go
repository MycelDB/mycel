package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const defaultForensicExportPageSize = 100
const maxForensicExportPageSize = 1000

type LocalGraphForensicExportOptions struct {
	PageSize    int
	PageToken   string
	SourceLabel string
}

type LocalGraphForensicExport struct {
	Stats         LocalGraphStats       `json:"stats"`
	Nodes         []ForensicGraphEntity `json:"nodes"`
	Edges         []ForensicGraphEntity `json:"edges"`
	NextPageToken string                `json:"next_page_token,omitempty"`
	Truncated     bool                  `json:"truncated"`
	Warnings      []string              `json:"warnings,omitempty"`
}

type ForensicGraphEntity struct {
	ID            string `json:"id"`
	Checksum      string `json:"checksum"`
	CanonicalJSON string `json:"canonical_json"`
}

type forensicEntity struct {
	kind string
	ForensicGraphEntity
}

func (m *Module) LocalGraphForensicExport(ctx context.Context, spaceID string, domainID string, opts LocalGraphForensicExportOptions) (LocalGraphForensicExport, error) {
	spaceID = strings.TrimSpace(spaceID)
	domainID = strings.TrimSpace(domainID)
	if spaceID == "" {
		return LocalGraphForensicExport{}, fmt.Errorf("%w: space_id is required", ErrInvalidInput)
	}
	parsedDomain, err := uuid.Parse(domainID)
	if err != nil || parsedDomain == uuid.Nil {
		return LocalGraphForensicExport{}, fmt.Errorf("%w: domain_id must be a UUID", ErrInvalidInput)
	}
	store, err := m.existingStoreForConsistencyStats(ctx, spaceID)
	if err != nil {
		return LocalGraphForensicExport{}, err
	}
	domain := domaingraph.DomainID(parsedDomain)
	nodes, err := store.ListNodesByDomain(ctx, domain)
	if err != nil {
		return LocalGraphForensicExport{}, mapStorageError(err)
	}
	allEdges, err := store.ListEdges(ctx)
	if err != nil {
		return LocalGraphForensicExport{}, mapStorageError(err)
	}
	edges := make([]domaingraph.Edge, 0, len(allEdges))
	for _, edge := range allEdges {
		if edge.DomainID == domain {
			edges = append(edges, edge)
		}
	}
	partitionID := uint32(0)
	if m.raftPartitionCount > 0 {
		if parsedSpace, err := uuid.Parse(spaceID); err == nil && parsedSpace != uuid.Nil {
			if cmd, err := consensus.NewSpaceCommand(domainspace.SpaceID(parsedSpace), m.raftPartitionCount, recordTypeGraphCommit, nil, "graph-forensic-export"); err == nil {
				partitionID = cmd.PartitionID
			}
		}
	}
	collectedAt := time.Now().UTC()
	stats, err := buildLocalGraphStats(spaceID, domainID, partitionID, store.Revision(), nodes, edges, collectedAt)
	if err != nil {
		return LocalGraphForensicExport{}, err
	}
	entities, err := buildForensicEntities(nodes, edges)
	if err != nil {
		return LocalGraphForensicExport{}, err
	}
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultForensicExportPageSize
	}
	if pageSize > maxForensicExportPageSize {
		pageSize = maxForensicExportPageSize
	}
	offset := 0
	if strings.TrimSpace(opts.PageToken) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(opts.PageToken))
		if err != nil || parsed < 0 {
			return LocalGraphForensicExport{}, fmt.Errorf("%w: page_token must be a non-negative entity offset", ErrInvalidInput)
		}
		offset = parsed
	}
	if offset > len(entities) {
		offset = len(entities)
	}
	end := offset + pageSize
	if end > len(entities) {
		end = len(entities)
	}
	export := LocalGraphForensicExport{Stats: stats, Warnings: []string{"forensic export is local latest-state evidence only; no repair or merge was performed"}}
	for _, entity := range entities[offset:end] {
		switch entity.kind {
		case "node":
			export.Nodes = append(export.Nodes, entity.ForensicGraphEntity)
		case "edge":
			export.Edges = append(export.Edges, entity.ForensicGraphEntity)
		}
	}
	if end < len(entities) {
		export.NextPageToken = strconv.Itoa(end)
		export.Truncated = true
	}
	return export, nil
}

func buildForensicEntities(nodes []domaingraph.Node, edges []domaingraph.Edge) ([]forensicEntity, error) {
	canonicalNodes := make([]canonicalNode, 0, len(nodes))
	for _, node := range nodes {
		canonicalNodes = append(canonicalNodes, canonicalizeNode(node))
	}
	sort.Slice(canonicalNodes, func(i, j int) bool { return canonicalNodes[i].ID < canonicalNodes[j].ID })
	canonicalEdges := make([]canonicalEdge, 0, len(edges))
	for _, edge := range edges {
		canonicalEdges = append(canonicalEdges, canonicalizeEdge(edge))
	}
	sort.Slice(canonicalEdges, func(i, j int) bool { return canonicalEdges[i].ID < canonicalEdges[j].ID })
	out := make([]forensicEntity, 0, len(canonicalNodes)+len(canonicalEdges))
	for _, node := range canonicalNodes {
		entity, err := forensicEntityFromCanonical("node", node.ID, node)
		if err != nil {
			return nil, err
		}
		out = append(out, entity)
	}
	for _, edge := range canonicalEdges {
		entity, err := forensicEntityFromCanonical("edge", edge.ID, edge)
		if err != nil {
			return nil, err
		}
		out = append(out, entity)
	}
	return out, nil
}

func forensicEntityFromCanonical(kind string, id string, value any) (forensicEntity, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return forensicEntity{}, err
	}
	checksum, err := checksumJSON(value)
	if err != nil {
		return forensicEntity{}, err
	}
	return forensicEntity{kind: kind, ForensicGraphEntity: ForensicGraphEntity{ID: id, Checksum: checksum, CanonicalJSON: string(data)}}, nil
}
