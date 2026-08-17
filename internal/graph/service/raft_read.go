package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Phase F read-consistency contract for raft-mode graph reads:
//
//   - Strong committed reads are the default. A committed/read-only graph read
//     must route to the owning partition leader and pass a raft ReadIndex/apply
//     barrier before reading local graph storage.
//   - Read-write transaction reads are overlay reads. They must stay on the
//     transaction home node and require that home node to remain the local
//     partition leader, so staged overlay state is never bypassed by forwarding
//     to another node.
//   - Stale/local follower reads are disallowed unless a future explicit
//     request/config opt-in labels the response as stale and excludes it from
//     write validation, transaction base-revision lookup, authorization, and
//     schema/edge endpoint validation paths.
//   - If the leader, route, read-index quorum, or apply barrier is unavailable,
//     raft-mode reads fail closed instead of falling back to local file state.
//   - Read-only transactions are linearizable current-read contexts in V1, not
//     historical repeatable-read snapshots. BeginTransaction records a strong
//     base revision, and each read-only graph read performs a fresh strong
//     barrier and may observe newer committed revisions; reads fail closed if
//     local state is ever behind the transaction base revision.
//
// F0 documents this contract and inventories the current read paths. F1/F2
// added consensus ReadIndex and graph strong-read barrier enforcement; F3 makes
// the read-only transaction model explicit.
// See docs/implementation/phase-f-read-consistency-inventory.md.
// See docs/implementation/phase-f-read-consistency-model-implementation-plan.md.
type raftGraphReadRequest struct {
	Op      string `json:"op"`
	SpaceID string `json:"space_id"`
	Tx      struct {
		ID, SessionID, PrincipalID, SpaceID, DomainID, Mode, State string
		HomeNodeID                                                 uint64
		BaseRevision                                               int64
	} `json:"tx"`
	ID                      string                        `json:"id,omitempty"`
	PageSize                int                           `json:"page_size,omitempty"`
	PageToken               string                        `json:"page_token,omitempty"`
	SchemaHash              string                        `json:"schema_hash,omitempty"`
	Indexes                 []schemamodel.IndexDefinition `json:"indexes,omitempty"`
	LabelScan               LabelScan                     `json:"label_scan,omitempty"`
	TagScan                 TagScan                       `json:"tag_scan,omitempty"`
	OrderedNodePropertyScan OrderedNodePropertyScan       `json:"ordered_node_property_scan,omitempty"`
	AdjacencyScan           AdjacencyScan                 `json:"adjacency_scan,omitempty"`
	SubtreeScan             SubtreeScan                   `json:"subtree_scan,omitempty"`
}

type StrongReadContext struct {
	GroupID          consensus.GroupID `json:"group_id,omitempty"`
	PartitionID      uint32            `json:"partition_id,omitempty"`
	LeaderNodeID     consensus.NodeID  `json:"leader_node_id,omitempty"`
	ReadIndex        uint64            `json:"read_index,omitempty"`
	AppliedIndex     uint64            `json:"applied_index,omitempty"`
	ObservedRevision int64             `json:"observed_revision,omitempty"`
	Strong           bool              `json:"strong,omitempty"`
}

type raftGraphNodeResponse struct {
	Node domaingraph.Node   `json:"node"`
	Read *StrongReadContext `json:"read,omitempty"`
}
type raftGraphNodesResponse struct {
	Nodes         []domaingraph.Node `json:"nodes"`
	NextPageToken string             `json:"next_page_token"`
	Read          *StrongReadContext `json:"read,omitempty"`
}
type raftGraphEdgeResponse struct {
	Edge domaingraph.Edge   `json:"edge"`
	Read *StrongReadContext `json:"read,omitempty"`
}
type raftGraphOptionalEdgeResponse struct {
	Edge *domaingraph.Edge  `json:"edge,omitempty"`
	Read *StrongReadContext `json:"read,omitempty"`
}
type raftGraphEdgesResponse struct {
	Edges         []domaingraph.Edge `json:"edges"`
	NextPageToken string             `json:"next_page_token"`
	Read          *StrongReadContext `json:"read,omitempty"`
}

type raftGraphRevisionResponse struct {
	Revision int64              `json:"revision"`
	Read     *StrongReadContext `json:"read,omitempty"`
}

type raftGraphCountResponse struct {
	Count int                `json:"count"`
	Read  *StrongReadContext `json:"read,omitempty"`
}

type raftGraphIndexedNodesResponse struct {
	Nodes         []domaingraph.Node `json:"nodes"`
	NextPageToken string             `json:"next_page_token"`
	Stats         IndexedReadStats   `json:"stats"`
	Read          *StrongReadContext `json:"read,omitempty"`
}

type raftGraphIndexedEdgesResponse struct {
	Edges         []domaingraph.Edge `json:"edges"`
	NextPageToken string             `json:"next_page_token"`
	Stats         IndexedReadStats   `json:"stats"`
	Read          *StrongReadContext `json:"read,omitempty"`
}

type raftGraphSubtreeResponse struct {
	Result SubtreeResult      `json:"result"`
	Stats  IndexedReadStats   `json:"stats"`
	Read   *StrongReadContext `json:"read,omitempty"`
}

type raftGraphOKResponse struct {
	OK   bool               `json:"ok"`
	Read *StrongReadContext `json:"read,omitempty"`
}

func (m *Module) EnableExperimentalRaftNetworking(local consensus.NodeID, addrs []string, token string) {
	m.raftLocalNode = local
	m.raftNodeAddrs = append([]string(nil), addrs...)
	m.raftBackendAuthToken = token
}

func (m *Module) shouldForwardRaftGraphRead(spaceID string) (consensus.NodeID, bool, error) {
	leader, local, err := m.raftGraphRoute(spaceID)
	if err != nil || leader == 0 {
		return 0, false, err
	}
	return leader, leader != local, nil
}

func (m *Module) shouldForwardRaftGraphTransactionRead(tx daemonsession.GraphTransaction) (consensus.NodeID, bool, error) {
	if tx.Mode == daemonsession.TransactionModeReadWrite {
		if err := m.requireRaftGraphWriteRoute(tx.SpaceID); err != nil {
			return 0, false, err
		}
		return 0, false, nil
	}
	return m.shouldForwardRaftGraphRead(tx.SpaceID)
}

func (m *Module) RequireLocalGraphWriteLeader(ctx context.Context, spaceID string) error {
	return m.requireLocalRaftGraphWriteRoute(spaceID)
}

func (m *Module) GraphWriteRoute(ctx context.Context, spaceID string) (consensus.NodeID, consensus.NodeID, error) {
	return m.raftGraphRoute(spaceID)
}

func (m *Module) requireRaftGraphWriteRoute(spaceID string) error {
	if m.raftGroups == nil {
		return m.requireLocalWriteAllowed()
	}
	leader, _, err := m.raftGraphRoute(spaceID)
	if err != nil || leader == 0 {
		return err
	}
	return nil
}

func (m *Module) requireLocalRaftGraphWriteRoute(spaceID string) error {
	if m.raftGroups == nil {
		return m.requireLocalWriteAllowed()
	}
	leader, local, err := m.raftGraphRoute(spaceID)
	if err != nil || leader == 0 {
		return err
	}
	if leader != local {
		return raftGraphUnavailable("raft graph write for space %s is not local to partition leader %d", spaceID, leader)
	}
	return nil
}

func (m *Module) raftGraphRoute(spaceID string) (leader consensus.NodeID, local consensus.NodeID, err error) {
	leader, local, _, _, err = m.raftGraphGroupRoute(spaceID)
	return leader, local, err
}

func (m *Module) raftGraphGroupRoute(spaceID string) (leader consensus.NodeID, local consensus.NodeID, group *consensus.Group, partitionID uint32, err error) {
	if m.raftGroups == nil {
		return 0, 0, nil, 0, nil
	}
	local = m.raftLocalNode
	if local == 0 {
		local = m.raftGroups.NodeID()
	}
	if local == 0 {
		return 0, 0, nil, 0, raftGraphUnavailable("raft graph routing is not configured")
	}
	if m.raftPartitionCount == 0 {
		return 0, 0, nil, 0, raftGraphUnavailable("raft partition count is not configured")
	}
	parsed, err := uuid.Parse(spaceID)
	if err != nil {
		return 0, 0, nil, 0, err
	}
	cmd, err := consensus.NewSpaceCommand(domainspace.SpaceID(parsed), m.raftPartitionCount, recordTypeGraphCommit, nil, "graph-route")
	if err != nil {
		return 0, 0, nil, 0, err
	}
	partitionID = cmd.PartitionID
	g, ok := m.raftGroups.Group(consensus.PartitionGroupID(partitionID))
	if !ok || g == nil {
		return 0, 0, nil, partitionID, raftGraphUnavailable("raft partition group %d is not available", partitionID)
	}
	leader = g.Leader()
	if leader == 0 {
		return 0, 0, nil, partitionID, raftGraphUnavailable("raft partition group %d has no leader", partitionID)
	}
	return leader, local, g, partitionID, nil
}

func (m *Module) strongGraphRead(ctx context.Context, spaceID string) (*StrongReadContext, error) {
	if m.raftGroups == nil {
		return nil, nil
	}
	leader, local, group, partitionID, err := m.raftGraphGroupRoute(spaceID)
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, nil
	}
	if leader != local {
		return nil, raftGraphUnavailable("raft graph strong read for space %s reached non-leader node %d; leader is %d", spaceID, local, leader)
	}
	barrier, err := group.LinearizableRead(ctx)
	if err != nil {
		return nil, mapRaftReadBarrierError(partitionID, err)
	}
	_, _, applied := group.Progress()
	return &StrongReadContext{GroupID: consensus.PartitionGroupID(partitionID), PartitionID: partitionID, LeaderNodeID: leader, ReadIndex: barrier.Index, AppliedIndex: applied, Strong: true}, nil
}

func (m *Module) strongGraphReadForTransaction(ctx context.Context, tx daemonsession.GraphTransaction) (*StrongReadContext, error) {
	if tx.Mode == daemonsession.TransactionModeReadWrite {
		RecordOverlayRead(ctx)
		return nil, nil
	}
	read, err := m.strongGraphRead(ctx, tx.SpaceID)
	if err != nil {
		return nil, err
	}
	store, err := m.store(ctx, tx.SpaceID)
	if err != nil {
		return read, err
	}
	observedRevision := int64(store.Revision())
	if read != nil {
		read.ObservedRevision = observedRevision
		RecordStrongReadContext(ctx, read)
	}
	if tx.BaseRevision > 0 && observedRevision < tx.BaseRevision {
		return read, raftGraphUnavailable("read-only graph transaction base revision %d is ahead of observed graph revision %d", tx.BaseRevision, observedRevision)
	}
	return read, nil
}

func mapRaftReadBarrierError(partitionID uint32, err error) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Errorf(codes.DeadlineExceeded, "raft graph strong read for partition %d timed out: %v", partitionID, err)
	}
	return raftGraphUnavailable("raft graph strong read for partition %d failed: %v", partitionID, err)
}

func raftGraphUnavailable(format string, args ...any) error {
	return status.Errorf(codes.Unavailable, format, args...)
}

func (m *Module) forwardRaftGraphRead(ctx context.Context, leader consensus.NodeID, req raftGraphReadRequest, out any) error {
	idx := int(leader) - 1
	if idx < 0 || idx >= len(m.raftNodeAddrs) || m.raftNodeAddrs[idx] == "" {
		return fmt.Errorf("raft leader %d has no configured backend address", leader)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	res, err := backend.Client{AuthToken: m.raftBackendAuthToken}.ExecuteRaftGraphRead(ctx, m.raftNodeAddrs[idx], req.SpaceID, payload)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(res, out); err != nil {
		return err
	}
	recordForwardedRaftGraphRead(ctx, out)
	return nil
}

func recordForwardedRaftGraphRead(ctx context.Context, out any) {
	switch res := out.(type) {
	case *raftGraphNodeResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphNodesResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphEdgeResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphOptionalEdgeResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphEdgesResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphRevisionResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphCountResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphIndexedNodesResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphIndexedEdgesResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphSubtreeResponse:
		RecordStrongReadContext(ctx, res.Read)
	case *raftGraphOKResponse:
		RecordStrongReadContext(ctx, res.Read)
	}
}

func (m *Module) ExecuteLocalRaftGraphRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error) {
	var req raftGraphReadRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	if req.SpaceID == "" {
		req.SpaceID = spaceID
	}
	if strings.TrimSpace(req.SpaceID) != strings.TrimSpace(spaceID) {
		return nil, status.Error(codes.InvalidArgument, "raft graph read space_id mismatch")
	}
	if leader, local, err := m.raftGraphRoute(req.SpaceID); err != nil {
		return nil, err
	} else if leader != 0 && leader != local {
		return nil, raftGraphUnavailable("raft graph read for space %s reached non-leader node %d; leader is %d", req.SpaceID, local, leader)
	}
	tx := req.toTx()
	if strings.TrimSpace(tx.SpaceID) == "" {
		tx.SpaceID = req.SpaceID
	}
	var read *StrongReadContext
	if tx.Mode != daemonsession.TransactionModeReadWrite {
		var err error
		read, err = m.strongGraphReadForTransaction(ctx, tx)
		if err != nil {
			return nil, err
		}
	}
	switch req.Op {
	case "get_node":
		id, err := parseUUID[domaingraph.NodeID](req.ID, "node_id")
		if err != nil {
			return nil, err
		}
		n, err := m.node(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphNodeResponse{Node: n, Read: read})
	case "list_nodes":
		n, next, err := m.listNodesLocal(ctx, tx, req.PageSize, req.PageToken)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphNodesResponse{Nodes: n, NextPageToken: next, Read: read})
	case "configure_indexes":
		store, err := m.store(ctx, tx.SpaceID)
		if err != nil {
			return nil, err
		}
		if err := store.ConfigureIndexes(ctx, mustDomainID(tx.DomainID), req.SchemaHash, req.Indexes); err != nil {
			return nil, mapStorageError(err)
		}
		return json.Marshal(raftGraphOKResponse{OK: true, Read: read})
	case "scan_label":
		n, next, stats, err := m.ScanLabel(ctx, tx, req.LabelScan)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphIndexedNodesResponse{Nodes: n, NextPageToken: next, Stats: stats, Read: read})
	case "scan_tag":
		n, next, stats, err := m.ScanTag(ctx, tx, req.TagScan)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphIndexedNodesResponse{Nodes: n, NextPageToken: next, Stats: stats, Read: read})
	case "scan_node_property_ordered":
		n, next, stats, err := m.ScanNodePropertyOrdered(ctx, tx, req.OrderedNodePropertyScan)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphIndexedNodesResponse{Nodes: n, NextPageToken: next, Stats: stats, Read: read})
	case "scan_adjacency":
		e, next, stats, err := m.ScanAdjacency(ctx, tx, req.AdjacencyScan)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphIndexedEdgesResponse{Edges: e, NextPageToken: next, Stats: stats, Read: read})
	case "scan_subtree":
		result, stats, err := m.ScanSubtree(ctx, tx, req.SubtreeScan)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphSubtreeResponse{Result: result, Stats: stats, Read: read})
	case "get_edge":
		id, err := parseUUID[domaingraph.EdgeID](req.ID, "edge_id")
		if err != nil {
			return nil, err
		}
		e, err := m.edge(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphEdgeResponse{Edge: e, Read: read})
	case "list_edges":
		e, next, err := m.listEdgesLocal(ctx, tx, req.PageSize, req.PageToken)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphEdgesResponse{Edges: e, NextPageToken: next, Read: read})
	case "list_children":
		e, err := m.listChildrenLocal(ctx, tx, req.ID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphEdgesResponse{Edges: e, Read: read})
	case "get_parent":
		e, err := m.parentEdgeLocal(ctx, tx, req.ID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphOptionalEdgeResponse{Edge: e, Read: read})
	case "current_revision":
		store, err := m.store(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		revision := int64(store.Revision())
		if read != nil {
			read.ObservedRevision = revision
		}
		return json.Marshal(raftGraphRevisionResponse{Revision: revision, Read: read})
	case "blob_ref_count":
		id := domaingraph.BlobID(strings.TrimSpace(req.ID))
		if _, err := id.Bytes(); err != nil {
			return nil, fmt.Errorf("%w: invalid blob_id", ErrInvalidInput)
		}
		store, err := m.store(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		count, err := store.BlobRefCount(ctx, id)
		if err != nil {
			return nil, mapStorageError(err)
		}
		return json.Marshal(raftGraphCountResponse{Count: count, Read: read})
	default:
		return nil, fmt.Errorf("unsupported raft graph read op %q", req.Op)
	}
}

func raftReadRequest(op string, tx daemonsession.GraphTransaction) raftGraphReadRequest {
	return raftGraphReadRequest{Op: op, SpaceID: tx.SpaceID, Tx: struct {
		ID, SessionID, PrincipalID, SpaceID, DomainID, Mode, State string
		HomeNodeID                                                 uint64
		BaseRevision                                               int64
	}{tx.ID, tx.SessionID, tx.PrincipalID, tx.SpaceID, tx.DomainID, string(tx.Mode), string(tx.State), uint64(tx.HomeNodeID), tx.BaseRevision}}
}
func (r raftGraphReadRequest) toTx() daemonsession.GraphTransaction {
	return daemonsession.GraphTransaction{ID: r.Tx.ID, SessionID: r.Tx.SessionID, PrincipalID: r.Tx.PrincipalID, SpaceID: r.Tx.SpaceID, DomainID: r.Tx.DomainID, HomeNodeID: consensus.NodeID(r.Tx.HomeNodeID), Mode: daemonsession.TransactionMode(r.Tx.Mode), State: daemonsession.TransactionState(r.Tx.State), BaseRevision: r.Tx.BaseRevision}
}
