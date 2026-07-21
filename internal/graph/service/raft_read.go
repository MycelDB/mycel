package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type raftGraphReadRequest struct {
	Op      string `json:"op"`
	SpaceID string `json:"space_id"`
	Tx      struct {
		ID, SessionID, UserID, SpaceID, DomainID, Mode, State string
		BaseRevision                                          int64
	} `json:"tx"`
	ID        string `json:"id,omitempty"`
	PageSize  int    `json:"page_size,omitempty"`
	PageToken string `json:"page_token,omitempty"`
}

type raftGraphNodeResponse struct {
	Node domaingraph.Node `json:"node"`
}
type raftGraphNodesResponse struct {
	Nodes         []domaingraph.Node `json:"nodes"`
	NextPageToken string             `json:"next_page_token"`
}
type raftGraphEdgeResponse struct {
	Edge domaingraph.Edge `json:"edge"`
}
type raftGraphOptionalEdgeResponse struct {
	Edge *domaingraph.Edge `json:"edge,omitempty"`
}
type raftGraphEdgesResponse struct {
	Edges         []domaingraph.Edge `json:"edges"`
	NextPageToken string             `json:"next_page_token"`
}

func (m *Module) EnableExperimentalRaftNetworking(local consensus.NodeID, addrs []string, token string) {
	m.raftLocalNode = local
	m.raftNodeAddrs = append([]string(nil), addrs...)
	m.raftBackendAuthToken = token
}

func (m *Module) shouldForwardRaftGraphRead(spaceID string) (consensus.NodeID, bool, error) {
	if m.raftGroups == nil || m.raftLocalNode == 0 {
		return 0, false, nil
	}
	if m.raftPartitionCount == 0 {
		return 0, false, fmt.Errorf("raft partition count is not configured")
	}
	parsed, err := uuid.Parse(spaceID)
	if err != nil {
		return 0, false, err
	}
	cmd, err := consensus.NewSpaceCommand(domainspace.SpaceID(parsed), m.raftPartitionCount, recordTypeGraphCommit, nil, "read-route")
	if err != nil {
		return 0, false, err
	}
	g, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || g == nil {
		return 0, false, fmt.Errorf("raft partition group %d is not available", cmd.PartitionID)
	}
	leader := g.Leader()
	return leader, leader != 0 && leader != m.raftLocalNode, nil
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
	return json.Unmarshal(res, out)
}

func (m *Module) ExecuteLocalRaftGraphRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error) {
	var req raftGraphReadRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	if req.SpaceID == "" {
		req.SpaceID = spaceID
	}
	tx := req.toTx()
	saved := m.raftGroups
	m.raftGroups = nil
	defer func() { m.raftGroups = saved }()
	switch req.Op {
	case "get_node":
		n, err := m.GetNode(ctx, tx, req.ID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphNodeResponse{Node: n})
	case "list_nodes":
		n, next, err := m.ListNodes(ctx, tx, req.PageSize, req.PageToken)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphNodesResponse{Nodes: n, NextPageToken: next})
	case "get_edge":
		e, err := m.GetEdge(ctx, tx, req.ID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphEdgeResponse{Edge: e})
	case "list_edges":
		e, next, err := m.ListEdges(ctx, tx, req.PageSize, req.PageToken)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphEdgesResponse{Edges: e, NextPageToken: next})
	case "list_children":
		e, err := m.ListChildren(ctx, tx, req.ID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphEdgesResponse{Edges: e})
	case "get_parent":
		e, err := m.GetParent(ctx, tx, req.ID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphOptionalEdgeResponse{Edge: e})
	default:
		return nil, fmt.Errorf("unsupported raft graph read op %q", req.Op)
	}
}

func raftReadRequest(op string, tx daemonsession.GraphTransaction) raftGraphReadRequest {
	return raftGraphReadRequest{Op: op, SpaceID: tx.SpaceID, Tx: struct {
		ID, SessionID, UserID, SpaceID, DomainID, Mode, State string
		BaseRevision                                          int64
	}{tx.ID, tx.SessionID, tx.UserID, tx.SpaceID, tx.DomainID, string(tx.Mode), string(tx.State), tx.BaseRevision}}
}
func (r raftGraphReadRequest) toTx() daemonsession.GraphTransaction {
	return daemonsession.GraphTransaction{ID: r.Tx.ID, SessionID: r.Tx.SessionID, UserID: r.Tx.UserID, SpaceID: r.Tx.SpaceID, DomainID: r.Tx.DomainID, Mode: daemonsession.TransactionMode(r.Tx.Mode), State: daemonsession.TransactionState(r.Tx.State), BaseRevision: r.Tx.BaseRevision}
}
