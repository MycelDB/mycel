package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type raftGraphRevisionResponse struct {
	Revision int64 `json:"revision"`
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
	if m.raftGroups == nil {
		return 0, 0, nil
	}
	local = m.raftLocalNode
	if local == 0 {
		local = m.raftGroups.NodeID()
	}
	if local == 0 {
		return 0, 0, raftGraphUnavailable("raft graph routing is not configured")
	}
	if m.raftPartitionCount == 0 {
		return 0, 0, raftGraphUnavailable("raft partition count is not configured")
	}
	parsed, err := uuid.Parse(spaceID)
	if err != nil {
		return 0, 0, err
	}
	cmd, err := consensus.NewSpaceCommand(domainspace.SpaceID(parsed), m.raftPartitionCount, recordTypeGraphCommit, nil, "graph-route")
	if err != nil {
		return 0, 0, err
	}
	g, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || g == nil {
		return 0, 0, raftGraphUnavailable("raft partition group %d is not available", cmd.PartitionID)
	}
	leader = g.Leader()
	if leader == 0 {
		return 0, 0, raftGraphUnavailable("raft partition group %d has no leader", cmd.PartitionID)
	}
	return leader, local, nil
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
	if strings.TrimSpace(req.SpaceID) != strings.TrimSpace(spaceID) {
		return nil, status.Error(codes.InvalidArgument, "raft graph read space_id mismatch")
	}
	if leader, local, err := m.raftGraphRoute(req.SpaceID); err != nil {
		return nil, err
	} else if leader != 0 && leader != local {
		return nil, raftGraphUnavailable("raft graph read for space %s reached non-leader node %d; leader is %d", req.SpaceID, local, leader)
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
	case "current_revision":
		revision, err := m.CurrentRevision(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftGraphRevisionResponse{Revision: revision})
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
