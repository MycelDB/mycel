package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type raftSemanticReadRequest struct {
	Op       string              `json:"op"`
	SpaceID  domainspace.SpaceID `json:"space_id"`
	Consumer string              `json:"consumer,omitempty"`
}
type raftSemanticIndexesResponse struct {
	Indexes []domainsemantic.SemanticIndex `json:"indexes"`
}
type raftSemanticGrantsResponse struct {
	Grants []domainsemantic.CredentialGrant `json:"grants"`
}
type raftSemanticPoliciesResponse struct {
	Policies []domainsemantic.InferencePolicy `json:"policies"`
}
type raftSemanticDirtyEventsResponse struct {
	Events []domainsemantic.GraphDirtyEvent `json:"events"`
}
type raftSemanticWorkItemsResponse struct {
	Items []domainsemantic.SemanticDirtyWorkItem `json:"items"`
}
type raftSemanticCheckpointResponse struct {
	Checkpoint storesemantic.MaintenanceCheckpoint `json:"checkpoint"`
}

func (m *Module) EnableExperimentalRaftNetworking(local consensus.NodeID, addrs []string, token string) {
	m.raftLocalNode = local
	m.raftNodeAddrs = append([]string(nil), addrs...)
	m.raftBackendAuthToken = token
}

func (m *Module) shouldForwardRaftSemanticRead(spaceID domainspace.SpaceID) (consensus.NodeID, bool, error) {
	if m.raftGroups == nil || m.raftLocalNode == 0 {
		return 0, false, nil
	}
	cmd, err := consensus.NewSpaceCommand(spaceID, m.raftPartitionCount, recordTypeSemanticSpace, nil, "semantic-read-route")
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

func (m *Module) forwardRaftSemanticRead(ctx context.Context, leader consensus.NodeID, req raftSemanticReadRequest, out any) error {
	idx := int(leader) - 1
	if idx < 0 || idx >= len(m.raftNodeAddrs) || strings.TrimSpace(m.raftNodeAddrs[idx]) == "" {
		return fmt.Errorf("raft leader %d has no configured backend address", leader)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	res, err := backend.Client{AuthToken: m.raftBackendAuthToken}.ExecuteRaftSemanticRead(ctx, m.raftNodeAddrs[idx], req.SpaceID.String(), payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(res, out)
}

func (m *Module) ExecuteLocalRaftSemanticRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error) {
	var req raftSemanticReadRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	if req.SpaceID == (domainspace.SpaceID{}) {
		parsed, err := uuid.Parse(spaceID)
		if err != nil {
			return nil, err
		}
		req.SpaceID = domainspace.SpaceID(parsed)
	}
	saved := m.raftGroups
	m.raftGroups = nil
	defer func() { m.raftGroups = saved }()
	switch req.Op {
	case "list_indexes":
		mgr, err := m.SpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticIndexesResponse{Indexes: v})
	case "list_grants":
		mgr, err := m.SpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticGrantsResponse{Grants: v})
	case "list_policies":
		mgr, err := m.SpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListInferencePolicies(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticPoliciesResponse{Policies: v})
	case "list_dirty_events":
		mgr, err := m.MaintenanceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListGraphDirtyEvents(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticDirtyEventsResponse{Events: v})
	case "list_work_items":
		mgr, err := m.MaintenanceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListDirtyWorkItems(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticWorkItemsResponse{Items: v})
	case "get_checkpoint":
		mgr, err := m.MaintenanceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.GetCheckpoint(ctx, req.Consumer)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticCheckpointResponse{Checkpoint: v})
	default:
		return nil, fmt.Errorf("unsupported raft semantic read op %q", req.Op)
	}
}
