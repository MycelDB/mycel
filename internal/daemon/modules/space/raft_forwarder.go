package space

import (
	"context"
	"errors"
	"fmt"

	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	storespaces "github.com/myceldb/mycel/internal/space/storage/spaces"
)

type raftSpaceForwarder struct {
	NodeAddrs []string
	Client    backend.Client
}

func (f raftSpaceForwarder) ForwardForSpace(ctx context.Context, route routing.Route, fn func(context.Context) error) error {
	return fmt.Errorf("raft forwarding for mutations is not implemented")
}

func (f raftSpaceForwarder) ForwardForSpaceValue(ctx context.Context, route routing.Route, fn routing.SpaceFunc[any]) (any, error) {
	addr, err := f.addrForLeader(route.Leader)
	if err != nil {
		return nil, err
	}
	return f.Client.GetRaftSpace(ctx, addr, route.SpaceID)
}

func (f raftSpaceForwarder) addrForLeader(leader consensus.NodeID) (string, error) {
	idx := int(leader) - 1
	if leader == 0 || idx < 0 || idx >= len(f.NodeAddrs) || f.NodeAddrs[idx] == "" {
		return "", fmt.Errorf("raft leader %d has no configured backend address", leader)
	}
	return f.NodeAddrs[idx], nil
}

func (m *Module) listSpacesViaRaftLeaders(ctx context.Context, includeArchived bool) ([]domainspace.Space, error) {
	if m.raftGroups == nil {
		return nil, fmt.Errorf("raft groups are not configured")
	}
	seenLeaders := map[consensus.NodeID]bool{}
	byID := map[domainspace.SpaceID]domainspace.Space{}
	for _, status := range m.raftGroups.Status() {
		if status.PartitionID == nil || status.Leader == 0 || seenLeaders[status.Leader] {
			continue
		}
		seenLeaders[status.Leader] = true
		var spaces []domainspace.Space
		var err error
		if status.Leader == m.raftLocalNode {
			spaces, err = m.ListLocalRaftSpaces(ctx, includeArchived)
		} else {
			addr, addrErr := (raftSpaceForwarder{NodeAddrs: m.raftNodeAddrs, Client: backend.Client{AuthToken: m.raftBackendAuthToken}}).addrForLeader(status.Leader)
			if addrErr != nil {
				return nil, addrErr
			}
			spaces, err = backend.Client{AuthToken: m.raftBackendAuthToken}.ListRaftSpaces(ctx, addr, includeArchived)
		}
		if err != nil {
			return nil, err
		}
		for _, sp := range spaces {
			byID[sp.SpaceID] = sp
		}
	}
	out := make([]domainspace.Space, 0, len(byID))
	for _, sp := range byID {
		out = append(out, sp)
	}
	return out, nil
}

func (m *Module) ListLocalRaftSpaces(ctx context.Context, includeArchived bool) ([]domainspace.Space, error) {
	spaces, err := m.spaces.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domainspace.Space, 0, len(spaces))
	for _, sp := range spaces {
		if !includeArchived && isArchived(sp) {
			continue
		}
		out = append(out, sp)
	}
	return out, nil
}

func (m *Module) GetLocalRaftSpace(ctx context.Context, spaceID string) (domainspace.Space, error) {
	id, err := parseSpaceID(spaceID)
	if err != nil {
		return domainspace.Space{}, err
	}
	sp, err := m.spaces.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, storespaces.ErrSpaceNotFound) {
			return domainspace.Space{}, ErrSpaceNotFound
		}
		return domainspace.Space{}, err
	}
	return sp, nil
}
