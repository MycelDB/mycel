package service

import (
	"context"
	"errors"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	storespaces "github.com/myceldb/mycel/internal/space/storage/spaces"
)

func (m *Module) getSpaceViaRaftLeader(ctx context.Context, spaceID domainspace.SpaceID, groups *consensus.MultiGroup, localNode consensus.NodeID, forwarder routing.Forwarder) (domainspace.Space, error) {
	exec := routing.NewRaftExecutor(m.partitionCount, localNode, consensus.NewMultiGroupLeaderResolver(groups), forwarder)
	return routing.RaftForSpaceValue[domainspace.Space](exec, ctx, spaceID.String(), func(ctx context.Context) (domainspace.Space, error) {
		sp, err := m.spaces.GetByID(ctx, spaceID)
		if err != nil {
			if errors.Is(err, storespaces.ErrSpaceNotFound) {
				return domainspace.Space{}, ErrSpaceNotFound
			}
			return domainspace.Space{}, err
		}
		return sp, nil
	})
}
