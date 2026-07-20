package space

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type noopSpaceRaftTransport struct{}

func (noopSpaceRaftTransport) Send(ctx context.Context, groupID consensus.GroupID, from consensus.NodeID, messages []raftpb.Message) {
}

func testUserID(t *testing.T) uuid.UUID {
	t.Helper()
	return uuid.New()
}

func mustPartitionForSpace(t *testing.T, spaceID domainspace.SpaceID) partitioning.PartitionID {
	t.Helper()
	p, err := partitioning.PartitionForSpaceID(spaceID, 64)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	return p
}
