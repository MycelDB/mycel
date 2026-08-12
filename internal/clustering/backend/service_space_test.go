package backend

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/identity/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type fakeSpaceReader struct {
	space      domainspace.Space
	localCalls int
	calls      int
}

func (r *fakeSpaceReader) GetSpace(ctx context.Context, spaceID string) (domainspace.Space, error) {
	r.calls++
	return r.space, nil
}

func (r *fakeSpaceReader) GetLocalRaftSpace(ctx context.Context, spaceID string) (domainspace.Space, error) {
	r.localCalls++
	return r.space, nil
}

func TestGetRaftSpaceUsesLocalReaderAndMapsSpace(t *testing.T) {
	created := time.Now().UTC().Truncate(time.Nanosecond)
	updated := created.Add(time.Second)
	sp := domainspace.Space{SpaceID: uuid.New(), OwnerID: identity.PrincipalID(uuid.NewString()), Name: "main", Status: "active", CreatedAt: created, UpdatedAt: updated}
	reader := &fakeSpaceReader{space: sp}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node-a", ClusterID: "cluster-a"}, model.NodeStateClustered, nil).WithSpaceReader(reader)
	res, err := svc.GetRaftSpace(context.Background(), &clusterpb.GetRaftSpaceRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: sp.SpaceID.String()})
	if err != nil {
		t.Fatalf("GetRaftSpace() error = %v", err)
	}
	if reader.localCalls != 1 || reader.calls != 0 {
		t.Fatalf("reader calls local=%d normal=%d, want local-only", reader.localCalls, reader.calls)
	}
	roundTrip, err := SpaceFromProto(res.GetSpace())
	if err != nil {
		t.Fatalf("SpaceFromProto() error = %v", err)
	}
	if roundTrip.SpaceID != sp.SpaceID || roundTrip.OwnerID != sp.OwnerID || roundTrip.Name != sp.Name || roundTrip.Status != sp.Status || !roundTrip.CreatedAt.Equal(sp.CreatedAt) || !roundTrip.UpdatedAt.Equal(sp.UpdatedAt) {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", roundTrip, sp)
	}
}
