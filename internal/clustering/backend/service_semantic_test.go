package backend

import (
	"context"
	"errors"
	"testing"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failingLocalRaftSemanticReader struct{ err error }

func (r failingLocalRaftSemanticReader) ExecuteLocalRaftSemanticRead(context.Context, string, []byte) ([]byte, error) {
	return nil, r.err
}

func TestExecuteRaftSemanticReadPreservesCategorizedStatus(t *testing.T) {
	service := &Service{SemanticReader: failingLocalRaftSemanticReader{err: status.Error(codes.PermissionDenied, "semantic inference authorization denied")}}
	_, err := service.ExecuteRaftSemanticRead(context.Background(), &clusterpb.ExecuteRaftSemanticReadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: "space", Payload: []byte(`{}`)})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ExecuteRaftSemanticRead() code=%s error=%v, want PermissionDenied", status.Code(err), err)
	}
}

func TestExecuteRaftSemanticReadMapsUncategorizedErrorToInternal(t *testing.T) {
	service := &Service{SemanticReader: failingLocalRaftSemanticReader{err: errors.New("storage failed")}}
	_, err := service.ExecuteRaftSemanticRead(context.Background(), &clusterpb.ExecuteRaftSemanticReadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: "space", Payload: []byte(`{}`)})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ExecuteRaftSemanticRead() code=%s error=%v, want Internal", status.Code(err), err)
	}
}
