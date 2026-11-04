package service

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type projectionReadinessBackendReader struct{ err error }

func (r projectionReadinessBackendReader) ExecuteLocalRaftSemanticRead(context.Context, string, []byte) ([]byte, error) {
	return nil, raftSemanticSearchError(r.err)
}

func TestProjectionReadinessSurvivesClusterBackendWireForwarding(t *testing.T) {
	for _, test := range []struct {
		state string
		code  codes.Code
	}{
		{state: "completed", code: codes.Unavailable},
		{state: "required", code: codes.FailedPrecondition},
	} {
		t.Run(test.state, func(t *testing.T) {
			readiness := &ProjectionReadinessError{SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", LocalNode: 2, Expected: ProjectionReadinessTuple{LeaderNode: 2, LeaderTerm: 9, SemanticGeneration: 3}, Reasons: []string{ProjectionReadinessMissingLease, ProjectionReadinessMissingBaseline}, RebuildState: test.state}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			clusterpb.RegisterClusterBackendServiceServer(server, &backend.Service{SemanticReader: projectionReadinessBackendReader{err: fmt.Errorf("projection fence: %w", readiness)}})
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = server.Serve(listener)
			}()
			t.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
				<-done
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = (backend.Client{AuthToken: "local-test-token"}).ExecuteRaftSemanticRead(ctx, listener.Addr().String(), uuid.NewString(), []byte(`{"op":"search"}`))
			st := status.Convert(err)
			if st.Code() != test.code || len(st.Details()) != 1 {
				t.Fatalf("forwarded status=%v details=%v, want %s with ErrorInfo", st, st.Details(), test.code)
			}
			info, ok := st.Details()[0].(*errdetails.ErrorInfo)
			if !ok || info.Reason != "SEMANTIC_PROJECTION_NOT_READY" || info.Domain != "myceldb.io" || !reflect.DeepEqual(info.Metadata, readiness.DiagnosticMetadata()) {
				t.Fatalf("forwarded details=%+v", st.Details())
			}
		})
	}
}
