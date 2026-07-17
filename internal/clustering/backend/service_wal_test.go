package backend

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type walStreamCapture struct {
	ctx     context.Context
	records []*clusterpb.WalRecord
}

func (s *walStreamCapture) Send(r *clusterpb.WalRecord) error {
	s.records = append(s.records, r)
	return nil
}
func (s *walStreamCapture) Context() context.Context     { return s.ctx }
func (s *walStreamCapture) SendMsg(any) error            { return nil }
func (s *walStreamCapture) RecvMsg(any) error            { return io.EOF }
func (s *walStreamCapture) SendHeader(metadata.MD) error { return nil }
func (s *walStreamCapture) SetHeader(metadata.MD) error  { return nil }
func (s *walStreamCapture) SetTrailer(metadata.MD)       {}

func newWalStreamService(t *testing.T) (*Service, *wal.Manager) {
	t.Helper()
	ctx := context.Background()
	wm, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(t.TempDir(), "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	self := model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_a", NodeName: "node-a", ClusterID: "cluster_a", ClusterName: "dev", ClusterAdmitted: true, BackendAdvertiseAddr: "127.0.0.1:1"}
	store := membership.NewFileStore(filepath.Join(t.TempDir(), "membership.json"), "cluster_a", "dev")
	_ = store.UpsertMember(ctx, membership.Member{NodeID: "node_b", NodeName: "node-b", State: membership.MemberStateActive})
	svc := NewService(self, model.NodeStateClustered, nil).WithMembership(store).WithAuthority(&clusterpb.ClusterAuthority{ClusterId: "cluster_a", Primary: &clusterpb.AuthorityPrimary{NodeId: "node_a", NodeName: "node-a"}, AuthorityEpoch: 1}).WithWAL(wm)
	return svc, wm
}

func TestStreamWalValidationAndRecordsAfterLSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	svc, wm := newWalStreamService(t)
	defer wm.Close()
	_, _ = wm.Append(ctx, wal.PendingRecord{Type: "test.v1", SchemaVersion: 1, Timestamp: time.Now(), Encoding: wal.PayloadEncodingJSON, Payload: []byte(`{"n":1}`)})
	_, _ = wm.Append(ctx, wal.PendingRecord{Type: "test.v1", SchemaVersion: 1, Timestamp: time.Now(), Encoding: wal.PayloadEncodingJSON, Payload: []byte(`{"n":2}`)})
	stream := &walStreamCapture{ctx: ctx}
	err := svc.StreamWal(&clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", FollowerNodeId: "node_b", AfterLsn: 1, AuthorityEpoch: 1}, stream)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if len(stream.records) != 1 || stream.records[0].GetLsn() != 2 {
		t.Fatalf("records=%#v err=%v", stream.records, err)
	}
}

func TestStreamWalRejectsInvalidRequests(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWalStreamService(t)
	stream := &walStreamCapture{ctx: ctx}
	cases := []struct {
		name string
		req  *clusterpb.StreamWalRequest
		code codes.Code
	}{{"wrong cluster", &clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "other", FollowerNodeId: "node_b"}, codes.FailedPrecondition}, {"epoch", &clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", FollowerNodeId: "node_b", AuthorityEpoch: 2}, codes.FailedPrecondition}, {"no follower", &clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a"}, codes.InvalidArgument}, {"unknown follower", &clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", FollowerNodeId: "node_x"}, codes.PermissionDenied}}
	for _, tc := range cases {
		if err := svc.StreamWal(tc.req, stream); status.Code(err) != tc.code {
			t.Fatalf("%s code=%v err=%v", tc.name, status.Code(err), err)
		}
	}
}

func TestStreamWalRejectsNonPrimaryAndMissingWAL(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWalStreamService(t)
	svc.Authority.Primary.NodeId = "node_other"
	err := svc.StreamWal(&clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", FollowerNodeId: "node_b"}, &walStreamCapture{ctx: ctx})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code=%v err=%v", status.Code(err), err)
	}
	svc.Authority.Primary.NodeId = "node_a"
	svc.WAL = nil
	err = svc.StreamWal(&clusterpb.StreamWalRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", FollowerNodeId: "node_b"}, &walStreamCapture{ctx: ctx})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code=%v err=%v", status.Code(err), err)
	}
}
