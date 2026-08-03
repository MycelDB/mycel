package backend

import (
	"context"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func TestCreateLocalBackupArchiveForwardsToProvider(t *testing.T) {
	provider := &captureClusterBackupProvider{result: CreateLocalBackupArchiveResult{
		ClusterID:      "cluster-a",
		PodName:        "myceld-0",
		NodeID:         "node-a",
		RaftNodeID:     1,
		Ordinal:        0,
		ArchiveName:    "archive.tar.zst",
		ArchiveURI:     "file:///backups/archive.tar.zst",
		ManifestName:   "archive.manifest.json",
		ManifestURI:    "file:///backups/archive.manifest.json",
		SizeBytes:      123,
		ChecksumSHA256: "abc",
		AppliedIndexes: map[string]uint64{"system": 7},
	}}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node-a", ClusterID: "cluster-a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClusterBackupProvider(provider)
	res, err := svc.CreateLocalBackupArchive(context.Background(), &clusterpb.CreateLocalBackupArchiveRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       "cluster-a",
		RequesterNodeId: 1,
		BackupSetId:     "backup-set-1",
		Reason:          "test",
		PodName:         "myceld-0",
		NodeId:          "node-a",
		RaftNodeId:      1,
		Ordinal:         0,
		OutputDir:       "/backups",
		ArchiveFormat:   "tar.zst",
		UtcTimestamp:    "20260803T183500Z",
		Barriers:        []*clusterpb.BackupRaftBarrier{{GroupId: "system", Index: 7}},
	})
	if err != nil {
		t.Fatalf("CreateLocalBackupArchive() error=%v", err)
	}
	if provider.input.BackupSetID != "backup-set-1" || provider.input.Barriers[0].Index != 7 {
		t.Fatalf("provider input=%#v", provider.input)
	}
	if res.GetArchiveUri() != provider.result.ArchiveURI || res.GetAppliedIndexes()["system"] != 7 {
		t.Fatalf("response=%#v", res)
	}
}

func TestCreateLocalBackupArchiveRejectsClusterMismatch(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node-a", ClusterID: "cluster-a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClusterBackupProvider(&captureClusterBackupProvider{})
	_, err := svc.CreateLocalBackupArchive(context.Background(), &clusterpb.CreateLocalBackupArchiveRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "other", UtcTimestamp: time.Now().UTC().Format("20060102T150405Z")})
	if err == nil {
		t.Fatal("CreateLocalBackupArchive() succeeded, want error")
	}
}

func TestCreateLocalBackupArchiveRejectsUnadmittedNode(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node-a", ClusterID: "cluster-a", ClusterAdmitted: false}, model.NodeStateClustered, nil).WithClusterBackupProvider(&captureClusterBackupProvider{})
	_, err := svc.CreateLocalBackupArchive(context.Background(), &clusterpb.CreateLocalBackupArchiveRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster-a", UtcTimestamp: time.Now().UTC().Format("20060102T150405Z")})
	if err == nil {
		t.Fatal("CreateLocalBackupArchive() succeeded, want error")
	}
}

type captureClusterBackupProvider struct {
	input  CreateLocalBackupArchiveInput
	result CreateLocalBackupArchiveResult
}

func (p *captureClusterBackupProvider) CreateLocalClusterBackupArchive(ctx context.Context, in CreateLocalBackupArchiveInput) (CreateLocalBackupArchiveResult, error) {
	_ = ctx
	p.input = in
	return p.result, nil
}
