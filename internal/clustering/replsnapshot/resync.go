package replsnapshot

import (
	"fmt"

	"github.com/myceldb/mycel/internal/wal"
)

type ResyncTarget struct {
	NodeID               string
	NodeName             string
	BackendAdvertiseAddr string
}

type SnapshotDescriptor struct {
	OperationID     string
	ClusterID       string
	PrimaryNodeID   string
	TargetNodeID    string
	AuthorityEpoch  int64
	SnapshotBaseLSN wal.LSN
	ManifestJSON    string
	TotalBytes      uint64
	Checksum        string
}

type InstallSnapshotResult struct {
	Installed  bool
	AppliedLSN wal.LSN
	Message    string
}

var (
	ErrResyncTargetNotFound     = fmt.Errorf("resync target not found")
	ErrResyncTargetNotFollower  = fmt.Errorf("resync target is not an active follower")
	ErrSnapshotValidationFailed = fmt.Errorf("snapshot validation failed")
	ErrSnapshotInstallFailed    = fmt.Errorf("snapshot install failed")
)
