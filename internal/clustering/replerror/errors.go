package replerror

import (
	"fmt"
	"strconv"

	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const SnapshotRequiredReason = "MYCEL_WAL_SNAPSHOT_REQUIRED"

const (
	SnapshotRequestedAfterLSNKey = "requested_after_lsn"
	SnapshotNextRequestedLSNKey  = "next_requested_lsn"
	SnapshotFirstRetainedLSNKey  = "first_retained_lsn"
	SnapshotLastCommittedLSNKey  = "last_committed_lsn"
	SnapshotCheckpointLSNKey     = "checkpoint_lsn"
	SnapshotPrimaryNodeIDKey     = "primary_node_id"
	SnapshotAuthorityEpochKey    = "authority_epoch"
)

type SnapshotRequiredInfo struct {
	RequestedAfterLSN wal.LSN `json:"requested_after_lsn"`
	NextRequestedLSN  wal.LSN `json:"next_requested_lsn"`
	FirstRetainedLSN  wal.LSN `json:"first_retained_lsn"`
	LastCommittedLSN  wal.LSN `json:"last_committed_lsn"`
	CheckpointLSN     wal.LSN `json:"checkpoint_lsn"`
	PrimaryNodeID     string  `json:"primary_node_id"`
	AuthorityEpoch    int64   `json:"authority_epoch"`
}

func SnapshotRequiredError(info SnapshotRequiredInfo) error {
	st := status.New(codes.FailedPrecondition, "follower requires snapshot catch-up")
	md := map[string]string{
		SnapshotRequestedAfterLSNKey: fmt.Sprintf("%d", info.RequestedAfterLSN),
		SnapshotNextRequestedLSNKey:  fmt.Sprintf("%d", info.NextRequestedLSN),
		SnapshotFirstRetainedLSNKey:  fmt.Sprintf("%d", info.FirstRetainedLSN),
		SnapshotLastCommittedLSNKey:  fmt.Sprintf("%d", info.LastCommittedLSN),
		SnapshotCheckpointLSNKey:     fmt.Sprintf("%d", info.CheckpointLSN),
		SnapshotPrimaryNodeIDKey:     info.PrimaryNodeID,
		SnapshotAuthorityEpochKey:    fmt.Sprintf("%d", info.AuthorityEpoch),
	}
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: SnapshotRequiredReason, Domain: "mycel.replication", Metadata: md})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func SnapshotRequiredInfoFromError(err error) (SnapshotRequiredInfo, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return SnapshotRequiredInfo{}, false
	}
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok || info.GetReason() != SnapshotRequiredReason {
			continue
		}
		md := info.GetMetadata()
		parse := func(k string) uint64 { v, _ := strconv.ParseUint(md[k], 10, 64); return v }
		epoch, _ := strconv.ParseInt(md[SnapshotAuthorityEpochKey], 10, 64)
		return SnapshotRequiredInfo{RequestedAfterLSN: wal.LSN(parse(SnapshotRequestedAfterLSNKey)), NextRequestedLSN: wal.LSN(parse(SnapshotNextRequestedLSNKey)), FirstRetainedLSN: wal.LSN(parse(SnapshotFirstRetainedLSNKey)), LastCommittedLSN: wal.LSN(parse(SnapshotLastCommittedLSNKey)), CheckpointLSN: wal.LSN(parse(SnapshotCheckpointLSNKey)), PrimaryNodeID: md[SnapshotPrimaryNodeIDKey], AuthorityEpoch: epoch}, true
	}
	return SnapshotRequiredInfo{}, false
}
