package replication

import (
	"fmt"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
)

func RecordFromProto(pb *clusterpb.WalRecord) (Record, error) {
	if pb == nil {
		return Record{}, fmt.Errorf("wal record is required")
	}
	ts, err := time.Parse(time.RFC3339Nano, pb.GetTimestamp())
	if err != nil {
		return Record{}, err
	}
	var enc wal.PayloadEncoding
	switch pb.GetEncoding() {
	case "json":
		enc = wal.PayloadEncodingJSON
	default:
		return Record{}, fmt.Errorf("unsupported wal encoding %q", pb.GetEncoding())
	}
	return Record{LSN: wal.LSN(pb.GetLsn()), Type: wal.RecordType(pb.GetType()), SchemaVersion: uint16(pb.GetSchemaVersion()), Timestamp: ts, Encoding: enc, Payload: append([]byte(nil), pb.GetPayload()...)}, nil
}
