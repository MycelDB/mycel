package backend

import (
	"fmt"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
)

func walRecordToProto(rec wal.Record) (*clusterpb.WalRecord, error) {
	encoding, err := walEncodingToString(rec.Encoding)
	if err != nil {
		return nil, err
	}
	return &clusterpb.WalRecord{Lsn: uint64(rec.LSN), Type: string(rec.Type), SchemaVersion: uint32(rec.SchemaVersion), Timestamp: rec.Timestamp.UTC().Format(time.RFC3339Nano), Encoding: encoding, Payload: rec.Payload}, nil
}

func walRecordFromProto(pb *clusterpb.WalRecord) (wal.Record, error) {
	if pb == nil {
		return wal.Record{}, fmt.Errorf("wal record is required")
	}
	if pb.GetType() == "" {
		return wal.Record{}, fmt.Errorf("wal record type is required")
	}
	timestamp, err := time.Parse(time.RFC3339Nano, pb.GetTimestamp())
	if err != nil {
		return wal.Record{}, fmt.Errorf("parse wal timestamp: %w", err)
	}
	encoding, err := walEncodingFromString(pb.GetEncoding())
	if err != nil {
		return wal.Record{}, err
	}
	return wal.Record{LSN: wal.LSN(pb.GetLsn()), Type: wal.RecordType(pb.GetType()), SchemaVersion: uint16(pb.GetSchemaVersion()), Timestamp: timestamp, Encoding: encoding, Payload: append([]byte(nil), pb.GetPayload()...)}, nil
}

func walEncodingToString(encoding wal.PayloadEncoding) (string, error) {
	switch encoding {
	case wal.PayloadEncodingJSON:
		return "json", nil
	default:
		return "", fmt.Errorf("unsupported wal encoding %d", encoding)
	}
}

func walEncodingFromString(encoding string) (wal.PayloadEncoding, error) {
	switch encoding {
	case "json":
		return wal.PayloadEncodingJSON, nil
	default:
		return 0, fmt.Errorf("unsupported wal encoding %q", encoding)
	}
}
