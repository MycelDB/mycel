package replication

import (
	"encoding/json"
	"time"

	"github.com/myceldb/mycel/internal/wal"
)

type Record struct {
	LSN           wal.LSN             `json:"lsn"`
	Type          wal.RecordType      `json:"type"`
	SchemaVersion uint16              `json:"schema_version"`
	Timestamp     time.Time           `json:"timestamp"`
	Encoding      wal.PayloadEncoding `json:"encoding"`
	Payload       json.RawMessage     `json:"payload"`
}

func FromWAL(rec wal.Record) Record {
	return Record{LSN: rec.LSN, Type: rec.Type, SchemaVersion: rec.SchemaVersion, Timestamp: rec.Timestamp, Encoding: rec.Encoding, Payload: append([]byte(nil), rec.Payload...)}
}

func (r Record) WALRecord() wal.Record {
	return wal.Record{LSN: r.LSN, Type: r.Type, SchemaVersion: r.SchemaVersion, Timestamp: r.Timestamp, Encoding: r.Encoding, Payload: append([]byte(nil), r.Payload...)}
}
