package wal

import "time"

type PayloadEncoding uint8

const PayloadEncodingJSON PayloadEncoding = 1

type RecordType string

type PendingRecord struct {
	Type          RecordType
	SchemaVersion uint16
	Timestamp     time.Time
	Encoding      PayloadEncoding
	Payload       []byte
}

type Record struct {
	LSN           LSN
	Type          RecordType
	SchemaVersion uint16
	Timestamp     time.Time
	Encoding      PayloadEncoding
	Payload       []byte
}
